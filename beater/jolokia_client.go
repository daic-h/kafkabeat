package beater

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/daichirata/kafkabeat/config"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
)

type JolokiaClient struct {
	hosts       []string
	proxyConfig *config.ProxyConfig
}

type requestPayload struct {
	Type      string       `json:"type"`
	Mbean     string       `json:"mbean"`
	Attribute *string      `json:"attribute"`
	Path      *string      `json:"path"`
	Target    *proxyTarget `json:"target"`
}

type proxyTarget struct {
	URL      string `json:"url"`
	Password string `json:"password"`
	User     string `json:"user"`
}

type jolokiaResponse struct {
	Status    uint32
	Timestamp uint32
	Request   map[string]interface{}
	Value     *meterMetric
	Error     string
}

type meterMetric struct {
	Count             int64
	FifteenMinuteRate float64
	FiveMinuteRate    float64
	OneMinuteRate     float64
	MeanRate          float64
}

var brokerMbeans = []string{
	"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec",
	"kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec",
	"kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec",
	"kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec",
	"kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec",
	"kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec",
}

func mbeanName(mbean string) string {
	s := strings.Split(mbean, ":")
	props := strings.Split(s[1], ",")
	for _, p := range props {
		ret := strings.Split(p, "=")
		if ret[0] == "name" {
			return ret[1]
		}
	}
	return ""
}

func NewJolokiaClient(hosts []string, proxyConfig *config.ProxyConfig) *JolokiaClient {
	return &JolokiaClient{
		hosts:       hosts,
		proxyConfig: proxyConfig,
	}
}

func (c *JolokiaClient) GetJMXEvents() []common.MapStr {
	var events []common.MapStr

	for _, host := range c.hosts {
		responses, err := c.executeRequest(host, brokerMbeans)
		if err != nil {
			logp.Err("%v", err)
			continue
		}
		jmx := getJMXEvent(host, responses)

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       "jmx",
			"jmx":        jmx,
		}

		events = append(events, event)
	}

	return events
}

func getJMXEvent(host string, responses []*jolokiaResponse) common.MapStr {
	event := common.MapStr{
		"host": host,
	}

	for _, response := range responses {
		if mbean, ok := response.Request["mbean"].(string); ok {
			val := response.Value
			event[mbeanName(mbean)] = common.MapStr{
				"Count":             val.Count,
				"FifteenMinuteRate": val.FifteenMinuteRate,
				"FiveMinuteRate":    val.FiveMinuteRate,
				"OneMinuteRate":     val.OneMinuteRate,
				"MeanRate":          val.MeanRate,
			}
		}
	}

	return event
}

func (c *JolokiaClient) hasProxy() bool {
	return c.proxyConfig.URL != ""
}

func (c *JolokiaClient) executeRequest(host string, mbeans []string) ([]*jolokiaResponse, error) {
	jsonStr, err := c.buildRequestJSON(host, mbeans)
	if err != nil {
		return nil, fmt.Errorf("buildRequestJSON Failed: %v", err)
	}

	resJSON, err := performPostRequest(c.buildRequestURL(host), jsonStr)
	if err != nil {
		return nil, fmt.Errorf("performPostRequest Failed: %v %s", err, string(jsonStr))
	}

	var responses []*jolokiaResponse
	if err := json.Unmarshal(resJSON, &responses); err != nil {
		return nil, fmt.Errorf("JSON Unmarshal Failed: %v %s", err, string(resJSON))
	}

	return responses, nil
}

func (c *JolokiaClient) buildRequestJSON(host string, mbeans []string) ([]byte, error) {
	var target *proxyTarget
	if c.hasProxy() {
		target = newProxyTarget(host, c.proxyConfig)
	}

	payloads := make([]*requestPayload, len(mbeans))
	for i, mbean := range mbeans {
		payloads[i] = newRequestPayload(mbean, "", "", target)
	}

	jsonStr, err := json.Marshal(payloads)
	if err != nil {
		return nil, fmt.Errorf("JSON Marshal Failed: %v", err)
	}

	return jsonStr, nil
}

func (c *JolokiaClient) buildRequestURL(rawURL string) string {
	var url string
	if c.hasProxy() {
		url = c.proxyConfig.URL
	} else {
		url = rawURL
	}

	if strings.Index(url, "//") == 0 {
		url = "http:" + url
	}
	if strings.Index(url, "://") == -1 {
		url = "http://" + url
	}
	return url + "/jolokia/"
}

func newProxyTarget(host string, proxyConfig *config.ProxyConfig) *proxyTarget {
	return &proxyTarget{
		URL:      "service:jmx:rmi:///jndi/rmi://" + host + "/jmxrmi",
		User:     proxyConfig.User,
		Password: proxyConfig.Password,
	}
}

func newRequestPayload(mbean string, attribute string, path string, target *proxyTarget) *requestPayload {
	payload := &requestPayload{
		Type:  "READ",
		Mbean: mbean,
	}
	if attribute != "" {
		payload.Attribute = &attribute
	}
	if path != "" {
		payload.Path = &path
	}
	if target != nil {
		payload.Target = target
	}
	return payload
}

func performPostRequest(url string, jsonStr []byte) ([]byte, error) {
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("HTTP Request Failed: %v", err)
	}
	defer response.Body.Close()

	body, _ := ioutil.ReadAll(response.Body)

	return body, nil
}
