package beater

import (
	"testing"

	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/daichirata/kafkabeat/config"
	"github.com/elastic/beats/libbeat/common"
	"github.com/stretchr/testify/assert"
)

func TestGetJMXEvents(t *testing.T) {
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `
[
    {
        "request": {
            "mbean": "kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics",
            "type": "read"
        },
        "value": {
            "RateUnit": "SECONDS",
            "EventType": "messages",
            "Count": 1,
            "OneMinuteRate": 1.1,
            "FiveMinuteRate": 1.2,
            "FifteenMinuteRate": 1.3,
            "MeanRate": 1.4
        },
        "timestamp": 1462174414,
        "status": 200
    },
    {
        "request": {
            "mbean": "kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics",
            "type": "read"
        },
        "value": {
            "RateUnit": "SECONDS",
            "EventType": "bytes",
            "Count": 2,
            "OneMinuteRate": 2.1,
            "FiveMinuteRate": 2.2,
            "FifteenMinuteRate": 2.3,
            "MeanRate": 2.4
        },
        "timestamp": 1462174414,
        "status": 200
    },
    {
        "request": {
            "mbean": "kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics",
            "type": "read"
        },
        "value": {
            "RateUnit": "SECONDS",
            "EventType": "bytes",
            "Count": 3,
            "OneMinuteRate": 3.1,
            "FiveMinuteRate": 3.2,
            "FifteenMinuteRate": 3.3,
            "MeanRate": 3.4
        },
        "timestamp": 1462174414,
        "status": 200
    },
    {
        "request": {
            "mbean": "kafka.server:name=BytesRejectedPerSec,type=BrokerTopicMetrics",
            "type": "read"
        },
        "value": {
            "RateUnit": "SECONDS",
            "EventType": "bytes",
            "Count": 4,
            "OneMinuteRate": 4.1,
            "FiveMinuteRate": 4.2,
            "FifteenMinuteRate": 4.3,
            "MeanRate": 4.4
        },
        "timestamp": 1462174414,
        "status": 200
    },
    {
        "request": {
            "mbean": "kafka.server:name=FailedProduceRequestsPerSec,type=BrokerTopicMetrics",
            "type": "read"
        },
        "value": {
            "RateUnit": "SECONDS",
            "EventType": "requests",
            "Count": 5,
            "OneMinuteRate": 5.1,
            "FiveMinuteRate": 5.2,
            "FifteenMinuteRate": 5.3,
            "MeanRate": 5.4
        },
        "timestamp": 1462174414,
        "status": 200
    },
    {
        "request": {
            "mbean": "kafka.server:name=FailedFetchRequestsPerSec,type=BrokerTopicMetrics",
            "type": "read"
        },
        "value": {
            "RateUnit": "SECONDS",
            "EventType": "requests",
            "Count": 6,
            "OneMinuteRate": 6.1,
            "FiveMinuteRate": 6.2,
            "FifteenMinuteRate": 6.3,
            "MeanRate": 6.4
        },
        "timestamp": 1462174414,
        "status": 200
    }
]
`)
	}))
	defer ts1.Close()

	client := NewJolokiaClient([]string{ts1.URL}, &config.ProxyConfig{})

	events := client.GetJMXEvents()
	jmx := events[0]["jmx"].(common.MapStr)
	assert := assert.New(t)

	assert.Equal(common.MapStr{
		"Count":             int64(1),
		"OneMinuteRate":     float64(1.1),
		"FiveMinuteRate":    float64(1.2),
		"FifteenMinuteRate": float64(1.3),
		"MeanRate":          float64(1.4),
	}, jmx["MessagesInPerSec"].(common.MapStr))

	assert.Equal(common.MapStr{
		"Count":             int64(2),
		"OneMinuteRate":     float64(2.1),
		"FiveMinuteRate":    float64(2.2),
		"FifteenMinuteRate": float64(2.3),
		"MeanRate":          float64(2.4),
	}, jmx["BytesInPerSec"].(common.MapStr))

	assert.Equal(common.MapStr{
		"Count":             int64(3),
		"OneMinuteRate":     float64(3.1),
		"FiveMinuteRate":    float64(3.2),
		"FifteenMinuteRate": float64(3.3),
		"MeanRate":          float64(3.4),
	}, jmx["BytesOutPerSec"].(common.MapStr))

	assert.Equal(common.MapStr{
		"Count":             int64(4),
		"OneMinuteRate":     float64(4.1),
		"FiveMinuteRate":    float64(4.2),
		"FifteenMinuteRate": float64(4.3),
		"MeanRate":          float64(4.4),
	}, jmx["BytesRejectedPerSec"].(common.MapStr))

	assert.Equal(common.MapStr{
		"Count":             int64(5),
		"OneMinuteRate":     float64(5.1),
		"FiveMinuteRate":    float64(5.2),
		"FifteenMinuteRate": float64(5.3),
		"MeanRate":          float64(5.4),
	}, jmx["FailedProduceRequestsPerSec"].(common.MapStr))

	assert.Equal(common.MapStr{
		"Count":             int64(6),
		"OneMinuteRate":     float64(6.1),
		"FiveMinuteRate":    float64(6.2),
		"FifteenMinuteRate": float64(6.3),
		"MeanRate":          float64(6.4),
	}, jmx["FailedFetchRequestsPerSec"].(common.MapStr))
}
