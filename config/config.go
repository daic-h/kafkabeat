// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

type Config struct {
	Kafkabeat KafkabeatConfig
}

type KafkabeatConfig struct {
	Period        string
	ConsumerGroup string `config:"consumer_group"`
	Topics        []string
	Hosts         []string
	Jolokia       JolokiaConfig
}

type JolokiaConfig struct {
	Hosts []string
	Proxy ProxyConfig
}

type ProxyConfig struct {
	URL      string
	Password string
	User     string
}
