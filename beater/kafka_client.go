package beater

import (
	// "log"
	// "os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
)

type KafkaClient struct {
	client sarama.Client
	group  string
	topics []string
}

type Offset struct {
	Group          string
	Topic          string
	Partition      int32
	ConsumerOffset int64
	BrokerOffset   int64
}

type consumerOffsetRequest struct {
	partitions topicPartitions
	request    *sarama.OffsetFetchRequest
}

type brokerOffsetRequest struct {
	partitions topicPartitions
	request    *sarama.OffsetRequest
}

type topicPartitions map[string][]int32
type partitionOffset map[int32]int64
type partitionOffsets map[string]partitionOffset

func NewKafkaClient(hosts []string, group string, topics []string) (*KafkaClient, error) {
	// sarama.Logger = log.New(os.Stderr, "", log.LstdFlags)
	client, err := sarama.NewClient(hosts, sarama.NewConfig())
	if err != nil {
		return nil, err
	}

	return &KafkaClient{client: client, group: group, topics: topics}, nil
}

func (c *KafkaClient) Close() error {
	return c.client.Close()
}

func (c *KafkaClient) GetOffsetEvents() []common.MapStr {
	var events []common.MapStr

	offsets, err := c.fetchOffsets()
	if err != nil {
		logp.Err("Failed to read kafka status: %v", err)
		return events
	}

	for _, o := range offsets {
		offset := getOffsetEvent(o)

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       "offset",
			"offset":     offset,
		}

		events = append(events, event)
	}

	return events
}

func getOffsetEvent(o *Offset) common.MapStr {
	return common.MapStr{
		"group":           o.Group,
		"topic":           o.Topic,
		"partition":       o.Partition,
		"consumer_offset": o.ConsumerOffset,
		"broker_offset":   o.BrokerOffset,
		"lag":             o.BrokerOffset - o.ConsumerOffset,
	}
}

func positiveNum(o int64) int64 {
	if o < 0 {
		return 0
	}
	return o
}

func (c *KafkaClient) fetchOffsets() ([]*Offset, error) {
	tp, err := c.topicPartitions()
	if err != nil {
		return nil, err
	}

	co, err := c.fetchConsumerOffsets(tp)
	if err != nil {
		return nil, err
	}

	bo, err := c.fetchBrokerOffsets(tp)
	if err != nil {
		return nil, err
	}

	var offsets []*Offset
	for topic, partitions := range tp {
		for _, partition := range partitions {
			offset := &Offset{
				Group:          c.group,
				Topic:          topic,
				Partition:      partition,
				ConsumerOffset: positiveNum(co[topic][partition]),
				BrokerOffset:   positiveNum(bo[topic][partition]),
			}
			offsets = append(offsets, offset)
		}
	}
	return offsets, nil
}

func (c *KafkaClient) topicPartitions() (topicPartitions, error) {
	topicPartitions := make(topicPartitions)
	for _, topic := range c.topics {
		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return nil, err
		}
		topicPartitions[topic] = partitions
	}
	return topicPartitions, nil
}

func (c *KafkaClient) fetchConsumerOffsets(tp topicPartitions) (partitionOffsets, error) {
	broker, err := c.client.Coordinator(c.group)
	if err != nil {
		return nil, err
	}

	request := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: c.group,
	}
	for topic, partitions := range tp {
		for _, p := range partitions {
			request.AddPartition(topic, p)
		}
	}

	response, err := broker.FetchOffset(request)
	if err != nil {
		return nil, err
	}

	offsets := make(partitionOffsets)
	for topic, partitions := range tp {
		for _, partition := range partitions {
			block := response.GetBlock(topic, partition)
			if block == nil {
				continue
			}
			if block.Err != sarama.ErrNoError {
				return nil, block.Err
			}

			if offsets[topic] == nil {
				offsets[topic] = make(partitionOffset)
			}
			offsets[topic][partition] = block.Offset
		}
	}

	return offsets, nil
}

func newBrokerOffsetRequest() *brokerOffsetRequest {
	return &brokerOffsetRequest{
		partitions: make(map[string][]int32),
		request:    &sarama.OffsetRequest{},
	}
}

func (c *brokerOffsetRequest) addBlock(topic string, partition int32) {
	c.request.AddBlock(topic, partition, sarama.OffsetNewest, 1)
	c.partitions[topic] = append(c.partitions[topic], partition)
}

func (c *KafkaClient) fetchBrokerOffsets(tp topicPartitions) (partitionOffsets, error) {
	requests := make(map[*sarama.Broker]*brokerOffsetRequest)

	for topic, partitions := range tp {
		for _, partition := range partitions {
			broker, err := c.client.Leader(topic, partition)
			if err != nil {
				return nil, err
			}
			if _, ok := requests[broker]; !ok {
				requests[broker] = newBrokerOffsetRequest()
			}

			requests[broker].addBlock(topic, partition)
		}
	}

	offsets := make(partitionOffsets)

	for broker, r := range requests {
		response, err := broker.GetAvailableOffsets(r.request)
		if err != nil {
			return nil, err
		}

		for topic, partitions := range r.partitions {
			for _, partition := range partitions {
				block := response.GetBlock(topic, partition)
				if block == nil {
					return nil, sarama.ErrIncompleteResponse
				}
				if block.Err != sarama.ErrNoError {
					return nil, block.Err
				}

				if offsets[topic] == nil {
					offsets[topic] = make(partitionOffset)
				}
				offsets[topic][partition] = block.Offsets[0] - 1
			}
		}
	}

	return offsets, nil
}
