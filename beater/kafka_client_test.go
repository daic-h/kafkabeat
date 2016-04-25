package beater

import (
	"io"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/elastic/beats/libbeat/common"
	"github.com/stretchr/testify/assert"
)

func safeClose(t testing.TB, c io.Closer) {
	err := c.Close()
	if err != nil {
		t.Error(err)
	}
}

func TestNewKafkaClient(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	seedBroker.Returns(new(sarama.MetadataResponse))

	client, err := NewKafkaClient([]string{seedBroker.Addr()}, "test", []string{"test-topic"})
	if err != nil {
		t.Fatal(err)
	}

	seedBroker.Close()
	safeClose(t, client)
}

func TestGetOffsetEvents(t *testing.T) {
	seedBroker := sarama.NewMockBroker(t, 1)
	coordinator := sarama.NewMockBroker(t, 2)
	leader := sarama.NewMockBroker(t, 3)

	initBrokers(seedBroker, coordinator, leader)

	client, err := NewKafkaClient([]string{seedBroker.Addr()}, "test", []string{"test-topic"})
	if err != nil {
		t.Fatal(err)
	}

	events := client.GetOffsetEvents()

	assert := assert.New(t)

	o1 := events[0]["offset"].(common.MapStr)
	assert.Equal("test", o1["group"].(string))
	assert.Equal("test-topic", o1["topic"].(string))
	assert.Equal(int32(0), o1["partition"].(int32))
	assert.Equal(int64(110), o1["broker_offset"].(int64))
	assert.Equal(int64(110), o1["consumer_offset"].(int64))
	assert.Equal(int64(0), o1["lag"].(int64))

	o2 := events[1]["offset"].(common.MapStr)
	assert.Equal("test", o2["group"].(string))
	assert.Equal("test-topic", o2["topic"].(string))
	assert.Equal(int32(1), o2["partition"].(int32))
	assert.Equal(int64(221), o2["broker_offset"].(int64))
	assert.Equal(int64(220), o2["consumer_offset"].(int64))
	assert.Equal(int64(1), o2["lag"].(int64))

	seedBroker.Close()
	coordinator.Close()
	leader.Close()
	safeClose(t, client)
}

func initBrokers(seedBroker, coordinator, leader *sarama.MockBroker) {
	metadateRes := new(sarama.MetadataResponse)
	metadateRes.AddBroker(leader.Addr(), leader.BrokerID())
	metadateRes.AddTopicPartition("test-topic", 0, leader.BrokerID(), nil, nil, sarama.ErrNoError)
	metadateRes.AddTopicPartition("test-topic", 1, leader.BrokerID(), nil, nil, sarama.ErrNoError)
	seedBroker.Returns(metadateRes)

	coordinatorRes := new(sarama.ConsumerMetadataResponse)
	coordinatorRes.CoordinatorID = coordinator.BrokerID()
	coordinatorRes.CoordinatorHost = "127.0.0.1"
	coordinatorRes.CoordinatorPort = coordinator.Port()
	seedBroker.Returns(coordinatorRes)

	offsetRes := new(sarama.OffsetResponse)
	offsetRes.AddTopicPartition("test-topic", 0, 111)
	offsetRes.AddTopicPartition("test-topic", 1, 222)
	leader.Returns(offsetRes)

	offsetFetchRes := new(sarama.OffsetFetchResponse)
	offsetFetchRes.AddBlock("test-topic", 0, &sarama.OffsetFetchResponseBlock{
		Err:      sarama.ErrNoError,
		Offset:   110,
		Metadata: "",
	})
	offsetFetchRes.AddBlock("test-topic", 1, &sarama.OffsetFetchResponseBlock{
		Err:      sarama.ErrNoError,
		Offset:   220,
		Metadata: "",
	})
	coordinator.Returns(offsetFetchRes)
}
