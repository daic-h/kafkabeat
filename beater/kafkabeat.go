package beater

import (
	"fmt"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/daichirata/kafkabeat/config"
)

type Kafkabeat struct {
	beatConfig *config.Config
	done       chan struct{}
	period     time.Duration

	client  *KafkaClient
	jClient *JolokiaClient
}

// Creates beater
func New() *Kafkabeat {
	return &Kafkabeat{
		done: make(chan struct{}),
	}
}

/// *** Beater interface methods ***///

func (bt *Kafkabeat) Config(b *beat.Beat) error {
	// Load beater beatConfig
	err := cfgfile.Read(&bt.beatConfig, "")
	if err != nil {
		return fmt.Errorf("Error reading config file: %v", err)
	}

	// Setting default period if not set
	if bt.beatConfig.Kafkabeat.Period == "" {
		bt.beatConfig.Kafkabeat.Period = "1s"
	}

	bt.period, err = time.ParseDuration(bt.beatConfig.Kafkabeat.Period)
	if err != nil {
		return err
	}

	return nil
}

func (bt *Kafkabeat) Setup(b *beat.Beat) error {
	conf := bt.beatConfig.Kafkabeat

	var err error
	bt.client, err = NewKafkaClient(conf.Hosts, conf.ConsumerGroup, conf.Topics)
	if err != nil {
		return err
	}

	if conf.Jolokia.Hosts != nil {
		bt.jClient = NewJolokiaClient(conf.Jolokia.Hosts, &conf.Jolokia.Proxy)
	}

	return nil
}

func (bt *Kafkabeat) Run(b *beat.Beat) error {
	logp.Info("kafkabeat is running! Hit CTRL-C to stop it.")

	ticker := time.NewTicker(bt.period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		timerStart := time.Now()

		events := bt.client.GetOffsetEvents()
		for _, event := range events {
			b.Events.PublishEvent(event)
		}

		if bt.jClient != nil {
			events := bt.jClient.GetJMXEvents()

			for _, event := range events {
				b.Events.PublishEvent(event)
			}
		}

		timerEnd := time.Now()
		duration := timerEnd.Sub(timerStart)
		if duration.Nanoseconds() > bt.period.Nanoseconds() {
			logp.Warn("Ignoring tick(s) due to processing taking longer than one period")
		}
	}
}

func (bt *Kafkabeat) Cleanup(b *beat.Beat) error {
	return bt.client.Close()
}

func (bt *Kafkabeat) Stop() {
	close(bt.done)
}
