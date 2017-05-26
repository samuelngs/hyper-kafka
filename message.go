package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/samuelngs/hyper/message"
)

// Message is Kafka message broker
type Message struct {
	options  Options
	consumer sarama.Consumer
	producer sarama.AsyncProducer
}

// Config to return kafka client configuration
func (v *Message) Config() *sarama.Config {
	// create configuration
	config := sarama.NewConfig()
	config.ClientID = v.options.ID
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true
	return config
}

// Start to launch the kafka publisher and consumer
func (v *Message) Start() error {
	// create connection client
	client, err := sarama.NewClient(v.options.Addrs, v.Config())
	if err != nil {
		return err
	}
	// create kafka consumer
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return err
	}
	// create kafka producer
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return err
	}
	v.consumer = consumer
	v.producer = producer
	return nil
}

// Stop to close kafka publisher and consumer connection
func (v *Message) Stop() error {
	if v.consumer != nil {
		v.consumer.Close()
	}
	if v.producer != nil {
		v.producer.AsyncClose()
	}
	return nil
}

// Emit to send message to kafka
func (v *Message) Emit(channel, message []byte) error {
	if v.producer != nil {
		pm := &sarama.ProducerMessage{
			Topic: string(channel[:]),
			Value: sarama.ByteEncoder(message),
		}
		select {
		case v.producer.Input() <- pm:
		default:
		}
	}
	return nil
}

// Listen to receive message from kafka
func (v *Message) Listen(channel []byte, fn message.Handler) message.Close {
	connected := v.consumer != nil
	if connected {
		stop := make(chan struct{}, 1)
		partition, err := v.consumer.ConsumePartition(string(channel[:]), 0, sarama.OffsetNewest)
		if err != nil {
			return func() {}
		}
		go func() {
			defer func() {
				partition.Close()
				close(stop)
			}()
		observer:
			for {
				select {
				case msg := <-partition.Messages():
					if msg != nil && msg.Topic == string(channel[:]) && msg.Value != nil && len(msg.Value) != 0 {
						fn(msg.Value)
					}
				case <-stop:
					break observer
				default:
				}
			}
		}()
		return func() {
			stop <- struct{}{}
		}
	}
	return func() {}
}

func (v *Message) String() string {
	return "Hyper::Kafka"
}
