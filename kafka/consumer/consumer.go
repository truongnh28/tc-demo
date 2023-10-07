package consumer

import (
	"chat-app/pkg/client/kafka/producer"
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
)

type GroupHandler interface {
	sarama.ConsumerGroupHandler
	WaitReady()
	Reset()
}

type Group struct {
	cg sarama.ConsumerGroup
}

func NewConsumerGroup(
	broker string,
	topics []string,
	group string,
	handler GroupHandler,
) (*Group, error) {
	ctx := context.Background()
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V0_10_2_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	client, err := sarama.NewConsumerGroup([]string{broker}, group, cfg)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			err := client.Consume(ctx, topics, handler)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					panic(err)
				}
			}
			if ctx.Err() != nil {
				return
			}
			handler.Reset()
		}
	}()

	handler.WaitReady() // Await till the consumer has been set up

	return &Group{
		cg: client,
	}, nil
}

func (c *Group) Close() error {
	return c.cg.Close()
}

type SessionMessage struct {
	Session sarama.ConsumerGroupSession
	Message *sarama.ConsumerMessage
}

func decodeMessage(data []byte) error {
	var msg producer.Message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return err
	}
	return nil
}

func StartSyncConsumer(broker, topic string) (*Group, error) {
	var count int64
	var start = time.Now()
	handler := NewSyncConsumerGroupHandler(func(data []byte) error {
		if err := decodeMessage(data); err != nil {
			return err
		}
		count++
		if count%5000 == 0 {
			fmt.Printf(
				"sync consumer consumed %d messages at speed %.2f/s\n",
				count,
				float64(count)/time.Since(start).Seconds(),
			)
		}
		return nil
	})
	consumer, err := NewConsumerGroup(
		broker,
		[]string{topic},
		"sync-consumer-"+fmt.Sprintf("%d", time.Now().Unix()),
		handler,
	)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func StartBatchConsumer(broker, topic string) (*Group, error) {
	var count int64
	var start = time.Now()
	handler := NewBatchConsumerGroupHandler(&BatchConsumerConfig{
		MaxBufSize: 1000,
		Callback: func(messages []*SessionMessage) error {
			for i := range messages {
				if err := decodeMessage(messages[i].Message.Value); err == nil {
					messages[i].Session.MarkMessage(messages[i].Message, "")
				}
			}
			count += int64(len(messages))
			if count%5000 == 0 {
				fmt.Printf(
					"batch consumer consumed %d messages at speed %.2f/s\n",
					count,
					float64(count)/time.Since(start).Seconds(),
				)
			}
			return nil
		},
	})
	consumer, err := NewConsumerGroup(
		broker,
		[]string{topic},
		"batch-consumer-"+fmt.Sprintf("%d", time.Now().Unix()),
		handler,
	)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func StartMultiAsyncConsumer(broker, topic string) (*Group, error) {
	var count int64
	var start = time.Now()
	var bufChan = make(chan *SessionMessage, 1000)
	for i := 0; i < 8; i++ {
		go func() {
			for message := range bufChan {
				if err := decodeMessage(message.Message.Value); err == nil {
					message.Session.MarkMessage(message.Message, "")
				}
				cur := atomic.AddInt64(&count, 1)
				if cur%5000 == 0 {
					fmt.Printf(
						"multi async consumer consumed %d messages at speed %.2f/s\n",
						cur,
						float64(cur)/time.Since(start).Seconds(),
					)
				}
			}
		}()
	}
	handler := NewMultiAsyncConsumerGroupHandler(&MultiAsyncConsumerConfig{
		BufChan: bufChan,
	})
	consumer, err := NewConsumerGroup(
		broker,
		[]string{topic},
		"multi-async-consumer-"+fmt.Sprintf("%d", time.Now().Unix()),
		handler,
	)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func StartMultiBatchConsumer(broker, topic string) (*Group, error) {
	var count int64
	var start = time.Now()
	var bufChan = make(chan batchMessages, 1000)
	for i := 0; i < 8; i++ {
		go func() {
			for messages := range bufChan {
				for j := range messages {
					if err := decodeMessage(messages[j].Message.Value); err == nil {
						messages[j].Session.MarkMessage(messages[j].Message, "")
					}
				}
				cur := atomic.AddInt64(&count, int64(len(messages)))
				if cur%1000 == 0 {
					fmt.Printf(
						"multi batch consumer consumed %d messages at speed %.2f/s\n",
						cur,
						float64(cur)/time.Since(start).Seconds(),
					)
				}
			}
		}()
	}
	handler := NewMultiBatchConsumerGroupHandler(&MultiBatchConsumerConfig{
		MaxBufSize: 1000,
		BufChan:    bufChan,
	})
	consumer, err := NewConsumerGroup(
		broker,
		[]string{topic},
		"multi-batch-consumer-"+fmt.Sprintf("%d", time.Now().Unix()),
		handler,
	)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
