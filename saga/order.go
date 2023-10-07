package main

import (
	"context"
	log2 "dt/log"
	repo2 "dt/repository"
	"github.com/bytedance/sonic"
	"github.com/go-redis/redis/v8"
	"strconv"
	"time"
)

type OrderService struct {
	orderTx  repo2.OrderRepository
	redisCli *redis.Client
}

func NewOrderService(
	orderTx repo2.OrderRepository,
	redisCli *redis.Client,
) *OrderService {
	return &OrderService{
		orderTx:  orderTx,
		redisCli: redisCli,
	}
}

func (o *OrderService) Run(ctx context.Context) {
	var (
		err    error
		logger = log2.GetLogger(ctx)
	)
	pubsub := o.redisCli.Subscribe(ctx, OrderChannel, ReplyChannel)
	if _, err = pubsub.Receive(ctx); err != nil {
		logger.Fatalf("error subscribing %s", err)
	}
	defer func() { _ = pubsub.Close() }()
	ch := pubsub.Channel()

	logger.Infoln("starting the Order service")
	for {
		select {
		case msg := <-ch:
			m := Message{}
			err := sonic.Unmarshal([]byte(msg.Payload), &m)
			if err != nil {
				logger.WithError(err).Errorf("Unmarshal err: %s\n", err)
				continue
			}
			switch msg.Channel {
			case OrderChannel:
				logger.Infof("recieved message with id %s ", m.ID)
				if m.Action == ActionStart {
					m.Action = func() string {
						order, err := o.orderTx.Create(ctx, m.Message.UserId, m.Message.BookId, m.Message.Number)
						if err != nil {
							logger.WithError(err).Errorf("OrderService Create err: %s\n", err)
							return ActionError
						}
						rr := o.redisCli.Set(ctx, m.ID, order.ID, 300*time.Second)
						if rr.Err() != nil {
							return ActionError
						}
						return ActionDone
					}()
					if m.Action != ActionDone {
						m.ServiceErr = ServiceOrder
					}
					if err = o.redisCli.Publish(ctx, ReplyChannel, m).Err(); err != nil {
						logger.WithError(err).Errorf("error publishing done-message to %s channel", ReplyChannel)
						continue
					}
					logger.Infof("done message published to channel :%s", ReplyChannel)
				}
				// Rollback flow
				if m.Action == ActionRollback {
					logger.Infof("rolling back transaction with ID :%s", m.ID)
					id, _ := o.redisCli.Get(ctx, m.ID).Result()
					idd, _ := strconv.Atoi(id)
					err = o.orderTx.Delete(ctx, uint(idd))
					if err != nil {
						logger.WithError(err).Errorf("Delete Order err: %s\n", err)
					} else {
						logger.Infof("rollback order success")
					}
				}
			}
		}
	}
}
