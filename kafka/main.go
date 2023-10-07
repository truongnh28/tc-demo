package kafka

import (
	"chat-app/pkg/client/kafka/consumer"
	"chat-app/pkg/client/kafka/producer"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

const (
	ProduceMode    = "produce"
	SyncMode       = "sync"
	BatchMode      = "batch"
	MultiAsyncMode = "multiAsync"
	MultiBatchMode = "multiBatch"
)

var mode string
var broker string

func init() {
	flag.StringVar(&mode, "m", "", "cmd mode, 'produce', 'sync', 'batch' or 'multiBatch'")
	flag.StringVar(&broker, "h", "127.0.0.1:9092", "kafka broker host:port")
}

func main() {
	flag.Parse()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	if mode == "" {
		flag.Usage()
		return
	}

	var topic = "test-practice-topic"

	var done = make(chan struct{})
	defer close(done)

	switch mode {
	case ProduceMode:
		producer, err := producer.NewProducer(broker)
		if err != nil {
			panic(err)
		}
		defer producer.Close()
		go producer.StartProduce(done, topic)
	case SyncMode:
		// 1. sync consumer
		consumer, err := consumer.StartSyncConsumer(broker, topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()
	case BatchMode:
		// 2. batch consumer
		consumer, err := consumer.StartBatchConsumer(broker, topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()
	case MultiAsyncMode:
		// 3. multi async consumer
		consumer, err := consumer.StartMultiAsyncConsumer(broker, topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()
	case MultiBatchMode:
		// 4. multi batch consumer
		consumer, err := consumer.StartMultiBatchConsumer(broker, topic)
		if err != nil {
			panic(err)
		}
		defer consumer.Close()
	default:
		flag.Usage()
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	fmt.Println("received signal", <-c)
}
