package kafkapc

import (
	"log"
	"os"
	"os/signal"
	"fmt"

	"github.com/Shopify/sarama"
)

//CreateKafkaProducer creates asynchronous producers
func CreateKafkaProducer(brokers []string) (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionNone
	// config.Producer.MaxMessageBytes = 1000000 //1GB. Maximum permitted size of a message (defaults to 1GB). MaxMessageBytes <= broker's `message.max.bytes`.
	// config.Producer.Flush.Messages = 1        //Maximum number of messages the producer will send in a single broker request. Defaults to 0 for unlimited.
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	//Relay incoming signals to channel 'c'
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)

	//Terminate the producer gracefully upon receiving a kill signal.
	//This is a must to prevent memory leak.
	go func() {
		sig := <-c //Block until a signal is received
		log.Println("Got signal:", sig)

		if err := producer.Close(); err != nil {
			log.Fatal("Error closing async producer:", err)
		}

		log.Println("Async Producer closed.")
		fmt.Println("hi jus jisu i j.")
		os.Exit(1)
	}()

	//Read from the Errors() channel to avoid producer deadlock
	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write message to topic:", err)
		}
	}()

	return producer, nil
}
