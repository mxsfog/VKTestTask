package main

import (
	"VKTestTask/config"
	"VKTestTask/internal/processor"
	"VKTestTask/internal/repository"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"os"
	"path/filepath"
	"sync"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	fmt.Println("Current working directory:", cwd)

	// Установить путь к конфигурационному файлу
	configPath := filepath.Join(cwd, "config/config.yaml")

	fmt.Println("Config path:", configPath)

	conf, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Config loaded successfully:", conf)

	// Настройка базы данных SQLite
	db, err := repository.SetupDatabase(conf.Database.DSN)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(conf.Kafka.Brokers, saramaConfig)
	if err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(conf.Kafka.Topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %v", err)
	}

	defer func(partitionConsumer sarama.PartitionConsumer) {
		err := partitionConsumer.Close()
		if err != nil {
			log.Printf("Error closing partition consumer: %v", err)
		}
	}(partitionConsumer)

	repo := repository.NewDatabaseRepository(db)
	proc := processor.NewDocumentProcessor(repo)

	// Добавление нескольких примерных данных через интерфейс
	sampleDocs := []*repository.TDocument{
		{
			Url:       "http://example.com/doc1",
			PubDate:   100,
			FetchTime: 200,
			Text:      "First version",
		},
		{
			Url:       "http://example.com/doc2",
			PubDate:   200,
			FetchTime: 300,
			Text:      "Second version",
		},
		{
			Url:       "http://example.com/doc3",
			PubDate:   300,
			FetchTime: 400,
			Text:      "Third version",
		},
	}

	for _, doc := range sampleDocs {
		if _, err := proc.Process(doc); err != nil {
			log.Fatalf("Failed to process sample document: %v", err)
		}
	}

	go func() {
		for err := range partitionConsumer.Errors() {
			log.Printf("Error consuming messages: %v", err)
		}
	}()

	var wg sync.WaitGroup

	for msg := range partitionConsumer.Messages() {
		wg.Add(1)
		go func(msg *sarama.ConsumerMessage) {
			defer wg.Done()
			var doc repository.TDocument
			if err := json.Unmarshal(msg.Value, &doc); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				return
			}

			processedDoc, err := proc.Process(&doc)
			if err != nil {
				log.Printf("Failed to process document: %v", err)
				return
			}

			log.Printf("Processed document: %+v", processedDoc)
		}(msg)
	}

	wg.Wait()

	producer, err := sarama.NewSyncProducer(conf.Kafka.Brokers, nil)
	if err != nil {
		log.Fatalf("Failed to start producer: %v", err)
	}
	defer func(producer sarama.SyncProducer) {
		err := producer.Close()
		if err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}(producer)

	for _, doc := range sampleDocs {
		docBytes, _ := json.Marshal(doc)
		message := &sarama.ProducerMessage{
			Topic: conf.Kafka.ProcessedTopic,
			Value: sarama.ByteEncoder(docBytes),
		}
		_, _, err = producer.SendMessage(message)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			log.Println("Message sent successfully")
		}
	}
}
