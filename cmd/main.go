package main

import (
	"VKTestTask/config"
	"VKTestTask/internal/processor"
	"VKTestTask/internal/repository"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
	"sync"
)

func main() {
	// Загрузка конфигурации
	conf, err := config.LoadConfig("config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Подключение к базе данных
	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=%s",
		conf.Database.Host,
		conf.Database.User,
		conf.Database.Password,
		conf.Database.DBName,
		conf.Database.Port,
		conf.Database.SSLMode,
		conf.Database.TimeZone,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Автоматическая миграция базы данных
	if err := db.AutoMigrate(&repository.TDocument{}); err != nil {
		log.Fatalf("Failed to migrate database: %v", err)
	}

	// Настройка Kafka потребителя
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

	// Настройка репозитория и процессора
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

	// Обработка ошибок потребителя
	go func() {
		for err := range partitionConsumer.Errors() {
			log.Printf("Error consuming messages: %v", err)
		}
	}()

	var wg sync.WaitGroup

	// Чтение сообщений из Kafka
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

			// Отправка обработанного сообщения в Kafka
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

			docBytes, _ := json.Marshal(processedDoc)
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
		}(msg)
	}

	wg.Wait()
}
