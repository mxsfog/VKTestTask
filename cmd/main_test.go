package main

import (
	"VKTestTask/config"
	"VKTestTask/internal/processor"
	"VKTestTask/internal/repository"
	"encoding/json"
	"github.com/IBM/sarama"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func setupTestDB(t *testing.T) (*gorm.DB, func()) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	if err := db.AutoMigrate(&repository.TDocument{}); err != nil {
		t.Fatalf("Failed to migrate database: %v", err)
	}
	return db, func() { db.Exec("DROP TABLE TDocuments") }
}

func TestMainIntegration(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}

	configPath := filepath.Join(cwd, "config/config.yaml")

	conf, err := config.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	db, cleanup := setupTestDB(t)
	defer cleanup()

	repo := repository.NewDatabaseRepository(db)
	proc := processor.NewDocumentProcessor(repo)

	// Test Kafka producer and consumer
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(conf.Kafka.Brokers, saramaConfig)
	if err != nil {
		t.Fatalf("Failed to start consumer: %v", err)
	}

	partitionConsumer, err := consumer.ConsumePartition(conf.Kafka.Topic, 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatalf("Failed to start partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	var wg sync.WaitGroup

	go func() {
		for err := range partitionConsumer.Errors() {
			t.Errorf("Error consuming messages: %v", err)
		}
	}()

	go func() {
		for msg := range partitionConsumer.Messages() {
			wg.Add(1)
			go func(msg *sarama.ConsumerMessage) {
				defer wg.Done()
				var doc repository.TDocument
				if err := json.Unmarshal(msg.Value, &doc); err != nil {
					t.Errorf("Failed to unmarshal message: %v", err)
					return
				}

				processedDoc, err := proc.Process(&doc)
				if err != nil {
					t.Errorf("Failed to process document: %v", err)
					return
				}

				t.Logf("Processed document: %+v", processedDoc)
			}(msg)
		}
	}()

	producer, err := sarama.NewSyncProducer(conf.Kafka.Brokers, nil)
	if err != nil {
		t.Fatalf("Failed to start producer: %v", err)
	}
	defer producer.Close()

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
		docBytes, _ := json.Marshal(doc)
		message := &sarama.ProducerMessage{
			Topic: conf.Kafka.ProcessedTopic,
			Value: sarama.ByteEncoder(docBytes),
		}
		_, _, err = producer.SendMessage(message)
		if err != nil {
			t.Errorf("Failed to send message: %v", err)
		} else {
			t.Log("Message sent successfully")
		}
	}

	wg.Wait()
}
