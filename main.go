package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka struct {
		Brokers  string
		Username string
		Password string
	}
	Server struct {
		Port string
	}
}

var config Config
var urlTopicMap = make(map[string]string)
var kafkaWriter *kafka.Writer
var ritmFeedbackHTML []byte

func loadRitmFeedbackHTML() {
	data, err := os.ReadFile("templates/ritm_feedback.html")
	if err != nil {
		log.Printf("warning: cannot read templates/ritm_feedback.html: %v", err)
		return
	}
	ritmFeedbackHTML = data
}

func buildURLPath(base string, path string) string {
	if path == "" || path == "/" {
		return "/" + base + "/"
	}
	return "/" + base + path
}

func resolvePostTopic(urlPath string, system string) (string, bool) {
	if t, exists := urlTopicMap[urlPath]; exists {
		return t, true
	}

	systemRootPath := "/" + system + "/"
	if _, systemExists := urlTopicMap[systemRootPath]; systemExists {
		return system + "_default", true
	}

	return "", false
}

func resolveGetTopic(urlPath string) (string, bool) {
	bestTopic := ""
	bestLen := -1

	for mappedPath, topic := range urlTopicMap {
		if !strings.HasPrefix(mappedPath, "/get/") {
			continue
		}
		if strings.HasPrefix(urlPath, mappedPath) && len(mappedPath) > bestLen {
			bestLen = len(mappedPath)
			bestTopic = topic
		}
	}

	if bestTopic == "" {
		return "", false
	}
	return bestTopic, true
}

func configPath() string {
	if p := os.Getenv("CONFIG_PATH"); p != "" {
		return p
	}
	return "config.yaml"
}

func applyEnvOverrides() {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		config.Kafka.Brokers = v
	}
	if v := os.Getenv("KAFKA_SASL_USERNAME"); v != "" {
		config.Kafka.Username = v
	}
	if v := os.Getenv("KAFKA_SASL_PASSWORD"); v != "" {
		config.Kafka.Password = v
	}
}

func loadConfig() error {
	data, err := os.ReadFile(configPath())
	if err != nil {
		return err
	}

	var raw struct {
		Kafka struct {
			Brokers      string `yaml:"brokers"`
			SASLUsername string `yaml:"sasl_username"`
			SASLPassword string `yaml:"sasl_password"`
		} `yaml:"kafka"`
		Server struct {
			Port string `yaml:"port"`
		} `yaml:"server"`
		UrlMapping map[string]string `yaml:"url_mapping"`
	}

	if err := yaml.Unmarshal(data, &raw); err != nil {
		return err
	}

	config.Kafka.Brokers = raw.Kafka.Brokers
	config.Kafka.Username = raw.Kafka.SASLUsername
	config.Kafka.Password = raw.Kafka.SASLPassword
	config.Server.Port = raw.Server.Port

	if port := os.Getenv("PORT"); port != "" {
		config.Server.Port = port
	} else {
		config.Server.Port = raw.Server.Port
	}

	for urlPath, topic := range raw.UrlMapping {
		urlTopicMap[urlPath] = topic
	}

	applyEnvOverrides()
	return nil
}

func initKafkaWriter() {
	mechanism := plain.Mechanism{
		Username: config.Kafka.Username,
		Password: config.Kafka.Password,
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
	}

	// Один writer без фиксированного топика — топик передаётся в каждом сообщении.
	// RequireAll: Kafka подтверждает запись всеми репликами перед ответом.
	// MaxAttempts: до 5 попыток при временном разрыве соединения.
	// RoundRobin: все сообщения идут в партиции по очереди, но для строгого
	// FIFO лучше держать 1 партицию в топике на стороне Kafka.
	kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{config.Kafka.Brokers},
		Dialer:       dialer,
		Balancer:     &kafka.RoundRobin{},
		RequiredAcks: int(kafka.RequireAll),
		MaxAttempts:  5,
	})
}

func sendToKafka(topic string, data map[string]interface{}) error {
	message, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// 30 секунд — с запасом на все MaxAttempts при медленном брокере
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return kafkaWriter.WriteMessages(ctx,
		kafka.Message{
			Topic: topic,
			Value: message,
		},
	)
}

func main() {
	if err := loadConfig(); err != nil {
		log.Fatal("Config error:", err)
	}
	loadRitmFeedbackHTML()

	initKafkaWriter()
	defer kafkaWriter.Close()

	gin.SetMode(gin.ReleaseMode) // Выключаем дебаг логи Gin

	r := gin.Default()

	// Только POST для вебхуков
	r.POST("/webhook/:system/*path", func(c *gin.Context) {
		system := c.Param("system")
		path := c.Param("path")

		// Формируем полный путь с системой
		urlPath := buildURLPath(system, path)

		// Логируем сразу при получении — до любой обработки
		log.Printf("webhook received: %s from %s", urlPath, c.ClientIP())

		var data map[string]interface{}
		if err := c.BindJSON(&data); err != nil {
			log.Printf("bad JSON from %s on %s: %v", c.ClientIP(), urlPath, err)
			c.JSON(400, gin.H{"error": "Bad JSON"})
			return
		}

		topic, ok := resolvePostTopic(urlPath, system)
		if !ok {
			log.Printf("unknown system: %s", system)
			c.JSON(404, gin.H{"error": "Unknown system"})
			return
		}

		if err := sendToKafka(topic, data); err != nil {
			log.Printf("kafka error for topic=%s url=%s: %v", topic, urlPath, err)
			c.JSON(500, gin.H{"error": "Kafka error"})
			return
		}

		log.Printf("kafka ok: topic=%s url=%s", topic, urlPath)
		c.JSON(200, gin.H{
			"status":   "ok",
			"topic":    topic,
			"url_path": urlPath,
		})
	})

	// GET для ссылок из писем и других click-tracking сценариев
	r.GET("/get/:system/*path", func(c *gin.Context) {
		system := c.Param("system")
		path := c.Param("path")
		urlPath := buildURLPath("get/"+system, path)

		log.Printf("get webhook received: %s from %s", urlPath, c.ClientIP())

		topic, ok := resolveGetTopic(urlPath)
		if !ok {
			c.JSON(404, gin.H{"error": "Unknown GET path"})
			return
		}

		query := make(map[string]string)
		for key, values := range c.Request.URL.Query() {
			if len(values) > 0 {
				query[key] = values[0]
			}
		}

		data := map[string]interface{}{
			"system":   system,
			"url_path": urlPath,
			"path":     path,
			"query":    query,
		}

		if err := sendToKafka(topic, data); err != nil {
			log.Printf("kafka error for GET topic=%s url=%s: %v", topic, urlPath, err)
			c.JSON(500, gin.H{"error": "Kafka error"})
			return
		}

		log.Printf("kafka ok for GET: topic=%s url=%s", topic, urlPath)
		if topic == "ritm_feedback" && strings.HasPrefix(urlPath, "/get/ritm/feedback") && len(ritmFeedbackHTML) > 0 {
			c.Data(http.StatusOK, "text/html; charset=utf-8", ritmFeedbackHTML)
			return
		}

		c.JSON(200, gin.H{
			"status":   "ok",
			"topic":    topic,
			"url_path": urlPath,
		})
	})

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	srv := &http.Server{
		Addr:         ":" + config.Server.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second, // максимальное время на чтение запроса от клиента
		WriteTimeout: 45 * time.Second, // максимальное время на отправку ответа (> таймаута Kafka)
		IdleTimeout:  60 * time.Second, // максимальное время keep-alive соединения без запросов
	}

	log.Printf("Server starting on :%s", config.Server.Port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("Server error:", err)
	}
}
