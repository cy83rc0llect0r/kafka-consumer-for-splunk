package main

import (
        "context"
        "fmt"
        "github.com/Shopify/sarama"
        "github.com/go-resty/resty/v2"
        "log"
        "os"
        "os/signal"
        "sync"
        "syscall"
        "time"
)

func main() {
        // Kafka configuration
        config := sarama.NewConfig()
        config.Version = sarama.V2_1_0_0
        config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
        config.Consumer.Offsets.Initial = sarama.OffsetOldest
        config.Net.DialTimeout = 30 * time.Second

        // Create Kafka consumer group
        group, err := sarama.NewConsumerGroup([]string{"KAFKA_FQDN_OR_IP:KAFKA_PORT"}, "GROUP_ID", config)
        if err != nil {
                log.Fatalf("Failed to create consumer group: %v", err)
        }
        defer group.Close()

        // Splunk HEC configuration
        splunkURL := "https://SPLUNK_HEC_URL:8088/services/collector/raw"
        splunkToken := "SPLUNK_HEC_TOKEN"
        client := resty.New()

        // Create channel to manage system signals
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()
        go func() {
                sigterm := make(chan os.Signal, 1)
                signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
                <-sigterm
                cancel()
        }()

        // Consume messages and send to Splunk
        handler := &consumerGroupHandler{client: client, splunkURL: splunkURL, splunkToken: splunkToken}
        for {
                if err := group.Consume(ctx, []string{"KAFKA_TOPIC"}, handler); err != nil {
                        log.Fatalf("Error from consumer: %v", err)
                }
                if ctx.Err() != nil {
                        return
                }
        }
}

type consumerGroupHandler struct {
        client      *resty.Client
        splunkURL   string
        splunkToken string
        mu          sync.Mutex
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
        log.Println("Consumer group setup")
        return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
        log.Println("Consumer group cleanup")
        return nil
}

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
        log.Println("Consume claim started")
        var wg sync.WaitGroup
        for msg := range claim.Messages() {
                wg.Add(1)
                go func(msg *sarama.ConsumerMessage) {
                        defer wg.Done()
                        logMessage := string(msg.Value)

                        // Print log to console
                        fmt.Printf("Received message: %s\n", logMessage)

                        // Retry mechanism
                        maxRetries := 3
                        for i := 0; i < maxRetries; i++ {
                                response, err := h.client.R().
                                        SetHeader("Authorization", "Splunk "+h.splunkToken).
                                        SetHeader("Content-Type", "application/json").
                                        SetBody(map[string]string{"event": logMessage, "sourcetype": "_json"}).
                                        Post(h.splunkURL)
                                if err != nil {
                                        log.Printf("Failed to send event to Splunk (attempt %d/%d): %v", i+1, maxRetries, err)
                                        time.Sleep(2 * time.Second) // Pause before retrying
                                } else {
                                        fmt.Printf("Sent message to Splunk: %s\n", logMessage)
                                        fmt.Printf("Response: %s\n", response)
                                        h.mu.Lock()
                                        sess.MarkMessage(msg, "")
                                        h.mu.Unlock()
                                        break
                                }
                        }
                }(msg)
        }
        wg.Wait()
        log.Println("Consume claim ended")
        return nil
}
