package integration_tests

import (
	"context"
	"fmt"
	"time"

	"github.com/modfin/creek"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamEventVerifier provides functionality to verify events on a NATS stream
type StreamEventVerifier struct {
	natsURL     string
	streamName  string
	tableSchema string
	tableName   string
}

// NewStreamEventVerifier creates a new verifier for the specified table
func NewStreamEventVerifier(tableSchema, tableName string) (*StreamEventVerifier, error) {
	// Ensure Creek is started
	EnsureStarted()

	// Get the NATS server URL and stream name from the configuration
	cfg := GetConfig()
	streamName := fmt.Sprintf("%s_%s_%s", cfg.NatsConfig.NameSpace, creek.WalStream, DBname)

	return &StreamEventVerifier{
		streamName:  streamName,
		tableSchema: tableSchema,
		tableName:   tableName,
	}, nil
}

// messageResult represents a result from the message iterator
type messageResult struct {
	msg jetstream.Msg
	err error
}

// CollectEvents collects all events that occurred since StartCapturing was called
// with an optional timeout for how long to wait for events
func (v *StreamEventVerifier) CollectEvents(from time.Time, timeout time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Create a new NATS connection each time
	nc, err := nats.Connect(GetNATSURL())
	if err != nil {
		return 0, fmt.Errorf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// Create JetStream context
	js, err := jetstream.New(nc)
	if err != nil {
		return 0, fmt.Errorf("failed to create JetStream context: %v", err)
	}

	// Get the stream
	stream, err := js.Stream(ctx, v.streamName)
	if err != nil {
		return 0, fmt.Errorf("failed to get stream: %v", err)
	}

	// Set up the subject for the consumer to filter on
	subject := fmt.Sprintf("%s.%s.%s", v.streamName, v.tableSchema, v.tableName)

	// Create an ordered consumer
	consumer, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects: []string{subject},
		DeliverPolicy:  jetstream.DeliverByStartTimePolicy,
		OptStartTime:   &from,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create consumer: %v", err)
	}
	// Get message iterator
	iter, err := consumer.Messages()
	if err != nil {
		return 0, fmt.Errorf("failed to get message iterator: %v", err)
	}
	defer iter.Stop()

	// Create a channel for receiving messages from the goroutine
	msgChan := make(chan messageResult)

	// Start a goroutine to fetch messages and send them to the channel
	go func() {
		for {
			// Check if context is done before trying to fetch a message
			if ctx.Err() != nil {
				close(msgChan)
				return
			}

			// Try to fetch the next message
			msg, err := iter.Next()

			// Send the result to the channel
			select {
			case msgChan <- messageResult{msg: msg, err: err}:
				// Successfully sent to channel
			case <-ctx.Done():
				// Context was canceled while we were trying to send
				close(msgChan)
				return
			}
		}
	}()

	// Count events until timeout
	count := 0
	timeoutChan := time.After(timeout - 100*time.Millisecond) // Slightly shorter than context timeout

	// Collect messages until timeout or context cancellation
	for {
		select {
		case <-timeoutChan:
			// We've reached our timeout
			cancel() // Cancel the context to stop the goroutine
			return count, nil

		case <-ctx.Done():
			// Context was canceled (could be parent timeout)
			return count, nil

		case result, ok := <-msgChan:
			// Channel was closed
			if !ok {
				return count, nil
			}

			// Check for errors
			if result.err != nil {
				continue
			}

			// Count the message
			count++

			// Acknowledge the message
			if err := result.msg.Ack(); err != nil {
				cancel() // Cancel context to stop goroutine
				return count, fmt.Errorf("failed to acknowledge message: %v", err)
			}
		}
	}
}

// VerifyPublishedEvents is a utility function that creates a verifier, waits for events,
// and returns the count for the specified table
func VerifyPublishedEvents(tableSchema, tableName string, timeout time.Duration, from time.Time) (int, error) {
	verifier, err := NewStreamEventVerifier(tableSchema, tableName)
	if err != nil {
		return 0, err
	}
	return verifier.CollectEvents(from, timeout)
}
