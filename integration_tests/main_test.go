package integration_tests

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/modfin/creek"
	"github.com/stretchr/testify/assert"

	"github.com/modfin/creek/integration_tests/env"

	"github.com/sirupsen/logrus"
)

const testNetName = "creek-db-integration_tests-net"

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	// Setup test environment, Docker network and Docker containers
	if err := setupTestEnvironment(ctx); err != nil {
		os.Exit(1)
	}

	LoadSql("base.sql")
	LoadSql("types.sql")
	LoadSql("partitions.sql")

	// Run tests...
	exitCode := m.Run()

	// Shut down test containers
	shutdownTestContainers(ctx)
	cancel()
	os.Exit(exitCode)
}

func setupTestEnvironment(ctx context.Context) error {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	var err error
	env.WD, err = os.Getwd()
	if err != nil {
		logrus.Errorf("Failed to get woriking directory %v", err)
		return err
	}

	// Setup Docker network for containers
	err = setupTestNetwork(testNetName)
	if err != nil {
		logrus.Errorf("Failed to setup Docker network %v", err)
		return err
	}

	// Start test containers
	err = startTestContainers(ctx, testNetName)
	if err != nil {
		logrus.Errorf("Failed to start integration_tests containers %v", err)
		return err
	}
	return nil
}

func setupTest(tb testing.TB) func(tb testing.TB) {
	ctx, cancel := context.WithCancel(tb.Context())
	queue, streamsDone := EnsureStarted(ctx)
	db := GetDBConn()

	var numRows int
	err := db.QueryRow(ctx, "SELECT count(*) FROM public.types_data").Scan(&numRows)
	assert.NoError(tb, err)
	assert.Equal(tb, 1000, numRows)

	return func(tb testing.TB) {
		cancel()
		for {
			select {
			case <-queue.Done():
				return
			case <-streamsDone:
				// Now we can close and wait until everything has published
				queue.Close()
			case <-time.After(1 * time.Second):
				logrus.Info("waiting to shutdown tests")
			}
		}
	}
}

func countMessages(ctx context.Context, stream *creek.WALStream) int {
	messageCtx, cancel := context.WithDeadline(ctx, time.Now().Add(2*time.Second))
	messages := 0
messageLoop:
	for {
		_, err := stream.Next(messageCtx)
		if err != nil {
			break messageLoop
		}
		messages++
	}
	cancel()
	return messages
}
