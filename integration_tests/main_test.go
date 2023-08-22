package integration_tests

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/modfin/creek/integration_tests/env"

	"github.com/sirupsen/logrus"
)

const testNetName = "creek-db-integration_tests-net"

func TestMain(m *testing.M) {
	var cancel context.CancelFunc
	testCtx, cancel = context.WithCancel(context.Background())

	// Setup test environment, Docker network and Docker containers
	if err := setupTestEnvironment(context.Background()); err != nil {
		os.Exit(1)
	}

	LoadSql("base.sql")
	LoadSql("types.sql")

	// Run tests...
	exitCode := m.Run()
	cancel()

	// Shut down test containers
	shutdownTestContainers(context.Background())
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
