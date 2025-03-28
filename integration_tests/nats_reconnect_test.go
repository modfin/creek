package integration_tests

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitForMessages() {
	time.Sleep(300 * time.Millisecond)
}

// TestNatsReconnection tests that the application attempts to reconnect to NATS
// Note: The current implementation uses nats.NoReconnect() option, which means
// the NATS client itself doesn't automatically reconnect. Instead, the application
// has its own reconnection logic that attempts to create a new connection.
func TestNatsReconnection(t *testing.T) {
	// Create a test-specific logger
	testLogger := logrus.New()
	testLogger.SetLevel(logrus.InfoLevel)

	testLogger.Info("Starting NATS reconnection test")

	// Make sure Creek service is running
	EnsureStarted()

	// Create a verifier for the 'other' table
	verifier, err := NewStreamEventVerifier("public", "other")
	assert.NoError(t, err)

	dbConn := GetDBConn()
	require.NotNil(t, dbConn, "Failed to get database connection")

	// Helper function to insert test rows
	insertRows := func(startID, count int) {
		// Insert test rows
		for i := 0; i < count; i++ {
			id := startID + i
			_, err := dbConn.Exec(TimeoutContext(time.Second), "INSERT INTO public.other VALUES ($1, 'new stuff');", id)
			if err != nil {
				testLogger.Warnf("Failed to insert row %d: %v", id, err)
			}
		}
	}

	// Step 1: Verify basic connectivity
	testLogger.Info("Step 1: Verify basic connectivity")
	from := time.Now() // set time to before insert to know which events to consume
	insertRows(1000, 5)
	waitForMessages()

	numberOfEvents, err := verifier.CollectEvents(from, 2*time.Second)
	assert.NoError(t, err, "Initial connectivity check failed")
	assert.Equal(t, 5, numberOfEvents, "Expected 5 messages in initial connectivity test")

	// Step 2: Disrupt connection by stopping NATS container
	testLogger.Info("Step 2: Disrupt connection by stopping NATS container")
	err = StopNatsContainer(t.Context())
	require.NoError(t, err, "Failed to stop NATS container")

	// Wait to ensure NATS is fully stopped
	time.Sleep(1 * time.Second)

	// Step 3: Insert data during disruption (these will fail to publish)
	from = time.Now() // set time to before insert to know which events to consume
	testLogger.Info("Step 3: Insert data during disruption")
	insertRows(2000, 3)
	waitForMessages()

	// Step 4: Restart NATS container
	testLogger.Info("Step 4: Restart NATS container")
	err = StartNatsContainer(t.Context())
	require.NoError(t, err, "Failed to start NATS container")

	time.Sleep(10 * time.Second)

	waitForMessages()

	numberOfEvents, err = verifier.CollectEvents(from, 2*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, 3, numberOfEvents, "No post-reconnection messages received, reconnection may have failed")

	// Step 4: Insert data after reconnection
	testLogger.Info("Step 4: Insert data after reconnection")
	from = time.Now() // set time to before insert to know which events to consume
	insertRows(3000, 5)
	waitForMessages()
	numberOfEvents, err = verifier.CollectEvents(from, 2*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, 5, numberOfEvents, "No messages received, can not send data after reconnection")

	testLogger.Info("NATS reconnection test completed successfully")
}
