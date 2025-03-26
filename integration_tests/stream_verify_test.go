package integration_tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStreamEventVerifier(t *testing.T) {
	// Initialize the test environment
	EnsureStarted()
	db := GetDBConn()

	// Create a verifier for the 'other' table
	verifier, err := NewStreamEventVerifier("public", "other")
	assert.NoError(t, err)

	from := time.Now().Add(-1 * time.Second)
	// Perform some database operations that will generate events
	_, err = db.Exec(TimeoutContext(time.Second), "INSERT INTO public.other VALUES (3, 'verify test');")
	assert.NoError(t, err)

	_, err = db.Exec(TimeoutContext(time.Second), "UPDATE public.other SET data='updated value' WHERE id=3;")
	assert.NoError(t, err)

	// Wait a short time for events to be published
	time.Sleep(100 * time.Millisecond)

	// Collect events with a timeout
	count, err := verifier.CollectEvents(from, 2*time.Second)
	assert.NoError(t, err)

	// Log the number of events collected for debugging
	t.Logf("Collected %d events", count)

	// Verify we have events
	assert.Equal(t, 2, count, "Expected 2 events (INSERT and UPDATE)")

	// Clean up - delete the test record
	_, err = db.Exec(TimeoutContext(time.Second), "DELETE FROM public.other WHERE id=3;")
	assert.NoError(t, err)
}

// Example showing how to use the simplified VerifyPublishedEvents function
func TestSimpleEventVerification(t *testing.T) {
	// Initialize the test environment
	EnsureStarted()
	db := GetDBConn()
	from := time.Now().Add(-1 * time.Second)
	// Perform a database operation
	_, err := db.Exec(TimeoutContext(time.Second), "INSERT INTO public.other VALUES (4, 'simple verify');")
	assert.NoError(t, err)

	// Wait a short time for events to be published
	time.Sleep(100 * time.Millisecond)

	// Verify events were published
	count, err := VerifyPublishedEvents("public", "other", 2*time.Second, from)
	assert.NoError(t, err)

	// Verify we got at least one event
	assert.True(t, count > 0, "Expected to receive at least one event")
	t.Logf("Received %d events", count)

	// Clean up
	_, err = db.Exec(TimeoutContext(time.Second), "DELETE FROM public.other WHERE id=4;")
	assert.NoError(t, err)
}
