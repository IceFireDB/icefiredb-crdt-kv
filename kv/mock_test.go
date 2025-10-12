package kv

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCRDTKeyValueDB_Mock tests basic operations with minimal P2P setup
func TestCRDTKeyValueDB_Mock(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-mock-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := Config{
		NodeServiceName:     "mock-test-service",
		DataStorePath:       filepath.Join(tempDir, "data"),
		DataSyncChannel:     "mock-data-sync",
		NetDiscoveryChannel: "mock-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	// Test database creation
	db, err := NewCRDTKeyValueDB(ctx, config)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Test basic operations with a short timeout
	testCtx, testCancel := context.WithTimeout(ctx, 5*time.Second)
	defer testCancel()

	// Test basic operations
	key := []byte("mock-key")
	value := []byte("mock-value")

	// Put operation
	err = db.Put(testCtx, key, value)
	require.NoError(t, err)

	// Get operation
	retrievedValue, err := db.Get(testCtx, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Has operation
	has, err := db.Has(testCtx, key)
	require.NoError(t, err)
	assert.True(t, has)

	// Close the database
	db.Close()
}

// TestCRDTKeyValueDB_Fast tests very basic operations with minimal setup
func TestCRDTKeyValueDB_Fast(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-fast-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := Config{
		NodeServiceName:     "fast-test-service",
		DataStorePath:       filepath.Join(tempDir, "data"),
		DataSyncChannel:     "fast-data-sync",
		NetDiscoveryChannel: "fast-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	// Test database creation only - don't perform operations
	db, err := NewCRDTKeyValueDB(ctx, config)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Just verify the database was created and can be closed
	db.Close()
}
