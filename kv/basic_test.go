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

// TestCRDTKeyValueDB_BasicIsolated tests basic operations without network sync
func TestCRDTKeyValueDB_BasicIsolated(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-basic-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := Config{
		NodeServiceName:     "test-service",
		DataStorePath:       filepath.Join(tempDir, "data"),
		DataSyncChannel:     "test-data-sync",
		NetDiscoveryChannel: "test-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	// Test database creation
	db, err := NewCRDTKeyValueDB(ctx, config)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	// Test basic operations
	key := []byte("test-key")
	value := []byte("test-value")

	// Put operation
	err = db.Put(ctx, key, value)
	require.NoError(t, err)

	// Get operation
	retrievedValue, err := db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Has operation
	has, err := db.Has(ctx, key)
	require.NoError(t, err)
	assert.True(t, has)

	// Delete operation
	err = db.Delete(ctx, key)
	require.NoError(t, err)

	// Verify deletion
	has, err = db.Has(ctx, key)
	require.NoError(t, err)
	assert.False(t, has)
}

// TestCRDTKeyValueDB_MultipleKeys tests operations with multiple keys
func TestCRDTKeyValueDB_MultipleKeys(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-multi-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := Config{
		NodeServiceName:     "test-service",
		DataStorePath:       filepath.Join(tempDir, "data"),
		DataSyncChannel:     "test-data-sync",
		NetDiscoveryChannel: "test-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	db, err := NewCRDTKeyValueDB(ctx, config)
	require.NoError(t, err)
	defer db.Close()

	// Test multiple keys
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")

	// Put multiple keys
	err = db.Put(ctx, key1, value1)
	require.NoError(t, err)

	err = db.Put(ctx, key2, value2)
	require.NoError(t, err)

	// Get multiple keys
	retrievedValue1, err := db.Get(ctx, key1)
	require.NoError(t, err)
	assert.Equal(t, value1, retrievedValue1)

	retrievedValue2, err := db.Get(ctx, key2)
	require.NoError(t, err)
	assert.Equal(t, value2, retrievedValue2)

	// Delete one key
	err = db.Delete(ctx, key1)
	require.NoError(t, err)

	// Verify only one key remains
	has1, err := db.Has(ctx, key1)
	require.NoError(t, err)
	assert.False(t, has1)

	has2, err := db.Has(ctx, key2)
	require.NoError(t, err)
	assert.True(t, has2)
}
