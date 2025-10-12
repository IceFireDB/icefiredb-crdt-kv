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

// TestCRDTKeyValueDB_MultiNodeSync tests basic CRDT synchronization between multiple nodes
func TestCRDTKeyValueDB_MultiNodeSync(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping multi-node sync test in short mode")
	}

	// Create temporary directories for two nodes
	tempDir1, err := os.MkdirTemp("", "icefiredb-node1-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir1)

	tempDir2, err := os.MkdirTemp("", "icefiredb-node2-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Create first node
	config1 := Config{
		NodeServiceName:     "test-service",
		DataStorePath:       filepath.Join(tempDir1, "data"),
		DataSyncChannel:     "test-data-sync",
		NetDiscoveryChannel: "test-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	node1, err := NewCRDTKeyValueDB(ctx, config1)
	require.NoError(t, err)
	require.NotNil(t, node1)
	defer node1.Close()

	// Create second node
	config2 := Config{
		NodeServiceName:     "test-service",
		DataStorePath:       filepath.Join(tempDir2, "data"),
		DataSyncChannel:     "test-data-sync",
		NetDiscoveryChannel: "test-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	node2, err := NewCRDTKeyValueDB(ctx, config2)
	require.NoError(t, err)
	require.NotNil(t, node2)
	defer node2.Close()

	// Give nodes time to discover each other
	time.Sleep(5 * time.Second)

	// Test basic put/get operations
	key := []byte("sync-test-key")
	value := []byte("sync-test-value")

	// Put on node1
	err = node1.Put(ctx, key, value)
	require.NoError(t, err)

	// Give time for sync
	time.Sleep(2 * time.Second)

	// Get from node2 (should be synced)
	retrievedValue, err := node2.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Test delete operation
	err = node2.Delete(ctx, key)
	require.NoError(t, err)

	// Give time for sync
	time.Sleep(2 * time.Second)

	// Verify deletion on node1
	has, err := node1.Has(ctx, key)
	require.NoError(t, err)
	assert.False(t, has)
}

// TestCRDTKeyValueDB_ConcurrentWrites tests concurrent writes to the same key from multiple nodes
func TestCRDTKeyValueDB_ConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent writes test in short mode")
	}

	// Create temporary directories for two nodes
	tempDir1, err := os.MkdirTemp("", "icefiredb-concurrent1-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir1)

	tempDir2, err := os.MkdirTemp("", "icefiredb-concurrent2-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Create first node
	config1 := Config{
		NodeServiceName:     "test-service",
		DataStorePath:       filepath.Join(tempDir1, "data"),
		DataSyncChannel:     "test-data-sync",
		NetDiscoveryChannel: "test-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	node1, err := NewCRDTKeyValueDB(ctx, config1)
	require.NoError(t, err)
	require.NotNil(t, node1)
	defer node1.Close()

	// Create second node
	config2 := Config{
		NodeServiceName:     "test-service",
		DataStorePath:       filepath.Join(tempDir2, "data"),
		DataSyncChannel:     "test-data-sync",
		NetDiscoveryChannel: "test-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	node2, err := NewCRDTKeyValueDB(ctx, config2)
	require.NoError(t, err)
	require.NotNil(t, node2)
	defer node2.Close()

	// Give nodes time to discover each other
	time.Sleep(5 * time.Second)

	// Test concurrent writes to the same key
	key := []byte("concurrent-key")
	value1 := []byte("value-from-node1")
	value2 := []byte("value-from-node2")

	// Write from both nodes concurrently
	err = node1.Put(ctx, key, value1)
	require.NoError(t, err)

	err = node2.Put(ctx, key, value2)
	require.NoError(t, err)

	// Give time for conflict resolution and sync
	time.Sleep(3 * time.Second)

	// Both nodes should have the same value (CRDT conflict resolution)
	finalValue1, err := node1.Get(ctx, key)
	require.NoError(t, err)

	finalValue2, err := node2.Get(ctx, key)
	require.NoError(t, err)

	// Values should be the same on both nodes (eventual consistency)
	assert.Equal(t, finalValue1, finalValue2)
}

// TestCRDTKeyValueDB_Isolated tests basic operations in isolated mode (no network sync)
func TestCRDTKeyValueDB_Isolated(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-isolated-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	config := Config{
		NodeServiceName:     "isolated-test-service",
		DataStorePath:       filepath.Join(tempDir, "data"),
		DataSyncChannel:     "isolated-data-sync",
		NetDiscoveryChannel: "isolated-net-discovery",
		Namespace:           "test-namespace",
		Logger:              logger,
	}

	// Test database creation
	db, err := NewCRDTKeyValueDB(ctx, config)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	// Test basic operations
	key := []byte("isolated-key")
	value := []byte("isolated-value")

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
