package kv

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsFileExist(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{
			name:     "non-existent file",
			path:     "/tmp/non-existent-file-12345",
			expected: false,
		},
		{
			name:     "existing directory",
			path:     "/tmp",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsFileExist(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCRDTKeyValueDB_BasicOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Reduce log noise during tests

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

	// Test Put operation
	key := []byte("test-key")
	value := []byte("test-value")

	err = db.Put(ctx, key, value)
	require.NoError(t, err)

	// Test Get operation
	retrievedValue, err := db.Get(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Test Has operation
	has, err := db.Has(ctx, key)
	require.NoError(t, err)
	assert.True(t, has)

	// Test Has with non-existent key
	has, err = db.Has(ctx, []byte("non-existent-key"))
	require.NoError(t, err)
	assert.False(t, has)

	// Test Query operation
	results, err := db.Query(ctx, query.Query{})
	require.NoError(t, err)

	var entries []query.Entry
	for result := range results.Next() {
		if result.Error != nil {
			t.Fatalf("query error: %v", result.Error)
		}
		entries = append(entries, result.Entry)
	}

	// Should have at least one entry
	assert.GreaterOrEqual(t, len(entries), 1)

	// Test Delete operation
	err = db.Delete(ctx, key)
	require.NoError(t, err)

	// Verify deletion
	has, err = db.Has(ctx, key)
	require.NoError(t, err)
	assert.False(t, has)

	// Test Get after deletion
	_, err = db.Get(ctx, key)
	assert.Error(t, err)
}

func TestCRDTKeyValueDB_BatchOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-batch-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	// Test batch operations
	batch, err := db.Batch(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)

	// Add multiple operations to batch
	key1 := []byte("batch-key-1")
	value1 := []byte("batch-value-1")
	key2 := []byte("batch-key-2")
	value2 := []byte("batch-value-2")

	err = batch.Put(ctx, ds.NewKey(string(key1)), value1)
	require.NoError(t, err)

	err = batch.Put(ctx, ds.NewKey(string(key2)), value2)
	require.NoError(t, err)

	// Commit batch
	err = batch.Commit(ctx)
	require.NoError(t, err)

	// Verify batch operations
	retrievedValue1, err := db.Get(ctx, key1)
	require.NoError(t, err)
	assert.Equal(t, value1, retrievedValue1)

	retrievedValue2, err := db.Get(ctx, key2)
	require.NoError(t, err)
	assert.Equal(t, value2, retrievedValue2)
}

func TestCRDTKeyValueDB_QueryWithFilters(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-query-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	// Insert test data with prefixes
	testData := map[string][]byte{
		"prefix1-key1": []byte("value1"),
		"prefix1-key2": []byte("value2"),
		"prefix2-key1": []byte("value3"),
		"other-key":    []byte("value4"),
	}

	for key, value := range testData {
		err = db.Put(ctx, []byte(key), value)
		require.NoError(t, err)
	}

	// Test query with prefix filter
	q := query.Query{
		Filters: []query.Filter{
			query.FilterKeyPrefix{
				Prefix: "prefix1",
			},
		},
	}

	results, err := db.Query(ctx, q)
	require.NoError(t, err)

	var prefix1Entries []query.Entry
	for result := range results.Next() {
		if result.Error != nil {
			t.Fatalf("query error: %v", result.Error)
		}
		prefix1Entries = append(prefix1Entries, result.Entry)
	}

	// Should have 2 entries with prefix1
	assert.Len(t, prefix1Entries, 2)
}

func TestCRDTKeyValueDB_StoreAndDBMethods(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	// Test Store() method
	store := db.Store()
	require.NotNil(t, store)

	// Test DB() method
	badgerDB := db.DB()
	require.NotNil(t, badgerDB)

	// Verify DB is accessible
	key := []byte("store-test-key")
	value := []byte("store-test-value")

	err = db.Put(ctx, key, value)
	require.NoError(t, err)

	// Verify through store interface
	has, err := store.Has(ctx, ds.NewKey(string(key)))
	require.NoError(t, err)
	assert.True(t, has)

	retrievedValue, err := store.Get(ctx, ds.NewKey(string(key)))
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)
}

func TestCRDTKeyValueDB_Repair(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-repair-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	// Test Repair method
	err = db.Repair(ctx)
	// Repair should not fail, but may not do anything in test environment
	assert.NoError(t, err)
}

func TestCRDTKeyValueDB_ConcurrentOperations(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "icefiredb-concurrent-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	// Test concurrent operations
	numGoroutines := 10
	numOperations := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < numOperations; j++ {
				key := []byte(string(rune(id)) + "-key-" + string(rune(j)))
				value := []byte(string(rune(id)) + "-value-" + string(rune(j)))

				// Put
				err := db.Put(ctx, key, value)
				assert.NoError(t, err)

				// Get
				retrieved, err := db.Get(ctx, key)
				assert.NoError(t, err)
				assert.Equal(t, value, retrieved)

				// Has
				has, err := db.Has(ctx, key)
				assert.NoError(t, err)
				assert.True(t, has)

				time.Sleep(time.Millisecond) // Small delay to increase concurrency
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
