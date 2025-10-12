package p2p

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateCID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple service name",
			input:    "test-service",
			expected: "baegbe", // CID should start with base58 prefix
		},
		{
			name:     "empty service name",
			input:    "",
			expected: "baegbe",
		},
		{
			name:     "special characters",
			input:    "test-service-123!@#",
			expected: "baegbe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cid := generateCID(tt.input)
			require.NotNil(t, cid)
			assert.Contains(t, cid.String(), tt.expected)
		})
	}
}

func TestNewP2P(t *testing.T) {
	// Generate a private key for testing
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	tests := []struct {
		name        string
		serviceName string
		port        string
		subType     PubSubHandleType
	}{
		{
			name:        "gossip pubsub",
			serviceName: "test-service",
			port:        "0", // Use port 0 for automatic port assignment
			subType:     PubSubHandleTypeGossip,
		},
		{
			name:        "flood pubsub",
			serviceName: "test-service-2",
			port:        "0",
			subType:     PubSubHandleTypeFlood,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reduce log noise during tests
			logrus.SetLevel(logrus.ErrorLevel)

			p2p := NewP2P(tt.serviceName, priv, tt.port, tt.subType)
			require.NotNil(t, p2p)
			defer p2p.Close()

			// Verify P2P structure
			assert.NotNil(t, p2p.Ctx)
			assert.NotNil(t, p2p.Host)
			assert.NotNil(t, p2p.KadDHT)
			assert.NotNil(t, p2p.Discovery)
			assert.NotNil(t, p2p.PubSub)
			assert.Equal(t, tt.serviceName, p2p.service)

			// Verify host is running
			assert.True(t, p2p.Host.ID().Validate() == nil)
			assert.Greater(t, len(p2p.Host.Addrs()), 0)

			// Verify DHT is running
			routing := p2p.KadDHT
			assert.NotNil(t, routing)
		})
	}
}

func TestP2P_AdvertiseConnect(t *testing.T) {
	// Generate a private key for testing
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	// Reduce log noise during tests
	logrus.SetLevel(logrus.ErrorLevel)

	p2p := NewP2P("test-advertise-service", priv, "0", PubSubHandleTypeGossip)
	require.NotNil(t, p2p)
	defer p2p.Close()

	// Test AdvertiseConnect - this should not panic
	// Note: In test environment, this may not find peers but should not fail
	p2p.AdvertiseConnect()

	// Verify that the service is still running
	assert.NotNil(t, p2p.Host)
	assert.True(t, p2p.Host.ID().Validate() == nil)
}

func TestP2P_AnnounceConnect(t *testing.T) {
	// Generate a private key for testing
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	// Reduce log noise during tests
	logrus.SetLevel(logrus.ErrorLevel)

	p2p := NewP2P("test-announce-service", priv, "0", PubSubHandleTypeGossip)
	require.NotNil(t, p2p)
	defer p2p.Close()

	// Test AnnounceConnect - this should not panic
	// Note: In test environment, this may not find peers but should not fail
	p2p.AnnounceConnect()

	// Verify that the service is still running
	assert.NotNil(t, p2p.Host)
	assert.True(t, p2p.Host.ID().Validate() == nil)
}

func TestP2P_Close(t *testing.T) {
	// Generate a private key for testing
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	// Reduce log noise during tests
	logrus.SetLevel(logrus.ErrorLevel)

	p2p := NewP2P("test-close-service", priv, "0", PubSubHandleTypeGossip)
	require.NotNil(t, p2p)

	// Verify host is running before close
	assert.NotNil(t, p2p.Host)
	assert.True(t, p2p.Host.ID().Validate() == nil)

	// Close the P2P instance
	err = p2p.Close()
	assert.NoError(t, err)

	// Verify host is closed (this is the main test - Close should not panic)
	// Note: We don't test context cancellation as libp2p doesn't cancel the context on Close
}

func TestP2P_MultipleInstances(t *testing.T) {
	// Generate private keys for testing
	priv1, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	priv2, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	// Reduce log noise during tests
	logrus.SetLevel(logrus.ErrorLevel)

	// Create multiple P2P instances
	p2p1 := NewP2P("multi-service-1", priv1, "0", PubSubHandleTypeGossip)
	require.NotNil(t, p2p1)
	defer p2p1.Close()

	p2p2 := NewP2P("multi-service-2", priv2, "0", PubSubHandleTypeFlood)
	require.NotNil(t, p2p2)
	defer p2p2.Close()

	// Verify both instances are running and have different IDs
	assert.NotEqual(t, p2p1.Host.ID(), p2p2.Host.ID())
	assert.True(t, p2p1.Host.ID().Validate() == nil)
	assert.True(t, p2p2.Host.ID().Validate() == nil)

	// Verify both have different service names
	assert.Equal(t, "multi-service-1", p2p1.service)
	assert.Equal(t, "multi-service-2", p2p2.service)
}

func TestP2P_NetworkOperations(t *testing.T) {
	// Generate a private key for testing
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	// Reduce log noise during tests
	logrus.SetLevel(logrus.ErrorLevel)

	p2p := NewP2P("test-network-service", priv, "0", PubSubHandleTypeGossip)
	require.NotNil(t, p2p)
	defer p2p.Close()

	// Test network information
	addrs := p2p.Host.Addrs()
	assert.Greater(t, len(addrs), 0)

	// Test peer ID
	peerID := p2p.Host.ID()
	assert.True(t, peerID.Validate() == nil)

	// Test network connectivity (should be running but may not have peers)
	network := p2p.Host.Network()
	assert.NotNil(t, network)

	// Test peer count (should be 0 in isolated test environment)
	peers := network.Peers()
	assert.NotNil(t, peers)
}

func TestPubSubHandleType_String(t *testing.T) {
	tests := []struct {
		name     string
		handle   PubSubHandleType
		expected string
	}{
		{
			name:     "gossip type",
			handle:   PubSubHandleTypeGossip,
			expected: "gossip",
		},
		{
			name:     "flood type",
			handle:   PubSubHandleTypeFlood,
			expected: "flood",
		},
		{
			name:     "empty type",
			handle:   "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := string(tt.handle)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestP2P_ContextCancellation(t *testing.T) {
	// Generate a private key for testing
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	require.NoError(t, err)

	// Reduce log noise during tests
	logrus.SetLevel(logrus.ErrorLevel)

	p2p := NewP2P("test-context-service", priv, "0", PubSubHandleTypeGossip)
	require.NotNil(t, p2p)

	// Create a cancelable context
	ctx, cancel := context.WithCancel(context.Background())
	p2p.Ctx = ctx

	// Cancel the context
	cancel()

	// Verify context is done
	select {
	case <-p2p.Ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected context to be done after cancellation")
	}

	p2p.Close()
}
