package p2p

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPubSubHandleType(t *testing.T) {
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

func TestGenerateCID_Consistency(t *testing.T) {
	// Test that the same input always produces the same CID
	input1 := "test-service"
	input2 := "test-service"

	cid1 := generateCID(input1)
	cid2 := generateCID(input2)

	assert.Equal(t, cid1.String(), cid2.String())

	// Test that different inputs produce different CIDs
	input3 := "different-service"
	cid3 := generateCID(input3)

	assert.NotEqual(t, cid1.String(), cid3.String())
}
