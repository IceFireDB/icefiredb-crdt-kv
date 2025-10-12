package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintVal(t *testing.T) {
	// This is a simple test for the printVal function
	// Since it just prints to stdout, we mainly test that it doesn't panic

	tests := []struct {
		name  string
		input interface{}
	}{
		{
			name:  "string value",
			input: "test string",
		},
		{
			name:  "integer value",
			input: 123,
		},
		{
			name:  "nil value",
			input: nil,
		},
		{
			name:  "error value",
			input: assert.AnError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This should not panic
			printVal(tt.input)
		})
	}
}

func TestMainFunctionExists(t *testing.T) {
	// This test simply verifies that the main function exists
	// and the package compiles correctly
	assert.NotNil(t, main)
}
