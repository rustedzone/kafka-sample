package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/google/gops/agent"
)

func Test_main(t *testing.T) {
	tests := []struct {
		name                          string
		mockFnDeliveryConsumeKafkaCMD func()
		mockFnGopsAgentList           func(agent.Options) error
		wantPanic                     bool
	}{
		{
			name:                          "positive case-1:main successfully initiated",
			mockFnDeliveryConsumeKafkaCMD: func() { return },
			mockFnGopsAgentList:           func(agent.Options) error { return nil },
		},
		{
			name:                          "negative case-1:fail initating gops agent",
			mockFnDeliveryConsumeKafkaCMD: func() { return },
			mockFnGopsAgentList:           func(agent.Options) error { return errors.New("fail initiate gops agent") },
			wantPanic:                     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fnDeliveryConsumeKafkaCMD = tt.mockFnDeliveryConsumeKafkaCMD
			fnGopsAgentList = tt.mockFnGopsAgentList
			if tt.wantPanic {
				assert.Panics(t, func() { main() })
			} else {
				main()
			}
		})
	}
}
