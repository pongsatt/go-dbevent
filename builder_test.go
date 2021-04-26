package dbevent_test

import (
	"testing"

	"github.com/pongsatt/go-dbevent"
)

func TestBuilder(t *testing.T) {
	type testdata struct {
		ID string
	}

	mockTestData := &testdata{ID: "test1"}

	type args struct {
		eventType string
		testdata  *testdata
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "eventType only", args: args{
				eventType: "testtype",
			},
		},
		{
			name: "eventType and data", args: args{
				eventType: "testtype",
				testdata:  mockTestData,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			build := dbevent.NewBuilder(tt.args.eventType)

			if tt.args.testdata != nil {
				build.Data(tt.args.testdata)
			}

			got := build.Build()

			if got == nil {
				t.Errorf("Build() != nil")
			}

			if got.Type == "" {
				t.Errorf("Type must not empty but got %s", got.Type)
			}

			if got.CreatedAt == nil {
				t.Errorf("CreatedAt != nil but got nil")
			}

			if tt.args.testdata != nil && got.Data == nil {
				t.Errorf("Data must not nil but got nil")
			}

		})
	}
}
