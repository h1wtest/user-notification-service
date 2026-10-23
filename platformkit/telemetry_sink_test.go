package platformkit

import "context"

// Explicit in-memory recording fixture; disabled production export is not a sink.
func recordingTelemetry(service string) *telemetry {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	close(done)
	return &telemetry{service: service, endpoint: "test-recording-sink", queue: make(chan otlpSpan, 256), stop: make(chan struct{}), done: done, lifetime: ctx, cancel: cancel}
}
