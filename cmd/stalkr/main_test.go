package main

import "testing"

func TestDemoRejectsNonLoopbackAndMalformedEndpoints(t *testing.T) {
	for _, endpoint := range []string{"https://collector.example/v1/metrics", "http://localhost:4318/v1/metrics", "ftp://127.0.0.1/x", "http://user:secret@127.0.0.1/x", "http://127.0.0.1/x?token=secret", "garbage"} {
		if validateEndpoint(endpoint, true) == nil {
			t.Errorf("accepted %s", endpoint)
		}
	}
	for _, endpoint := range []string{"http://127.0.0.1:4318/v1/metrics", "http://[::1]:4318/v1/metrics"} {
		if err := validateEndpoint(endpoint, true); err != nil {
			t.Error(err)
		}
	}
}
