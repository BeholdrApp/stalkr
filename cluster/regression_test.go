package cluster

import (
	"context"
	"testing"
	"time"
)

func TestZeroReadyReplicasIsAuthoritative(t *testing.T) {
	src := oneNodeOnePodFixture()
	src.deployments[0].Status.ReadyReplicas = 0
	c := New(src, time.Second, time.Second, testLogger())
	c.Poll(context.Background())
	m := c.Snapshot().Microservices[0]
	if m.RunningPods != 1 || m.ReadyReplicas != 0 {
		t.Fatalf("running phase must not override controller readiness: %+v", m)
	}
}
