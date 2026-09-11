package cluster

import (
	corev1 "k8s.io/api/core/v1"
	"testing"
)

func TestPodStatusReason(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status corev1.PodStatus
		want   string
	}{
		{"running", corev1.PodStatus{Phase: corev1.PodRunning}, ""},
		{"crash loop", corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}}, "CrashLoopBackOff"},
		{"init", corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}}, "ImagePullBackOff"},
		{"oom", corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137}}}}}, "OOMKilled"},
		{"completed", corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed", ExitCode: 0}}}}}, ""},
		{"evicted", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"}, "Evicted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := podStatusReason(&corev1.Pod{Status: tc.status}); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
