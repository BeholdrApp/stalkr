// Package demo supplies isolated, synthetic inputs to the normal collector and
// service-health query paths. It has no Kubernetes or HTTP client.
package demo

import (
	"context"
	"fmt"
	"math"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/beholdrapp/stalkr/cluster"
	"github.com/beholdrapp/stalkr/k8s"
)

type Source struct{}

var _ cluster.Source = Source{}

var services = []struct {
	name, namespace string
	replicas        int32
}{
	{"catalog", "shop", 3}, {"checkout", "shop", 3}, {"payments", "shop", 2}, {"gateway", "platform", 2},
}

func (Source) Nodes(context.Context) ([]corev1.Node, error) {
	var nodes []corev1.Node
	for i := 1; i <= 3; i++ {
		nodes = append(nodes, corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("demo-worker-%d", i)},
			Status: corev1.NodeStatus{
				Capacity: resources("4", "8Gi"), Allocatable: resources("3800m", "7Gi"),
				Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
				NodeInfo:   corev1.NodeSystemInfo{KubeletVersion: "demo"},
			},
		})
	}
	return nodes, nil
}

func (Source) Deployments(context.Context) ([]appsv1.Deployment, error) {
	var out []appsv1.Deployment
	for _, s := range services {
		ready := s.replicas
		if s.name == "checkout" {
			ready--
		}
		out = append(out, appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: s.name, Namespace: s.namespace},
			Spec:       appsv1.DeploymentSpec{Replicas: &s.replicas},
			Status:     appsv1.DeploymentStatus{ReadyReplicas: ready, AvailableReplicas: ready},
		})
	}
	return out, nil
}

func (Source) StatefulSets(context.Context) ([]appsv1.StatefulSet, error) {
	n := int32(1)
	return []appsv1.StatefulSet{{ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "shop"}, Spec: appsv1.StatefulSetSpec{Replicas: &n}, Status: appsv1.StatefulSetStatus{ReadyReplicas: 1}}}, nil
}

func (Source) DaemonSets(context.Context) ([]appsv1.DaemonSet, error) {
	return []appsv1.DaemonSet{{ObjectMeta: metav1.ObjectMeta{Name: "node-agent", Namespace: "platform"}, Status: appsv1.DaemonSetStatus{DesiredNumberScheduled: 3, NumberReady: 3}}}, nil
}

func (Source) HPAs(context.Context) ([]autoscalingv1.HorizontalPodAutoscaler, error) {
	min, target, current := int32(2), int32(70), int32(82)
	return []autoscalingv1.HorizontalPodAutoscaler{{
		ObjectMeta: metav1.ObjectMeta{Name: "checkout", Namespace: "shop"},
		Spec:       autoscalingv1.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv1.CrossVersionObjectReference{Kind: "Deployment", Name: "checkout"}, MinReplicas: &min, MaxReplicas: 6, TargetCPUUtilizationPercentage: &target},
		Status:     autoscalingv1.HorizontalPodAutoscalerStatus{CurrentReplicas: 3, DesiredReplicas: 4, CurrentCPUUtilizationPercentage: &current},
	}}, nil
}

func (Source) Pods(context.Context) ([]corev1.Pod, error) {
	var pods []corev1.Pod
	for _, s := range services {
		for i := 0; i < int(s.replicas); i++ {
			p := pod(s.namespace, fmt.Sprintf("%s-7d9f8c6b54-demo%d", s.name, i), "ReplicaSet", s.name+"-7d9f8c6b54", i)
			if s.name == "checkout" && i == 2 {
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", RestartCount: 12, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}
			}
			pods = append(pods, p)
		}
	}
	pods = append(pods, pod("shop", "cache-0", "StatefulSet", "cache", 1))
	for i := 0; i < 3; i++ {
		pods = append(pods, pod("platform", fmt.Sprintf("node-agent-demo%d", i), "DaemonSet", "node-agent", i))
	}
	return pods, nil
}

func pod(ns, name, kind, owner string, node int) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, OwnerReferences: []metav1.OwnerReference{{Kind: kind, Name: owner}}},
		Spec:       corev1.PodSpec{NodeName: fmt.Sprintf("demo-worker-%d", node%3+1), Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: resources("250m", "128Mi"), Limits: resources("500m", "512Mi")}}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func resources(cpu, memory string) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory)}
}

func (Source) NodeMetrics(context.Context) (map[string]k8s.Usage, error) {
	out := map[string]k8s.Usage{}
	for i := 1; i <= 3; i++ {
		out[fmt.Sprintf("demo-worker-%d", i)] = k8s.Usage{CPUMilli: int64(800 + float64(i)*180 + wave(time.Now())*120), MemBytes: int64(2+i) * 1024 * 1024 * 1024}
	}
	return out, nil
}

func (s Source) PodMetrics(ctx context.Context) (map[string]k8s.Usage, error) {
	pods, _ := s.Pods(ctx)
	out := map[string]k8s.Usage{}
	for i, p := range pods {
		out[p.Namespace+"/"+p.Name] = k8s.Usage{CPUMilli: int64(60+i*12) + int64(wave(time.Now())*20), MemBytes: int64(60+i*8) * 1024 * 1024}
	}
	return out, nil
}

func wave(t time.Time) float64 { return math.Sin(float64(t.Unix()) / 60) }
