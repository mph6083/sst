package emulatorbridge

import (
	"strings"
	"testing"
)

func TestResolveNamesUsesTheAppAndStage(t *testing.T) {
	names := ResolveNames("my-app", "dev")
	if names.Table != "sst-live-my-app-dev" {
		t.Fatalf("unexpected table name %q", names.Table)
	}
	if names.Queue != "sst-live-my-app-dev.fifo" {
		t.Fatalf("unexpected queue name %q", names.Queue)
	}
}

func TestResolveNamesReplacesCharactersSqsRejects(t *testing.T) {
	names := ResolveNames("my.app", "feature/x")
	if names.Table != "sst-live-my-app-feature-x" {
		t.Fatalf("unexpected table name %q", names.Table)
	}
}

func TestResolveNamesKeepsLongNamesInsideTheSqsLimitAndDistinct(t *testing.T) {
	const sqsQueueNameLimit = 80
	first := ResolveNames(strings.Repeat("a", 60), strings.Repeat("b", 60))
	second := ResolveNames(strings.Repeat("a", 60), strings.Repeat("b", 59)+"c")
	if len(first.Queue) > sqsQueueNameLimit {
		t.Fatalf("queue name is %d characters, over the limit of %d", len(first.Queue), sqsQueueNameLimit)
	}
	if first.Queue == second.Queue {
		t.Fatalf("two different long stages resolved to the same queue %q", first.Queue)
	}
}

func TestWorkerFromChannelReturnsTheWorkerSegment(t *testing.T) {
	worker := workerFromChannel("/sst/my-app/dev/my-app-dev-HelloFunction-abcd/in")
	if worker != "my-app-dev-HelloFunction-abcd" {
		t.Fatalf("unexpected worker %q", worker)
	}
}
