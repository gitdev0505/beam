package connectors

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/beamlink/circuit"
	"github.com/Beam-Network/beam/internal/orchestrator/dispatch"
	orchestratordomain "github.com/Beam-Network/beam/internal/orchestrator/domain"
	"github.com/Beam-Network/beam/internal/orchestrator/registry"
	"github.com/Beam-Network/beam/internal/workload/domain"
	"github.com/Beam-Network/beam/internal/workload/runtime"
	"github.com/vmihailenco/msgpack/v5"
)

type batchControl struct {
	mu            sync.Mutex
	rejectAttempt string
	offered       []string
	entered       chan struct{}
	release       chan struct{}
}

func (c *batchControl) Offer(_ context.Context, workerID string, spec domain.Spec) (runtime.Decision, error) {
	if c.entered != nil {
		c.entered <- struct{}{}
	}
	if c.release != nil {
		<-c.release
	}
	c.mu.Lock()
	c.offered = append(c.offered, workerID+"/"+spec.AttemptID)
	c.mu.Unlock()
	if spec.AttemptID == c.rejectAttempt {
		return runtime.Decision{Reason: "insufficient resource capacity"}, nil
	}
	return runtime.Decision{Accepted: true}, nil
}

func (c *batchControl) Commit(context.Context, string, domain.Commit) error  { return nil }
func (c *batchControl) Cancel(context.Context, string, string, string) error { return nil }
func (c *batchControl) Connected(string) bool                                { return true }
func (c *batchControl) UpsertCircuit(context.Context, circuit.Plan) error    { return nil }
func (c *batchControl) RevokeCircuit(context.Context, circuit.Revocation) error {
	return nil
}

func batchTasks(t *testing.T, control dispatch.Control, workers []string) *dispatch.Service {
	t.Helper()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg, err := registry.New(orchestratordomain.Orchestrator{OrchestratorID: "orch", CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	for _, workerID := range workers {
		if err := reg.Join(orchestratordomain.Membership{
			OrchestratorID: "orch", WorkerID: workerID, NodeID: "node-" + workerID, Status: "active",
		}, now); err != nil {
			t.Fatal(err)
		}
		resources := domain.Resources{
			CPUMillis: 1000, MemoryBytes: 512 << 20, ScratchBytes: 1 << 30,
			BandwidthMbps: 10, Connections: 1024, Streams: 1024,
		}
		if err := reg.UpdateObservation(orchestratordomain.WorkerObservation{
			WorkerID: workerID, NodeID: "node-" + workerID, Status: "active",
			Capabilities: []string{"transfer.multipart"},
			Total:        resources, Available: resources, ObservedAt: now,
		}, now); err != nil {
			t.Fatal(err)
		}
	}
	tasks, err := dispatch.NewService(
		dispatch.Config{OrchestratorID: "orch", Now: func() time.Time { return now.Add(time.Second) }},
		reg, control, dispatch.NewMemoryStore(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return tasks
}

func encodeOffers(t *testing.T, ids ...string) []byte {
	t.Helper()
	offers := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		offers = append(offers, map[string]any{
			"offer_id": id,
			"source":   map[string]any{"url": "https://example.test/" + id, "chunk_size": int64(1024)},
			"destinations": []any{
				map[string]any{"url": "https://example.test/out/" + id},
			},
		})
	}
	encoded, err := msgpack.Marshal(map[string]any{
		"type":                  "task_offer_batch",
		"batch_id":              "batch-1",
		"assignment_timeout_ms": int64(15000),
		"offers":                offers,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestTaskOfferBatchContinuesAfterRejectedOffer(t *testing.T) {
	control := &batchControl{rejectAttempt: "bad"}
	tasks := batchTasks(t, control, []string{"worker-a"})
	err := (&roomControl{tasks: tasks}).handleTaskOfferBatch(context.Background(), encodeOffers(t, "bad", "good"))
	if err == nil {
		t.Fatal("expected the rejected offer to be reported")
	}
	offered := map[string]bool{}
	for _, item := range control.offered {
		offered[item] = true
	}
	if len(control.offered) != 2 || !offered["worker-a/bad"] || !offered["worker-a/good"] {
		t.Fatalf("offers=%v err=%v", control.offered, err)
	}
	good, ok := tasks.Record("good/good")
	if !ok || good.State != dispatch.StateCommitted {
		t.Fatalf("second offer record=%+v ok=%v", good, ok)
	}
}

func TestTaskOfferBatchSpreadsAcrossWorkers(t *testing.T) {
	control := &batchControl{}
	tasks := batchTasks(t, control, []string{"worker-a", "worker-b"})
	if err := (&roomControl{tasks: tasks}).handleTaskOfferBatch(context.Background(), encodeOffers(t, "one", "two")); err != nil {
		t.Fatal(err)
	}
	first, _ := tasks.Record("one/one")
	second, _ := tasks.Record("two/two")
	if first.WorkerID == "" || second.WorkerID == "" || first.WorkerID == second.WorkerID {
		t.Fatalf("workers %s and %s", first.WorkerID, second.WorkerID)
	}
}

func TestTaskOfferBatchDispatchesOffersTogether(t *testing.T) {
	control := &batchControl{entered: make(chan struct{}), release: make(chan struct{})}
	tasks := batchTasks(t, control, []string{"worker-a", "worker-b"})
	done := make(chan error, 1)
	go func() {
		done <- (&roomControl{tasks: tasks}).handleTaskOfferBatch(context.Background(), encodeOffers(t, "one", "two"))
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-control.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("offers were dispatched one at a time")
		}
	}
	close(control.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
