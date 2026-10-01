package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/beamlink/circuit"
	orchestratordomain "github.com/Beam-Network/beam/internal/orchestrator/domain"
	"github.com/Beam-Network/beam/internal/orchestrator/registry"
	"github.com/Beam-Network/beam/internal/workload/domain"
	"github.com/Beam-Network/beam/internal/workload/runtime"
)

type admitControl struct {
	rejectWorker string
	downWorker   string
	offered      []string
}

func (c *admitControl) Offer(_ context.Context, workerID string, spec domain.Spec) (runtime.Decision, error) {
	c.offered = append(c.offered, workerID)
	if workerID == c.rejectWorker {
		return runtime.Decision{WorkloadID: spec.WorkloadID, AttemptID: spec.AttemptID, Reason: "insufficient resource capacity"}, nil
	}
	return runtime.Decision{WorkloadID: spec.WorkloadID, AttemptID: spec.AttemptID, Accepted: true}, nil
}

func (c *admitControl) Commit(context.Context, string, domain.Commit) error  { return nil }
func (c *admitControl) Cancel(context.Context, string, string, string) error { return nil }
func (c *admitControl) Connected(workerID string) bool                       { return workerID != c.downWorker }
func (c *admitControl) UpsertCircuit(context.Context, circuit.Plan) error    { return nil }
func (c *admitControl) RevokeCircuit(context.Context, circuit.Revocation) error {
	return nil
}

func serviceWithWorkers(t *testing.T, control Control, bandwidth int64) *Service {
	t.Helper()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg, err := registry.New(orchestratordomain.Orchestrator{OrchestratorID: "orch", CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	for _, workerID := range []string{"worker-a", "worker-b"} {
		if err := reg.Join(orchestratordomain.Membership{
			OrchestratorID: "orch", WorkerID: workerID, NodeID: "node-" + workerID, Status: "active",
		}, now); err != nil {
			t.Fatal(err)
		}
		resources := domain.Resources{
			CPUMillis: 1000, MemoryBytes: 512 << 20, ScratchBytes: 1 << 30,
			BandwidthMbps: bandwidth, Connections: 1024, Streams: 1024,
		}
		if err := reg.UpdateObservation(orchestratordomain.WorkerObservation{
			WorkerID: workerID, NodeID: "node-" + workerID, Status: "active",
			Capabilities: []string{"transfer.multipart"},
			Total:        resources, Available: resources, ObservedAt: now,
		}, now); err != nil {
			t.Fatal(err)
		}
	}
	service, err := NewService(Config{OrchestratorID: "orch", Now: func() time.Time { return now.Add(time.Second) }}, reg, control, NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func chunkSpec(id string) domain.Spec {
	return domain.Spec{
		WorkloadID: id, AttemptID: id, Kind: domain.KindTransferMultipart,
		RequiredCapabilities: []string{"transfer.multipart"},
		Resources: domain.Resources{
			MemoryBytes: 4 << 20, BandwidthMbps: 1, Connections: 2, Streams: 2,
		},
	}
}

func TestDispatchRetriesWhenChosenWorkerRefuses(t *testing.T) {
	control := &admitControl{rejectWorker: "worker-a"}
	service := serviceWithWorkers(t, control, 10)
	record, err := service.Dispatch(context.Background(), DispatchRequest{
		Source: SourceBeamCore, ExternalID: "offer-1", BatchID: "batch-1", Spec: chunkSpec("offer-1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.WorkerID != "worker-b" || record.State != StateCommitted {
		t.Fatalf("record worker=%s state=%s", record.WorkerID, record.State)
	}
	if len(control.offered) != 2 || control.offered[0] != "worker-a" || control.offered[1] != "worker-b" {
		t.Fatalf("offers=%v", control.offered)
	}
}

func TestDispatchSkipsDisconnectedWorker(t *testing.T) {
	control := &admitControl{downWorker: "worker-a"}
	service := serviceWithWorkers(t, control, 10)
	record, err := service.Dispatch(context.Background(), DispatchRequest{
		Source: SourceBeamCore, ExternalID: "offer-1", BatchID: "batch-1", Spec: chunkSpec("offer-1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.WorkerID != "worker-b" {
		t.Fatalf("record worker=%s", record.WorkerID)
	}
	if len(control.offered) != 1 || control.offered[0] != "worker-b" {
		t.Fatalf("offers=%v", control.offered)
	}
}
