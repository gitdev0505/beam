package scheduling

import (
	"testing"
	"time"

	orchestratordomain "github.com/Beam-Network/beam/internal/orchestrator/domain"
	"github.com/Beam-Network/beam/internal/orchestrator/registry"
	workload "github.com/Beam-Network/beam/internal/workload/domain"
)

func testRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg, err := registry.New(orchestratordomain.Orchestrator{OrchestratorID: "orch", CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func observe(t *testing.T, reg *registry.Registry, workerID string, bandwidth int64, at time.Time) {
	t.Helper()
	if err := reg.Join(orchestratordomain.Membership{
		OrchestratorID: "orch", WorkerID: workerID, NodeID: "node-" + workerID, Status: "active",
	}, at); err != nil {
		t.Fatal(err)
	}
	resources := workload.Resources{
		CPUMillis: 1000, MemoryBytes: 512 << 20, ScratchBytes: 1 << 30,
		BandwidthMbps: bandwidth, Connections: 1024, Streams: 1024,
	}
	if err := reg.UpdateObservation(orchestratordomain.WorkerObservation{
		WorkerID: workerID, NodeID: "node-" + workerID, Status: "active",
		Capabilities: []string{"transfer.multipart"},
		Total:        resources, Available: resources, ObservedAt: at,
	}, at); err != nil {
		t.Fatal(err)
	}
}

func chunkRequest() Request {
	return Request{RequiredCapabilities: []string{"transfer.multipart"}, Resources: workload.Resources{
		MemoryBytes: 4 << 20, BandwidthMbps: 1, Connections: 2, Streams: 2,
	}}
}

func TestEqualWorkersAlternateAfterReservation(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg := testRegistry(t)
	observe(t, reg, "worker-a", 10, at)
	observe(t, reg, "worker-b", 10, at)
	scheduler := New(reg)
	now := at.Add(time.Second)
	resources := chunkRequest().Resources

	first, err := scheduler.Select(chunkRequest(), now)
	if err != nil || first.WorkerID != "worker-a" {
		t.Fatalf("first placement=%+v err=%v", first, err)
	}
	scheduler.NoteReservation("worker-a", resources, now)

	second, err := scheduler.Select(chunkRequest(), now)
	if err != nil || second.WorkerID != "worker-b" {
		t.Fatalf("second placement=%+v err=%v", second, err)
	}
	scheduler.NoteReservation("worker-b", resources, now)

	third, err := scheduler.Select(chunkRequest(), now)
	if err != nil || third.WorkerID != "worker-a" {
		t.Fatalf("third placement=%+v err=%v", third, err)
	}
}

func TestNewerHeartbeatClearsReservation(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg := testRegistry(t)
	observe(t, reg, "worker-a", 1, at)
	scheduler := New(reg)
	now := at.Add(time.Second)
	if _, err := scheduler.Select(chunkRequest(), now); err != nil {
		t.Fatal(err)
	}
	scheduler.NoteReservation("worker-a", chunkRequest().Resources, now)
	if _, err := scheduler.Select(chunkRequest(), now); err == nil {
		t.Fatal("reservation was still free")
	}
	observe(t, reg, "worker-a", 1, now.Add(5*time.Second))
	if _, err := scheduler.Select(chunkRequest(), now.Add(5*time.Second)); err != nil {
		t.Fatalf("newer heartbeat did not clear the reservation: %v", err)
	}
}

func TestOneSlotWorkerIsNotPickedTwice(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg := testRegistry(t)
	observe(t, reg, "worker-a", 1, at)
	scheduler := New(reg)
	now := at.Add(time.Second)
	placement, err := scheduler.Select(chunkRequest(), now)
	if err != nil || placement.WorkerID != "worker-a" {
		t.Fatalf("placement=%+v err=%v", placement, err)
	}
	scheduler.NoteReservation("worker-a", chunkRequest().Resources, now)
	if _, err := scheduler.Select(chunkRequest(), now); err != ErrNoCandidate {
		t.Fatalf("second placement err=%v", err)
	}
}

func TestReservationOlderThan30SecondsIsDropped(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg := testRegistry(t)
	observe(t, reg, "worker-a", 1, at)
	scheduler := New(reg)
	scheduler.NoteReservation("worker-a", chunkRequest().Resources, at.Add(time.Second))
	later := at.Add(40 * time.Second)
	request := chunkRequest()
	request.MaxObservationAge = time.Hour
	if _, err := scheduler.Select(request, later); err != nil {
		t.Fatalf("expired reservation still blocked placement: %v", err)
	}
}

func TestMeasuredFastWorkerKeepsTheBatch(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg := testRegistry(t)
	observe(t, reg, "worker-fast", 10, at)
	observe(t, reg, "worker-slow", 10, at)
	scheduler := New(reg)
	now := at.Add(time.Second)
	scheduler.NoteSample("worker-fast", chunkBytes, 2*time.Second, now)
	scheduler.NoteSample("worker-slow", chunkBytes, 20*time.Second, now)
	resources := chunkRequest().Resources

	for step := 0; step < 3; step++ {
		placement, err := scheduler.Select(chunkRequest(), now)
		if err != nil || placement.WorkerID != "worker-fast" {
			t.Fatalf("step %d placement=%+v err=%v", step, placement, err)
		}
		scheduler.NoteReservation("worker-fast", resources, now)
	}
}

func TestUnmeasuredWorkerGetsOneOfferAfterFastWorkerIsFull(t *testing.T) {
	at := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	reg := testRegistry(t)
	observe(t, reg, "worker-fast", 1, at)
	observe(t, reg, "worker-new", 10, at)
	scheduler := New(reg)
	now := at.Add(time.Second)
	scheduler.NoteSample("worker-fast", chunkBytes, 2*time.Second, now)
	resources := chunkRequest().Resources

	first, err := scheduler.Select(chunkRequest(), now)
	if err != nil || first.WorkerID != "worker-fast" {
		t.Fatalf("first placement=%+v err=%v", first, err)
	}
	scheduler.NoteReservation("worker-fast", resources, now)

	second, err := scheduler.Select(chunkRequest(), now)
	if err != nil || second.WorkerID != "worker-new" {
		t.Fatalf("second placement=%+v err=%v", second, err)
	}
	scheduler.NoteReservation("worker-new", resources, now)

	if _, err := scheduler.Select(chunkRequest(), now); err != ErrNoCandidate {
		t.Fatalf("third placement err=%v", err)
	}
}

func TestNoteReservationOnNilScheduler(t *testing.T) {
	var scheduler *Scheduler
	scheduler.NoteReservation("worker-a", chunkRequest().Resources, time.Now())
	var zero Scheduler
	zero.NoteReservation("worker-a", chunkRequest().Resources, time.Now())
}
