package scheduling

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	orchestratordomain "github.com/Beam-Network/beam/internal/orchestrator/domain"
	"github.com/Beam-Network/beam/internal/orchestrator/registry"
	workload "github.com/Beam-Network/beam/internal/workload/domain"
)

var ErrNoCandidate = errors.New("no compatible Worker candidate")

type Request struct {
	RequiredCapabilities []string
	Resources            workload.Resources
	MaxObservationAge    time.Duration
	ExcludedWorkerIDs    []string
}

type Rejection struct {
	WorkerID string
	Reason   string
}

type Placement struct {
	WorkerID string
	NodeID   string
	Score    int64
	Rejected []Rejection
}

type reservation struct {
	resources workload.Resources
	at        time.Time
}

type Scheduler struct {
	registry *registry.Registry

	mu           sync.Mutex
	reservations map[string][]reservation
}

func New(registry *registry.Registry) *Scheduler { return &Scheduler{registry: registry} }

// NoteReservation records resources just handed to a worker. Select subtracts
// reservations newer than that worker's last heartbeat, because the heartbeat
// still reports the old free capacity for up to one interval. Safe on a nil
// or zero-value scheduler.
func (s *Scheduler) NoteReservation(workerID string, resources workload.Resources, at time.Time) {
	if s == nil || workerID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservations == nil {
		s.reservations = map[string][]reservation{}
	}
	s.pruneReservationsLocked(at)
	s.reservations[workerID] = append(s.reservations[workerID], reservation{resources: resources, at: at})
}

func (s *Scheduler) pruneReservationsLocked(now time.Time) {
	cutoff := now.Add(-30 * time.Second)
	for workerID, pending := range s.reservations {
		kept := pending[:0]
		for _, item := range pending {
			if item.at.After(cutoff) {
				kept = append(kept, item)
			}
		}
		if len(kept) == 0 {
			delete(s.reservations, workerID)
			continue
		}
		s.reservations[workerID] = kept
	}
}

func (s *Scheduler) reservationsFor(workerID string, now time.Time) []reservation {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneReservationsLocked(now)
	pending := s.reservations[workerID]
	if len(pending) == 0 {
		return nil
	}
	return append([]reservation(nil), pending...)
}

func applyReservations(observation orchestratordomain.WorkerObservation, pending []reservation) orchestratordomain.WorkerObservation {
	if len(pending) == 0 {
		return observation
	}
	// A reservation within 1s before the heartbeat still counts. The heartbeat
	// can be timestamped just before the offer it has not reported yet.
	cutoff := observation.ObservedAt.Add(-time.Second)
	var used workload.Resources
	for _, item := range pending {
		if !item.at.Before(cutoff) {
			used = used.Add(item.resources)
		}
	}
	observation.Available = observation.Available.SubFloor(used)
	return observation
}

func (s *Scheduler) Select(request Request, now time.Time) (Placement, error) {
	if request.MaxObservationAge <= 0 {
		request.MaxObservationAge = 30 * time.Second
	}
	type candidate struct {
		observation orchestratordomain.WorkerObservation
		score       int64
	}
	var candidates []candidate
	placement := Placement{}
	for _, observation := range s.registry.Observations() {
		observation = applyReservations(observation, s.reservationsFor(observation.WorkerID, now))
		reason := rejectionReason(observation, request, now)
		if reason != "" {
			placement.Rejected = append(placement.Rejected, Rejection{WorkerID: observation.WorkerID, Reason: reason})
			continue
		}
		score := observation.Available.BandwidthMbps*1_000_000 + observation.Available.MemoryBytes/(1<<20)
		candidates = append(candidates, candidate{observation: observation, score: score})
	}
	if len(candidates) == 0 {
		return placement, ErrNoCandidate
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].observation.WorkerID < candidates[j].observation.WorkerID
		}
		return candidates[i].score > candidates[j].score
	})
	selected := candidates[0]
	placement.WorkerID = selected.observation.WorkerID
	placement.NodeID = selected.observation.NodeID
	placement.Score = selected.score
	return placement, nil
}

func rejectionReason(observation orchestratordomain.WorkerObservation, request Request, now time.Time) string {
	if slices.Contains(request.ExcludedWorkerIDs, observation.WorkerID) {
		return "excluded by placement request"
	}
	if observation.Status != "active" {
		return "not active"
	}
	if observation.ObservedAt.IsZero() || now.Sub(observation.ObservedAt) > request.MaxObservationAge {
		return "stale observation"
	}
	for _, capability := range request.RequiredCapabilities {
		if !slices.Contains(observation.Capabilities, capability) {
			return fmt.Sprintf("missing capability %s", capability)
		}
	}
	if !observation.Available.Fits(request.Resources) {
		return "insufficient resources"
	}
	return ""
}
