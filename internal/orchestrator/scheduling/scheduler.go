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

const (
	// A standard transfer chunk is 50 MiB. The assignment wave times out at 15s,
	// and a 5s gap between original results reassigns the unfinished chunks.
	chunkBytes       = 50 << 20
	assignmentWave   = 15 * time.Second
	resultGap        = 5 * time.Second
	maxPaceSamples   = 8
	paceSampleWindow = time.Hour
)

type reservation struct {
	resources workload.Resources
	at        time.Time
}

type sample struct {
	mbps float64
	at   time.Time
}

type Scheduler struct {
	registry *registry.Registry

	mu           sync.Mutex
	reservations map[string][]reservation
	samples      map[string][]sample
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
	s.noteReservationLocked(workerID, resources, at)
}

// Reserve selects a worker and records the reservation before returning, so
// concurrent batch offers cannot all choose the same free slot.
func (s *Scheduler) Reserve(request Request, now time.Time) (Placement, error) {
	if s == nil {
		return Placement{}, ErrNoCandidate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	placement, err := s.selectLocked(request, now)
	if err != nil {
		return placement, err
	}
	s.noteReservationLocked(placement.WorkerID, request.Resources, now)
	return placement, nil
}

// ReleaseReservation returns one reserved slot when the worker never accepted
// the offer.
func (s *Scheduler) ReleaseReservation(workerID string, resources workload.Resources) {
	if s == nil || workerID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending := s.reservations[workerID]
	for i, item := range pending {
		if item.resources.BandwidthMbps != resources.BandwidthMbps {
			continue
		}
		s.reservations[workerID] = append(pending[:i], pending[i+1:]...)
		if len(s.reservations[workerID]) == 0 {
			delete(s.reservations, workerID)
		}
		return
	}
}

// NoteSample records one finished transfer. Later placement prefers workers
// whose recent chunks finish together, and gives an unmeasured worker one
// offer instead of an equal share of the batch.
func (s *Scheduler) NoteSample(workerID string, bytes int64, duration time.Duration, at time.Time) {
	if s == nil || workerID == "" || bytes < 1<<20 || duration <= 0 || duration > 30*time.Minute {
		return
	}
	mbps := float64(bytes*8) / duration.Seconds() / 1_000_000
	if mbps <= 0 || at.IsZero() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.samples == nil {
		s.samples = map[string][]sample{}
	}
	s.samples[workerID] = append(s.samples[workerID], sample{mbps: mbps, at: at})
	s.pruneSamplesLocked(at)
}

func (s *Scheduler) noteReservationLocked(workerID string, resources workload.Resources, at time.Time) {
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
	if s == nil {
		return Placement{}, ErrNoCandidate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.selectLocked(request, now)
}

type candidate struct {
	observation orchestratordomain.WorkerObservation
	score       int64
	measured    bool
	single      time.Duration
	limit       int64
	reserved    int64
}

func (s *Scheduler) selectLocked(request Request, now time.Time) (Placement, error) {
	if request.MaxObservationAge <= 0 {
		request.MaxObservationAge = 30 * time.Second
	}
	s.pruneReservationsLocked(now)
	s.pruneSamplesLocked(now)
	bestSingle, anyMeasured := s.paceAnchorLocked(request, now)
	var candidates []candidate
	placement := Placement{}
	for _, observation := range s.registry.Observations() {
		pending := s.reservations[observation.WorkerID]
		reserved := reservedBandwidth(pending, observation.ObservedAt)
		observation = applyReservations(observation, pending)
		reason := rejectionReason(observation, request, now)
		if reason != "" {
			placement.Rejected = append(placement.Rejected, Rejection{WorkerID: observation.WorkerID, Reason: reason})
			continue
		}
		mbps := s.medianMbpsLocked(observation.WorkerID, now)
		item := candidate{
			observation: observation,
			score:       observation.Available.BandwidthMbps*1_000_000 + observation.Available.MemoryBytes/(1<<20),
			measured:    mbps > 0,
			reserved:    reserved,
		}
		if item.measured {
			item.single = singleChunkDuration(mbps)
			item.limit = paceLimit(item.single)
		} else {
			item.limit = 1
		}
		candidates = append(candidates, item)
	}
	selected, ok := chooseCandidate(candidates, bestSingle, anyMeasured)
	if !ok {
		return placement, ErrNoCandidate
	}
	placement.WorkerID = selected.observation.WorkerID
	placement.NodeID = selected.observation.NodeID
	placement.Score = selected.score
	return placement, nil
}

func chooseCandidate(candidates []candidate, bestSingle time.Duration, anyMeasured bool) (candidate, bool) {
	if len(candidates) == 0 {
		return candidate{}, false
	}
	if !anyMeasured {
		sortByFreeSlots(candidates)
		return candidates[0], true
	}
	var paced []candidate
	for _, item := range candidates {
		if item.measured && item.reserved < item.limit && item.single <= bestSingle+resultGap {
			paced = append(paced, item)
		}
	}
	if len(paced) > 0 {
		sort.Slice(paced, func(i, j int) bool {
			if paced[i].reserved == paced[j].reserved {
				if paced[i].single == paced[j].single {
					return paced[i].observation.WorkerID < paced[j].observation.WorkerID
				}
				return paced[i].single < paced[j].single
			}
			return paced[i].reserved < paced[j].reserved
		})
		return paced[0], true
	}
	var unmeasured []candidate
	for _, item := range candidates {
		if !item.measured && item.reserved < item.limit {
			unmeasured = append(unmeasured, item)
		}
	}
	if len(unmeasured) == 0 {
		return candidate{}, false
	}
	sortByFreeSlots(unmeasured)
	return unmeasured[0], true
}

func sortByFreeSlots(candidates []candidate) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].observation.WorkerID < candidates[j].observation.WorkerID
		}
		return candidates[i].score > candidates[j].score
	})
}

func (s *Scheduler) paceAnchorLocked(request Request, now time.Time) (time.Duration, bool) {
	var best time.Duration
	anyMeasured := false
	for _, observation := range s.registry.Observations() {
		reason := rejectionReason(observation, request, now)
		if reason != "" && reason != "insufficient resources" {
			continue
		}
		mbps := s.medianMbpsLocked(observation.WorkerID, now)
		if mbps <= 0 {
			continue
		}
		anyMeasured = true
		single := singleChunkDuration(mbps)
		if best == 0 || single < best {
			best = single
		}
	}
	return best, anyMeasured
}

func (s *Scheduler) medianMbpsLocked(workerID string, now time.Time) float64 {
	cutoff := now.Add(-paceSampleWindow)
	values := make([]float64, 0, len(s.samples[workerID]))
	for _, item := range s.samples[workerID] {
		if item.at.After(cutoff) {
			values = append(values, item.mbps)
		}
	}
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	mid := len(values) / 2
	if len(values)%2 == 1 {
		return values[mid]
	}
	return (values[mid-1] + values[mid]) / 2
}

func (s *Scheduler) pruneSamplesLocked(now time.Time) {
	cutoff := now.Add(-paceSampleWindow)
	for workerID, pending := range s.samples {
		kept := pending[:0]
		for _, item := range pending {
			if item.at.After(cutoff) {
				kept = append(kept, item)
			}
		}
		if len(kept) > maxPaceSamples {
			kept = append([]sample(nil), kept[len(kept)-maxPaceSamples:]...)
		}
		if len(kept) == 0 {
			delete(s.samples, workerID)
			continue
		}
		s.samples[workerID] = kept
	}
}

func singleChunkDuration(mbps float64) time.Duration {
	if mbps <= 0 {
		return assignmentWave
	}
	seconds := float64(chunkBytes*8) / (mbps * 1_000_000)
	return time.Duration(seconds * float64(time.Second))
}

func paceLimit(single time.Duration) int64 {
	if single <= 0 {
		return 1
	}
	limit := int64(assignmentWave / single)
	if limit < 1 {
		return 1
	}
	return limit
}

func reservedBandwidth(pending []reservation, observedAt time.Time) int64 {
	cutoff := observedAt.Add(-time.Second)
	var total int64
	for _, item := range pending {
		if !item.at.Before(cutoff) {
			total += item.resources.BandwidthMbps
		}
	}
	return total
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
