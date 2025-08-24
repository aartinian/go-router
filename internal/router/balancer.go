package router

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"

	"github.com/rs/zerolog"
)

// Balancer handles task worker selection logic, choosing an appropriate
// task worker for new sessions based on their reported load and capacity.
type Balancer interface {
	SelectWorker(
		ctx context.Context, workers map[string]Heartbeat,
	) (string, error)
}

// WeightedBalancer implements load balancing using weighted random selection
// based on task worker load and capacity.
type WeightedBalancer struct {
	logger zerolog.Logger
	mutex  sync.Mutex
}

// NewWeightedBalancer creates a new weighted random load balancer.
func NewWeightedBalancer(logger zerolog.Logger) *WeightedBalancer {
	return &WeightedBalancer{
		logger: logger,
	}
}

// workerCandidate represents a worker candidate for load balancing selection.
type workerCandidate struct {
	ID        string
	LoadRatio float64
	Heartbeat Heartbeat
}

// SelectWorker selects a task worker using weighted random load balancing.
// It filters out task workers at full capacity and selects from the least
// loaded ones.
func (lb *WeightedBalancer) SelectWorker(
	ctx context.Context, workers map[string]Heartbeat,
) (string, error) {
	// Validate workers
	if len(workers) == 0 {
		return "", fmt.Errorf("no workers found")
	}

	// Filter and collect available workers
	candidates := lb.filterAvailableWorkers(workers)
	if len(candidates) == 0 {
		return "", fmt.Errorf("no workers found with capacity")
	}

	// Sort by load ratio (ascending - least loaded first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].LoadRatio < candidates[j].LoadRatio
	})

	// Perform weighted random selection
	return lb.selectWorkerByWeight(candidates)
}

// filterAvailableWorkers filters out workers that are at full capacity or have
// invalid capacity.
func (lb *WeightedBalancer) filterAvailableWorkers(
	workers map[string]Heartbeat,
) []workerCandidate {
	candidates := make([]workerCandidate, 0, len(workers))

	for workerID, heartbeat := range workers {
		// Skip workers with invalid capacity
		if heartbeat.Capacity <= 0 {
			continue
		}

		// Skip workers at full capacity
		loadRatio := float64(heartbeat.Load) / float64(heartbeat.Capacity)
		if loadRatio >= 1.0 {
			continue
		}

		candidates = append(candidates, workerCandidate{
			ID:        workerID,
			LoadRatio: loadRatio,
			Heartbeat: heartbeat,
		})
	}

	return candidates
}

// selectWorkerByWeight performs weighted random selection based on inverse
// load ratio.
func (lb *WeightedBalancer) selectWorkerByWeight(
	candidates []workerCandidate,
) (string, error) {
	if len(candidates) == 0 {
		return "", fmt.Errorf("no candidates available for selection")
	}

	// If only one candidate, return it immediately
	if len(candidates) == 1 {
		return candidates[0].ID, nil
	}

	// Calculate weights and perform weighted random selection
	weights := lb.calculateWeights(candidates)
	cdIndex := lb.weightedRandomSelect(weights)

	return candidates[cdIndex].ID, nil
}

// calculateWeights calculates weights for each candidate based on inverse
// load ratio.
func (lb *WeightedBalancer) calculateWeights(
	candidates []workerCandidate,
) []float64 {
	weights := make([]float64, len(candidates))

	for i, candidate := range candidates {
		// Weight is inverse of load ratio + epsilon to avoid division by zero
		weight := 1.0 / (candidate.LoadRatio + epsilon)
		weights[i] = weight
	}

	return weights
}

// weightedRandomSelect performs weighted random selection using the cumulative
// weight method.
func (lb *WeightedBalancer) weightedRandomSelect(weights []float64) int {
	// Calculate total weight
	totalWeight := 0.0
	for _, weight := range weights {
		totalWeight += weight
	}

	// Generate random value and select worker
	lb.mutex.Lock()
	randomValue := rand.Float64() * totalWeight
	lb.mutex.Unlock()

	// Select worker based on cumulative weights
	cumulative := 0.0
	for i, weight := range weights {
		cumulative += weight
		if randomValue <= cumulative {
			return i
		}
	}

	// Fallback to last candidate
	return len(weights) - 1
}
