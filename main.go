// Package traefik_batch_middleware is a Traefik plugin for dynamic request batching
package traefik_batch_middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Config holds the plugin configuration
type Config struct {
	MaxBatchSize       int    `json:"maxBatchSize"`
	BatchTimeout       string `json:"batchTimeout"`
	HighPriorityHeader string `json:"highPriorityHeader"`
	PriorityThreshold  int    `json:"priorityThreshold"`
	EnableMetrics      bool   `json:"enableMetrics"`
}

// CreateConfig creates a new plugin configuration
func CreateConfig() *Config {
	return &Config{
		MaxBatchSize:       50,
		BatchTimeout:       "2s",
		HighPriorityHeader: "X-Event-Urgency",
		PriorityThreshold:  8,
		EnableMetrics:      true,
	}
}

// BatchRequest represents a batched HTTP request with metadata
type BatchRequest struct {
	OriginalRequest *http.Request
	ResponseWriter  http.ResponseWriter
	Priority        int
	Timestamp       time.Time
}

// Batch represents a collection of requests to be processed together
type Batch struct {
	Requests  []*BatchRequest
	CreatedAt time.Time
}

// RequestBatcher is the middleware plugin type
type RequestBatcher struct {
	next         http.Handler
	name         string
	config       *Config
	batches      map[string]*Batch // Key is the route
	batchMutex   sync.Mutex
	timers       map[string]*time.Timer
	timerMutex   sync.Mutex
	batchTimeout time.Duration
}

// New creates a new RequestBatcher middleware plugin
func New(ctx context.Context, next http.Handler, config *Config, name string) (http.Handler, error) {
	batchTimeout, err := time.ParseDuration(config.BatchTimeout)
	if err != nil {
		return nil, err
	}

	return &RequestBatcher{
		next:         next,
		name:         name,
		config:       config,
		batches:      make(map[string]*Batch),
		timers:       make(map[string]*time.Timer),
		batchTimeout: batchTimeout,
	}, nil
}

// ServeHTTP implements the middleware functionality
func (r *RequestBatcher) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	// Extract route identifier (you might want to customize this based on your needs)
	routeID := req.URL.Path
	req.Header.Add("X-Recieved-Time", time.Now().Format(time.RFC3339))
	// Create batch request
	batchReq := &BatchRequest{
		OriginalRequest: req,
		ResponseWriter:  rw,
		Priority:        r.getPriority(req),
		Timestamp:       time.Now(),
	}

	r.batchMutex.Lock()

	// Create new batch if doesn't exist for this route
	if _, exists := r.batches[routeID]; !exists {
		r.batches[routeID] = &Batch{
			Requests:  make([]*BatchRequest, 0),
			CreatedAt: time.Now(),
		}
		// Start timer for this batch
		r.startBatchTimer(routeID)
	}

	// Add request to batch
	r.batches[routeID].Requests = append(r.batches[routeID].Requests, batchReq)

	// Check if we should process the batch
	shouldProcess := len(r.batches[routeID].Requests) >= r.config.MaxBatchSize ||
		batchReq.Priority >= r.config.PriorityThreshold

	if shouldProcess {
		batch := r.batches[routeID]
		delete(r.batches, routeID)
		r.batchMutex.Unlock()

		r.processBatch(batch)
		return
	}

	r.batchMutex.Unlock()
}

// startBatchTimer starts a timer for the batch
func (r *RequestBatcher) startBatchTimer(routeID string) {
	r.timerMutex.Lock()
	defer r.timerMutex.Unlock()

	if timer, exists := r.timers[routeID]; exists {
		timer.Stop()
	}

	r.timers[routeID] = time.AfterFunc(r.batchTimeout, func() {
		r.batchMutex.Lock()
		if batch, exists := r.batches[routeID]; exists {
			delete(r.batches, routeID)
			r.batchMutex.Unlock()
			r.processBatch(batch)
			return
		}
		r.batchMutex.Unlock()
	})
}

// processBatch handles the batch processing
func (r *RequestBatcher) processBatch(batch *Batch) {
	// Sort requests by priority
	r.sortBatchByPriority(batch)

	// Create a wait group to track all requests in the batch
	var wg sync.WaitGroup
	wg.Add(len(batch.Requests))

	// Process each request in the batch
	for _, req := range batch.Requests {
		go func(batchReq *BatchRequest) {
			defer wg.Done()
			batchReq.OriginalRequest.Header.Add("X-Exit-Time", time.Now().Format(time.RFC3339))
			r.next.ServeHTTP(batchReq.ResponseWriter, batchReq.OriginalRequest)
		}(req)
	}

	// Wait for all requests to complete
	wg.Wait()
}

// sortBatchByPriority sorts the batch requests by priority
func (r *RequestBatcher) sortBatchByPriority(batch *Batch) {
	// Simple bubble sort (use sort.Slice in production)
	for i := 0; i < len(batch.Requests)-1; i++ {
		for j := 0; j < len(batch.Requests)-i-1; j++ {
			if batch.Requests[j].Priority < batch.Requests[j+1].Priority {
				batch.Requests[j], batch.Requests[j+1] = batch.Requests[j+1], batch.Requests[j]
			}
		}
	}
}

// getPriority extracts priority from request headers
func (r *RequestBatcher) getPriority(req *http.Request) int {
	priorityStr := req.Header.Get(r.config.HighPriorityHeader)
	if priorityStr == "" {
		return 0
	}

	var priority int
	if err := json.Unmarshal([]byte(priorityStr), &priority); err != nil {
		return 0
	}
	return priority
}
