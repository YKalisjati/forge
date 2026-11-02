package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/YKalisjati/forge/internal/adapter/postgres"
	"github.com/YKalisjati/forge/internal/adapter/rabbitmq"
	appOutbox "github.com/YKalisjati/forge/internal/app/outbox"
	appWorker "github.com/YKalisjati/forge/internal/app/worker"
	"github.com/YKalisjati/forge/internal/app/worker/handlers"
	"github.com/YKalisjati/forge/pkg/config"
	"github.com/YKalisjati/forge/pkg/logger"
)

// BenchmarkResult records statistical metrics of a test run.
type BenchmarkResult struct {
	Scenario        string        `json:"scenario"`
	Concurrency     int           `json:"concurrency"`
	Duration        time.Duration `json:"duration"`
	TotalRequests   int64         `json:"total_requests"`
	SuccessRequests int64         `json:"success_requests"`
	FailedRequests  int64         `json:"failed_requests"`
	ThroughputRPS   float64       `json:"throughput_rps"`
	MinLatencyMS    float64       `json:"min_latency_ms"`
	P50LatencyMS    float64       `json:"p50_latency_ms"`
	P90LatencyMS    float64       `json:"p90_latency_ms"`
	P95LatencyMS    float64       `json:"p95_latency_ms"`
	P99LatencyMS    float64       `json:"p99_latency_ms"`
	MaxLatencyMS    float64       `json:"max_latency_ms"`
	AvgLatencyMS    float64       `json:"avg_latency_ms"`
	StatusCounts    map[int]int64 `json:"status_counts"`
}

func main() {
	scenario := flag.String("scenario", "all", "Scenario to execute: post, get, mixed, worker, or all")
	targetURL := flag.String("target-url", "http://localhost:8080", "Base API URL")
	concurrency := flag.Int("concurrency", 50, "Number of concurrent virtual users")
	duration := flag.Duration("duration", 20*time.Second, "Active benchmark measurement duration")
	warmup := flag.Duration("warmup", 5*time.Second, "Warm-up duration before metrics recording")
	workerJobs := flag.Int("worker-jobs", 1000, "Number of jobs to process per worker concurrency configuration")
	workerConcurrencies := flag.String("worker-concurrency", "5,15,30", "Comma-separated worker concurrency limits")
	outputJSON := flag.String("output-json", "", "Optional path to export raw benchmark JSON results")
	flag.Parse()

	fmt.Println("==============================================================")
	fmt.Println("        FORGE PERFORMANCE BENCHMARK HARNESS                   ")
	fmt.Println("==============================================================")
	fmt.Printf("Target API URL:       %s\n", *targetURL)
	fmt.Printf("HTTP Concurrency:     %d VUs\n", *concurrency)
	fmt.Printf("Warm-up Duration:     %v\n", *warmup)
	fmt.Printf("Measurement Duration: %v\n", *duration)
	fmt.Printf("Scenario Selected:    %s\n", *scenario)
	fmt.Println("==============================================================")

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        500,
			MaxIdleConnsPerHost: 500,
			MaxConnsPerHost:     500,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  false,
		},
		Timeout: 10 * time.Second,
	}

	results := make([]BenchmarkResult, 0)

	// Verify API is reachable before starting HTTP scenarios
	if *scenario != "worker" {
		resp, err := client.Get(*targetURL + "/healthz")
		if err != nil {
			fmt.Printf("FATAL: Target API %s is not reachable: %v\n", *targetURL, err)
			os.Exit(1)
		}
		resp.Body.Close()
		fmt.Printf("Verified target API is alive at %s\n\n", *targetURL)
	}

	// Execute HTTP Scenarios
	if *scenario == "post" || *scenario == "all" {
		res := runPostBenchmark(client, *targetURL, *concurrency, *warmup, *duration)
		results = append(results, res)
		printResultTable(res)
	}

	if *scenario == "get" || *scenario == "all" {
		seededIDs := seedJobs(client, *targetURL, 200)
		res := runGetBenchmark(client, *targetURL, seededIDs, *concurrency, *warmup, *duration)
		results = append(results, res)
		printResultTable(res)
	}

	if *scenario == "mixed" || *scenario == "all" {
		seededIDs := seedJobs(client, *targetURL, 200)
		res := runMixedBenchmark(client, *targetURL, seededIDs, *concurrency, *warmup, *duration)
		results = append(results, res)
		printResultTable(res)
	}

	// Execute Worker Concurrency Scenarios
	if *scenario == "worker" || *scenario == "all" {
		concurrencyLevels := parseWorkerConcurrencies(*workerConcurrencies)
		for _, wConc := range concurrencyLevels {
			res := runWorkerBenchmark(wConc, *workerJobs)
			results = append(results, res)
			printWorkerResult(res, wConc, *workerJobs)
		}
	}

	if *outputJSON != "" {
		data, err := json.MarshalIndent(results, "", "  ")
		if err == nil {
			_ = os.WriteFile(*outputJSON, data, 0644)
			fmt.Printf("\nSaved full JSON benchmark report to: %s\n", *outputJSON)
		}
	}
}

func parseWorkerConcurrencies(raw string) []int {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		if v, err := strconv.Atoi(strings.TrimSpace(p)); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return []int{5, 15, 30}
	}
	return out
}

func generatePayload() []byte {
	// Generate ~1KB synthetic report.generate payload with 40 data points
	type reportPayload struct {
		ReportID   string    `json:"report_id"`
		Title      string    `json:"title"`
		Author     string    `json:"author"`
		DataPoints []float64 `json:"data_points"`
		Timestamp  string    `json:"timestamp"`
	}

	type submitReq struct {
		Type       string        `json:"type"`
		Payload    reportPayload `json:"payload"`
		Priority   int           `json:"priority"`
		MaxRetries int           `json:"max_retries"`
	}

	dataPoints := make([]float64, 40)
	for i := range dataPoints {
		dataPoints[i] = math.Round(float64((i*17)%1000)*100) / 100
	}

	req := submitReq{
		Type: "report.generate",
		Payload: reportPayload{
			ReportID:   "rep-" + uuid.New().String(),
			Title:      "Synthetic Benchmark Report Generation Workload",
			Author:     "bench-harness",
			DataPoints: dataPoints,
			Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
		},
		Priority:   10,
		MaxRetries: 3,
	}

	bytes, _ := json.Marshal(req)
	return bytes
}

func randomUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func seedJobs(client *http.Client, targetURL string, count int) []string {
	fmt.Printf("\nSeeding %d jobs for status polling benchmark...", count)
	ids := make([]string, 0, count)
	payload := generatePayload()

	for i := 0; i < count; i++ {
		req, _ := http.NewRequest("POST", targetURL+"/v1/jobs", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", randomUUID())

		resp, err := client.Do(req)
		if err == nil {
			if resp.StatusCode == http.StatusAccepted {
				var body struct {
					ID string `json:"id"`
				}
				_ = json.NewDecoder(resp.Body).Decode(&body)
				if body.ID != "" {
					ids = append(ids, body.ID)
				}
			}
			resp.Body.Close()
		}
	}
	fmt.Printf(" Done (%d jobs ready)\n", len(ids))
	return ids
}

// runPostBenchmark tests sustained ingestion throughput on POST /v1/jobs.
func runPostBenchmark(client *http.Client, targetURL string, concurrency int, warmup, duration time.Duration) BenchmarkResult {
	fmt.Printf("\n[1/4] Running POST /v1/jobs Ingestion Benchmark (%d VUs, %v warmup + %v duration)...\n", concurrency, warmup, duration)

	var (
		totalReqs   int64
		successReqs int64
		failedReqs  int64
		statusMu    sync.Mutex
		statusCodes = make(map[int]int64)
		latenciesMu sync.Mutex
		latencies   = make([]float64, 0, 100000)
		stopWarmup  = make(chan struct{})
		stopRun     = make(chan struct{})
		payload     = generatePayload()
	)

	// Warm-up phase
	var wgWarmup sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wgWarmup.Add(1)
		go func() {
			defer wgWarmup.Done()
			for {
				select {
				case <-stopWarmup:
					return
				default:
					req, _ := http.NewRequest("POST", targetURL+"/v1/jobs", bytes.NewReader(payload))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Idempotency-Key", randomUUID())
					resp, err := client.Do(req)
					if err == nil {
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				}
			}
		}()
	}

	time.Sleep(warmup)
	close(stopWarmup)
	wgWarmup.Wait()
	fmt.Println("Warm-up complete. Recording measurement...")

	// Active measurement phase
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			localLatencies := make([]float64, 0, 2000)
			for {
				select {
				case <-stopRun:
					latenciesMu.Lock()
					latencies = append(latencies, localLatencies...)
					latenciesMu.Unlock()
					return
				default:
					t0 := time.Now()
					req, _ := http.NewRequest("POST", targetURL+"/v1/jobs", bytes.NewReader(payload))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Idempotency-Key", randomUUID())

					resp, err := client.Do(req)
					elapsedMS := float64(time.Since(t0).Microseconds()) / 1000.0

					atomic.AddInt64(&totalReqs, 1)
					localLatencies = append(localLatencies, elapsedMS)

					if err != nil {
						atomic.AddInt64(&failedReqs, 1)
						statusMu.Lock()
						statusCodes[0]++
						statusMu.Unlock()
					} else {
						statusMu.Lock()
						statusCodes[resp.StatusCode]++
						statusMu.Unlock()
						if resp.StatusCode == http.StatusAccepted {
							atomic.AddInt64(&successReqs, 1)
						} else {
							atomic.AddInt64(&failedReqs, 1)
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				}
			}
		}()
	}

	time.Sleep(duration)
	close(stopRun)
	wg.Wait()
	actualDuration := time.Since(start)

	return computeBenchmarkResult("POST /v1/jobs (Ingestion)", concurrency, actualDuration, totalReqs, successReqs, failedReqs, statusCodes, latencies)
}

// runGetBenchmark tests status polling on GET /v1/jobs/{id}.
func runGetBenchmark(client *http.Client, targetURL string, seededIDs []string, concurrency int, _, duration time.Duration) BenchmarkResult {
	fmt.Printf("\n[2/4] Running GET /v1/jobs/{id} Polling Benchmark (%d VUs, %v duration)...\n", concurrency, duration)

	var (
		totalReqs   int64
		successReqs int64
		failedReqs  int64
		statusMu    sync.Mutex
		statusCodes = make(map[int]int64)
		latenciesMu sync.Mutex
		latencies   = make([]float64, 0, 100000)
		stopRun     = make(chan struct{})
	)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			localLatencies := make([]float64, 0, 2000)
			cursor := workerIdx % len(seededIDs)
			for {
				select {
				case <-stopRun:
					latenciesMu.Lock()
					latencies = append(latencies, localLatencies...)
					latenciesMu.Unlock()
					return
				default:
					jobID := seededIDs[cursor]
					cursor = (cursor + 1) % len(seededIDs)

					t0 := time.Now()
					req, _ := http.NewRequest("GET", targetURL+"/v1/jobs/"+jobID, nil)
					resp, err := client.Do(req)
					elapsedMS := float64(time.Since(t0).Microseconds()) / 1000.0

					atomic.AddInt64(&totalReqs, 1)
					localLatencies = append(localLatencies, elapsedMS)

					if err != nil {
						atomic.AddInt64(&failedReqs, 1)
						statusMu.Lock()
						statusCodes[0]++
						statusMu.Unlock()
					} else {
						statusMu.Lock()
						statusCodes[resp.StatusCode]++
						statusMu.Unlock()
						if resp.StatusCode == http.StatusOK {
							atomic.AddInt64(&successReqs, 1)
						} else {
							atomic.AddInt64(&failedReqs, 1)
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				}
			}
		}(i)
	}

	time.Sleep(duration)
	close(stopRun)
	wg.Wait()
	actualDuration := time.Since(start)

	return computeBenchmarkResult("GET /v1/jobs/{id} (Polling)", concurrency, actualDuration, totalReqs, successReqs, failedReqs, statusCodes, latencies)
}

// runMixedBenchmark tests a realistic 70% GET / 30% POST workload.
func runMixedBenchmark(client *http.Client, targetURL string, seededIDs []string, concurrency int, _, duration time.Duration) BenchmarkResult {
	fmt.Printf("\n[3/4] Running Mixed 70%% Read / 30%% Write Workload (%d VUs, %v duration)...\n", concurrency, duration)

	var (
		totalReqs   int64
		successReqs int64
		failedReqs  int64
		statusMu    sync.Mutex
		statusCodes = make(map[int]int64)
		latenciesMu sync.Mutex
		latencies   = make([]float64, 0, 100000)
		stopRun     = make(chan struct{})
		payload     = generatePayload()
	)

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			localLatencies := make([]float64, 0, 2000)
			cursor := workerIdx % len(seededIDs)
			step := 0
			for {
				select {
				case <-stopRun:
					latenciesMu.Lock()
					latencies = append(latencies, localLatencies...)
					latenciesMu.Unlock()
					return
				default:
					step++
					isRead := (step % 10) < 7 // 70% reads, 30% writes

					var req *http.Request
					if isRead {
						jobID := seededIDs[cursor]
						cursor = (cursor + 1) % len(seededIDs)
						req, _ = http.NewRequest("GET", targetURL+"/v1/jobs/"+jobID, nil)
					} else {
						req, _ = http.NewRequest("POST", targetURL+"/v1/jobs", bytes.NewReader(payload))
						req.Header.Set("Content-Type", "application/json")
						req.Header.Set("Idempotency-Key", randomUUID())
					}

					t0 := time.Now()
					resp, err := client.Do(req)
					elapsedMS := float64(time.Since(t0).Microseconds()) / 1000.0

					atomic.AddInt64(&totalReqs, 1)
					localLatencies = append(localLatencies, elapsedMS)

					if err != nil {
						atomic.AddInt64(&failedReqs, 1)
						statusMu.Lock()
						statusCodes[0]++
						statusMu.Unlock()
					} else {
						statusMu.Lock()
						statusCodes[resp.StatusCode]++
						statusMu.Unlock()
						if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted {
							atomic.AddInt64(&successReqs, 1)
						} else {
							atomic.AddInt64(&failedReqs, 1)
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				}
			}
		}(i)
	}

	time.Sleep(duration)
	close(stopRun)
	wg.Wait()
	actualDuration := time.Since(start)

	return computeBenchmarkResult("Mixed Workload (70% Read / 30% Write)", concurrency, actualDuration, totalReqs, successReqs, failedReqs, statusCodes, latencies)
}

// runWorkerBenchmark executes an isolated worker throughput and queue drain test.
func runWorkerBenchmark(workerConcurrency, numJobs int) BenchmarkResult {
	fmt.Printf("\n[4/4] Testing Worker Drain Throughput: Concurrency = %d, Jobs = %d...\n", workerConcurrency, numJobs)

	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return BenchmarkResult{}
	}

	ctx := context.Background()
	log := logger.New("bench-worker", &cfg.App)

	// Connect PostgreSQL
	pgClient, err := postgres.NewClient(ctx, &cfg.Postgres)
	if err != nil {
		fmt.Printf("PostgreSQL connection error: %v\n", err)
		return BenchmarkResult{}
	}
	defer pgClient.Close()
	pool := pgClient.Pool()

	// Connect RabbitMQ
	rmqClient, err := rabbitmq.NewClient(ctx, &cfg.RabbitMQ)
	if err != nil {
		fmt.Printf("RabbitMQ connection error: %v\n", err)
		return BenchmarkResult{}
	}
	defer rmqClient.Close()

	// Ensure durable topology
	_ = rabbitmq.DeclareTopology(ctx, rmqClient, &cfg.RabbitMQ)

	// Clean any residual jobs from previous runs
	_, _ = pool.Exec(ctx, "TRUNCATE jobs, job_executions, idempotency_records, outbox_messages CASCADE;")

	// Purge RabbitMQ queue
	ch, chErr := rmqClient.Connection().Channel()
	if chErr == nil {
		_, _ = ch.QueuePurge(cfg.RabbitMQ.Queue, false)
		ch.Close()
	}

	// 1. Bulk insert numJobs directly into jobs and outbox_messages
	fmt.Printf("Pre-populating %d jobs and outbox records in PostgreSQL...", numJobs)
	insertStart := time.Now()
	syntheticPayload := `{"report_id":"bench","title":"Bench","data_points":[10.5,20.3,30.1],"author":"benchmarker"}`

	// Chunked inserts for fast insertion
	batchSize := 200
	for i := 0; i < numJobs; i += batchSize {
		chunkEnd := i + batchSize
		if chunkEnd > numJobs {
			chunkEnd = numJobs
		}
		var qb strings.Builder
		qb.WriteString("INSERT INTO jobs (id, idempotency_key, type, payload, state, priority, max_retries, next_attempt_at, created_at, updated_at) VALUES ")
		var ob strings.Builder
		ob.WriteString("INSERT INTO outbox_messages (job_id, event_type, payload, status, scheduled_at, created_at) VALUES ")

		for j := i; j < chunkEnd; j++ {
			jobID := uuid.New().String()
			idemKey := "bench-" + hex.EncodeToString([]byte(jobID[:8])) + "-" + strconv.Itoa(j)
			nowStr := time.Now().UTC().Format(time.RFC3339Nano)

			if j > i {
				qb.WriteString(",")
				ob.WriteString(",")
			}
			qb.WriteString(fmt.Sprintf("('%s', '%s', 'report.generate', '%s'::jsonb, 'pending', 0, 3, '%s', '%s', '%s')", jobID, idemKey, syntheticPayload, nowStr, nowStr, nowStr))
			ob.WriteString(fmt.Sprintf("('%s', 'job.submitted', '{\"job_id\":\"%s\"}'::jsonb, 'pending', '%s', '%s')", jobID, jobID, nowStr, nowStr))
		}
		qb.WriteString(";")
		ob.WriteString(";")

		if _, err := pool.Exec(ctx, qb.String()); err != nil {
			fmt.Printf("Job insert failed: %v\n", err)
			return BenchmarkResult{}
		}
		if _, err := pool.Exec(ctx, ob.String()); err != nil {
			fmt.Printf("Outbox insert failed: %v\n", err)
			return BenchmarkResult{}
		}
	}
	fmt.Printf(" Completed in %v\n", time.Since(insertStart).Round(time.Millisecond))

	// 2. Start Outbox Relay
	outboxRepo := postgres.NewOutboxRepository(pool)
	publisher := rabbitmq.NewPublisher(rmqClient, cfg.RabbitMQ.Exchange, cfg.RabbitMQ.Queue)
	defer publisher.Close()
	relay := appOutbox.NewRelay(outboxRepo, publisher, nil, log, appOutbox.Config{
		PollInterval:       25 * time.Millisecond,
		BatchSize:          200,
		MaxRetries:         5,
		ClaimLeaseDuration: 30 * time.Second,
		PublishTimeout:     3 * time.Second,
	})

	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()
	go func() {
		_ = relay.Run(relayCtx)
	}()

	// 3. Start Worker
	jobRepo := postgres.NewJobRepository(pool)
	consumer := rabbitmq.NewConsumer(rmqClient)
	defer consumer.Close()

	registry := appWorker.NewRegistry()
	_ = registry.Register("report.generate", handlers.NewReportHandler(log))
	_ = registry.Register("test.job", handlers.NewTestJobHandler())

	worker := appWorker.NewWorker(jobRepo, consumer, registry, nil, log, appWorker.Config{
		Queue:             cfg.RabbitMQ.Queue,
		Concurrency:       workerConcurrency,
		PrefetchCount:     workerConcurrency * 2,
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	})

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	go func() {
		_ = worker.Run(workerCtx)
	}()

	// 4. Poll until all jobs are in completed state
	drainStart := time.Now()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	var completedCount int
	timeout := time.After(3 * time.Minute)

	for {
		select {
		case <-timeout:
			fmt.Printf("Worker drain benchmark timed out before completing all jobs (completed: %d/%d)\n", completedCount, numJobs)
			goto FINISHED
		case <-ticker.C:
			_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE state = 'completed'").Scan(&completedCount)
			if completedCount >= numJobs {
				goto FINISHED
			}
		}
	}

FINISHED:
	drainDuration := time.Since(drainStart)
	cancelRelay()
	cancelWorker()

	// 5. Query end-to-end latencies (completed_at - created_at) from PostgreSQL
	rows, qErr := pool.Query(ctx, `
		SELECT EXTRACT(EPOCH FROM (completed_at - created_at)) * 1000.0 AS duration_ms
		FROM jobs
		WHERE state = 'completed' AND completed_at IS NOT NULL
		ORDER BY duration_ms ASC;
	`)
	latencies := make([]float64, 0, numJobs)
	if qErr == nil {
		for rows.Next() {
			var d float64
			if err := rows.Scan(&d); err == nil {
				latencies = append(latencies, d)
			}
		}
		rows.Close()
	}

	throughput := float64(completedCount) / drainDuration.Seconds()

	res := computeBenchmarkResult(
		fmt.Sprintf("Worker Drain (Concurrency=%d, Prefetch=%d)", workerConcurrency, workerConcurrency*2),
		workerConcurrency,
		drainDuration,
		int64(numJobs),
		int64(completedCount),
		int64(numJobs-completedCount),
		map[int]int64{200: int64(completedCount)},
		latencies,
	)
	res.ThroughputRPS = throughput
	return res
}

func computeBenchmarkResult(
	scenario string,
	concurrency int,
	duration time.Duration,
	total, success, failed int64,
	statusCodes map[int]int64,
	latencies []float64,
) BenchmarkResult {
	res := BenchmarkResult{
		Scenario:        scenario,
		Concurrency:     concurrency,
		Duration:        duration,
		TotalRequests:   total,
		SuccessRequests: success,
		FailedRequests:  failed,
		StatusCounts:    statusCodes,
	}

	if duration.Seconds() > 0 {
		res.ThroughputRPS = float64(success) / duration.Seconds()
	}

	if len(latencies) > 0 {
		sort.Float64s(latencies)
		res.MinLatencyMS = latencies[0]
		res.MaxLatencyMS = latencies[len(latencies)-1]
		res.P50LatencyMS = percentile(latencies, 50)
		res.P90LatencyMS = percentile(latencies, 90)
		res.P95LatencyMS = percentile(latencies, 95)
		res.P99LatencyMS = percentile(latencies, 99)

		var sum float64
		for _, v := range latencies {
			sum += v
		}
		res.AvgLatencyMS = sum / float64(len(latencies))
	}

	return res
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func printResultTable(r BenchmarkResult) {
	fmt.Printf("\n--- Benchmark Result: %s ---\n", r.Scenario)
	fmt.Printf("| %-30s | %12s |\n", "Metric", "Value")
	fmt.Printf("|%s|%s|\n", strings.Repeat("-", 32), strings.Repeat("-", 14))
	fmt.Printf("| %-30s | %12d |\n", "Concurrency (VUs)", r.Concurrency)
	fmt.Printf("| %-30s | %12s |\n", "Duration", r.Duration.Round(time.Millisecond))
	fmt.Printf("| %-30s | %12d |\n", "Total Requests", r.TotalRequests)
	fmt.Printf("| %-30s | %12d |\n", "Successful (HTTP 2xx)", r.SuccessRequests)
	fmt.Printf("| %-30s | %12d |\n", "Failed Requests", r.FailedRequests)
	fmt.Printf("| %-30s | %12.2f |\n", "Throughput (RPS)", r.ThroughputRPS)
	fmt.Printf("| %-30s | %12.2f ms |\n", "Latency Min", r.MinLatencyMS)
	fmt.Printf("| %-30s | %12.2f ms |\n", "Latency p50", r.P50LatencyMS)
	fmt.Printf("| %-30s | %12.2f ms |\n", "Latency p90", r.P90LatencyMS)
	fmt.Printf("| %-30s | %12.2f ms |\n", "Latency p95", r.P95LatencyMS)
	fmt.Printf("| %-30s | %12.2f ms |\n", "Latency p99", r.P99LatencyMS)
	fmt.Printf("| %-30s | %12.2f ms |\n", "Latency Max", r.MaxLatencyMS)
	fmt.Printf("| %-30s | %12.2f ms |\n", "Latency Avg", r.AvgLatencyMS)

	statusStrs := make([]string, 0, len(r.StatusCounts))
	for code, count := range r.StatusCounts {
		statusStrs = append(statusStrs, fmt.Sprintf("%d: %d", code, count))
	}
	fmt.Printf("| %-30s | %12s |\n", "Status Codes", strings.Join(statusStrs, ", "))
	fmt.Println()
}

func printWorkerResult(r BenchmarkResult, concurrency, jobs int) {
	fmt.Printf("\n--- Worker Drain Result: Concurrency = %d (Prefetch = %d) ---\n", concurrency, concurrency*2)
	fmt.Printf("| %-35s | %14s |\n", "Metric", "Value")
	fmt.Printf("|%s|%s|\n", strings.Repeat("-", 37), strings.Repeat("-", 16))
	fmt.Printf("| %-35s | %14d |\n", "Total Jobs Dispatched", jobs)
	fmt.Printf("| %-35s | %14d |\n", "Jobs Completed", r.SuccessRequests)
	fmt.Printf("| %-35s | %14s |\n", "Drain Time", r.Duration.Round(time.Millisecond))
	fmt.Printf("| %-35s | %14.2f |\n", "Throughput (Jobs/sec)", r.ThroughputRPS)
	fmt.Printf("| %-35s | %14.2f ms |\n", "End-to-End Latency p50", r.P50LatencyMS)
	fmt.Printf("| %-35s | %14.2f ms |\n", "End-to-End Latency p95", r.P95LatencyMS)
	fmt.Printf("| %-35s | %14.2f ms |\n", "End-to-End Latency p99", r.P99LatencyMS)
	fmt.Println()
}
