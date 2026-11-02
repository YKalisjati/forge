// k6 Load Testing Suite for Forge Background Job Engine
// Execute with: k6 run benchmarks/k6_scenarios.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const jobSubmissionDuration = new Trend('job_submission_duration_ms', true);
const jobPollingDuration = new Trend('job_polling_duration_ms', true);
const successfulSubmissions = new Counter('jobs_submitted_success');
const submissionErrors = new Rate('job_submission_error_rate');

export const options = {
  scenarios: {
    // Scenario 1: Sustained Ingestion Load Test (POST /v1/jobs)
    ingestion_baseline: {
      executor: 'ramping-vus',
      startVUs: 5,
      stages: [
        { duration: '10s', target: 20 },  // Warm-up
        { duration: '30s', target: 50 },  // Sustained load
        { duration: '10s', target: 0 },   // Ramp-down
      ],
      gracefulRampDown: '5s',
      exec: 'submitJobScenario',
    },

    // Scenario 2: Status Polling Stress Test (GET /v1/jobs/:id)
    polling_stress: {
      executor: 'constant-vus',
      vus: 30,
      duration: '30s',
      startTime: '55s', // Run after ingestion has created jobs
      exec: 'pollJobScenario',
    },

    // Scenario 3: Mixed Workload (30% POST, 70% GET)
    mixed_workload: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '10s', target: 30 },
        { duration: '30s', target: 60 },
        { duration: '10s', target: 0 },
      ],
      startTime: '90s',
      exec: 'mixedScenario',
    },
  },
  thresholds: {
    'http_req_duration{scenario:ingestion_baseline}': ['p(95)<100', 'p(99)<250'],
    'http_req_duration{scenario:polling_stress}': ['p(95)<20', 'p(99)<50'],
    'http_req_failed': ['rate<0.01'], // < 1% error rate
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8080';

// Global array of seeded Job IDs for polling
let seededJobIds = [];

// Helper: Generate UUID v4
function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// Generate ~1KB synthetic payload
function generate1KBPayload() {
  const dataPoints = [];
  for (let i = 0; i < 40; i++) {
    dataPoints.push(Math.round(Math.random() * 10000) / 100);
  }
  return {
    type: 'report.generate',
    payload: {
      report_id: `rep-${uuidv4()}`,
      title: 'Performance Benchmark Load Generation Report',
      author: 'k6-load-agent',
      data_points: dataPoints,
      timestamp: new Date().toISOString(),
    },
    priority: 10,
    max_retries: 3,
  };
}

export function submitJobScenario() {
  const idempotencyKey = uuidv4();
  const payload = JSON.stringify(generate1KBPayload());
  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyKey,
    },
  };

  const start = Date.now();
  const res = http.post(`${BASE_URL}/v1/jobs`, payload, params);
  const latency = Date.now() - start;
  jobSubmissionDuration.add(latency);

  const ok = check(res, {
    'status is 202': (r) => r.status === 202,
    'has job id': (r) => {
      try {
        const body = JSON.parse(r.body);
        if (body.id && seededJobIds.length < 500) {
          seededJobIds.push(body.id);
        }
        return !!body.id;
      } catch (e) {
        return false;
      }
    },
  });

  if (ok) {
    successfulSubmissions.add(1);
    submissionErrors.add(0);
  } else {
    submissionErrors.add(1);
  }
}

export function pollJobScenario() {
  let targetId = seededJobIds[Math.floor(Math.random() * seededJobIds.length)];
  if (!targetId) {
    // Fallback if seeded list empty
    targetId = uuidv4();
  }

  const start = Date.now();
  const res = http.get(`${BASE_URL}/v1/jobs/${targetId}`);
  jobPollingDuration.add(Date.now() - start);

  check(res, {
    'status is 200 or 404': (r) => r.status === 200 || r.status === 404,
  });
}

export function mixedScenario() {
  const isRead = Math.random() < 0.7; // 70% reads, 30% writes
  if (isRead) {
    pollJobScenario();
  } else {
    submitJobScenario();
  }
}

