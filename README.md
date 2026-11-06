# JQueue

A distributed task queue in Java 21, backed by Redis — delayed and scheduled jobs,
priority queues, weighted worker pools, and the reliability machinery that makes
at-least-once delivery actually hold: retries with exponential backoff, a dead-letter
queue, idempotency keys, and graceful shutdown that finishes in-flight work.

Four Prometheus metrics, a provisioned Grafana dashboard, and a bundled web dashboard for
inspecting and retrying jobs.

Built on **DDD + Clean Architecture** with **Spring Boot 4** as the composition root — the
domain and application layers are plain Java with no framework annotations.

> **Origin.** JQueue is a Java port of [GoQueue](https://github.com/anashasan312/goqueue),
> a Go service. The architecture, the Redis data model and the seven Lua scripts are
> carried over deliberately unchanged; what changed is everything the language and
> ecosystem decide. [From Go to Java](#from-go-to-java) maps one onto the other.

**Stack:** Java 21 · Spring Boot 4.1 (Spring MVC, Tomcat) · Jedis 7 · Redis 7 + Lua ·
Micrometer / Prometheus · Grafana · JUnit 5 · AssertJ · Awaitility · JaCoCo · Docker ·
GitHub Actions

---

## Quickstart

### The whole stack in one command

```bash
docker compose -f deployment/docker-compose.yml up --build
./scripts/seed.sh
```

| | |
|---|---|
| Operator dashboard | http://localhost:9090/dashboard |
| Job API | http://localhost:8080/api/v1 |
| Metrics | http://localhost:9090/metrics |
| Prometheus | http://localhost:9091 |
| Grafana | http://localhost:3000 (admin / admin) |

If Docker reports `Bind for 0.0.0.0:6379 failed: port is already allocated`, another Redis
is already using that port. Either stop it, or publish this stack's Redis elsewhere:
`JQUEUE_REDIS_HOST_PORT=6380 docker compose -f deployment/docker-compose.yml up --build`
(then use `REDIS_ADDR=127.0.0.1:6380` for the integration tests).

### From source

Only a JDK 21 is required; `./mvnw` downloads the pinned Maven on first use.

```bash
make env-up      # start Redis on 127.0.0.1:6379 (or run any local Redis)
make run         # build and start the service
make seed        # enqueue a spread of demo jobs
```

`make help` lists every target.

---

## Why it is built this way

### Dependencies point inwards

```
com.jqueue
 ├── JQueueApplication     process entry point
 ├── server/                the two HTTP listeners and the filter chain
 ├── api/                   controllers and middleware — HTTP in, HTTP out
 ├── application/           services — orchestration, no business rules
 ├── domain/                aggregates, value objects, ports  ← nothing above depends on
 ├── contracts/             request/response DTOs
 ├── hydrator/              domain → contract mapping
 ├── infrastructure/        Redis, Prometheus, worker pool, scheduler, config
 ├── di/                    the composition root
 └── tasks/                 the demo task handlers
```

The domain imports nothing from the layers around it. It does not know Redis exists, does
not know Spring exists, and has never heard of Prometheus. It declares the interfaces it
needs — `JobRepository`, `JobBroker`, `QueueRepository`, `IdempotencyStore`,
`MetricsRecorder` — and `di/JQueueConfiguration` is the single class that decides which
concrete type satisfies each one.

Spring's component scan is limited to `api`, `server` and `di`. Every domain, application
and infrastructure object is built with an explicit constructor call in the composition
root, so none of them carries a Spring annotation, and every one of them can be
constructed by hand in a test.

The practical payoff is `src/test/.../testing/InMemoryStore`: a complete in-memory
implementation of every port. The enqueuer, the inspector, the processor and the worker
pool are all tested against it, with no Redis, no Docker and no network. Swapping Redis
for Postgres would add one adapter package and change the composition root, and nothing
else.

### Business rules live on the aggregate, not in services

`Job` is the consistency boundary. Its fields are private and every lifecycle change goes
through a method that enforces an invariant:

```java
FailureOutcome outcome = job.fail(cause, clock.instant());  // the JOB decides: retry, or dead-letter?
if (outcome.shouldRetry()) {
    broker.retry(job, outcome.retryAt());
} else {
    broker.kill(job);
}
```

There is no retry arithmetic in `DefaultJobProcessorService`. It asks the job what
happened and reacts. "Is this job out of retries" is an invariant of the job, so it is
answered in one place that can be unit-tested without a broker — and two callers cannot
drift apart on it.

The legal transitions are a table in `domain/job/vo/JobState`. Nothing can move a job from
`completed` back to `pending`, because the type refuses.

### SOLID, concretely

**Single responsibility.** The three pieces of job execution are separate types with
separate reasons to change: `WorkerPool` owns concurrency and shutdown;
`DefaultJobProcessorService` owns what one execution means; `RedisJobBroker` owns atomic
movement between Redis sets. That split is why graceful shutdown is testable without a
Redis and retry policy is testable without a thread.

**Open/closed.** Two real extension points. A new task type is a call to
`registry.register(...)` — the pool, the processor and the routes are untouched. A new
backoff curve is a class implementing `BackoffStrategy` — `RetryPolicy` does not change.

**Liskov.** `InMemoryStore` implements the same four ports as the Redis adapters, and its
`dequeue` holds a write lock for exactly as long as the Lua script holds Redis — so a
concurrency test written against the fake means something about the real broker.

**Interface segregation.** `EnqueuerService` and `InspectorService` are separate because a
producer that only submits work should not be handed a dependency that can also delete
jobs and pause queues. Configuration is the same idea: `props.workerPoolConfig()` hands
the pool a small record rather than the whole configuration document.

**Dependency inversion.** Interfaces are declared by the consumer, in `domain/persistence`,
and implemented in `infrastructure/redis`. `di` is the only place both are named together.

---

## How the queue works

### Redis data model

| Key | Type | Purpose |
|---|---|---|
| `{ns}:job:{id}` | hash | the job document |
| `{ns}:queue:{q}:pending` | zset | eligible for dequeue, scored by priority then age |
| `{ns}:queue:{q}:active` | zset | leased, scored by lease expiry |
| `{ns}:queue:{q}:scheduled` | zset | future jobs, scored by process-at |
| `{ns}:queue:{q}:retrying` | zset | failed jobs, scored by next attempt |
| `{ns}:queue:{q}:completed` | zset | succeeded, scored by retention deadline |
| `{ns}:queue:{q}:dead` | zset | dead-letter, scored by time of death |
| `{ns}:queue:{q}:meta` | hash | weight and paused flag |
| `{ns}:queues` | set | the queue registry |
| `{ns}:idempotency:{key}` | string | deduplication claim, with TTL |

A pending job's score is `(MaxPriority − priority) × 10¹⁴ + enqueuedAtMs`, so `ZPOPMIN`
returns the highest priority first and, within one priority, the oldest first. The band is
`10¹⁴`: wide enough that millisecond timestamps cannot bleed into the next band for
thousands of years, small enough that the largest score stays inside the range where
floating-point arithmetic in Lua is exact.

Below the namespace prefix, the key layout, hash fields and payload encoding are the same
as the original GoQueue's, so setting `namespace: goqueue` should let the two share one
Redis. (Not tested against a running GoQueue instance.)

### Atomicity

Seven Lua scripts in `src/main/resources/lua/` do the work that must be indivisible. They
are byte-for-byte the originals. `dequeue.lua` is the important one: `ZPOPMIN` and the move
into the active set happen in a single Redis execution, which is what makes it impossible
for two workers to claim the same job.

Scripts run by SHA (`EVALSHA`), with the SHA computed locally; a cold script cache falls
back to `EVAL` transparently (`LuaScript.run`).

`RedisBrokerIT` proves the atomicity with twenty threads racing for a hundred jobs against
a real Redis, and fifty threads racing for one idempotency key.

### At-least-once delivery

A dequeued job carries a lease, written by the same script that claimed it, so a worker
that dies between the claim and its own bookkeeping still leaves a reclaimable job behind.
The reclaim sweep returns jobs with lapsed leases to `pending`. The failed attempt stays
counted, so an orphaned job does not get an unlimited retry budget, and `last_error`
records that it was orphaned rather than that it failed on its own merits.

The consequence worth stating plainly: **handlers must be idempotent.** A job can run more
than once — that is the trade this design makes on purpose, because the alternative
(at-most-once) silently drops work when a worker dies.

### Timeouts and crashes

Each handler runs on its own **virtual thread**; the worker waits on it with
`Future.get(timeout)`. When the timeout elapses the handler is interrupted and the attempt
is recorded as `job_timeout`, so a stuck handler cannot hold a worker slot. An unchecked
exception escaping a handler is caught and recorded as `handler_panic` — one bad handler
must not take the pool down.

### Graceful shutdown

On `SIGTERM`, in order: the HTTP listeners stop accepting, then the worker pool drains,
then the scheduler stops. The order comes from Spring `SmartLifecycle` phases
(`di/ManagedComponent`): lifecycle beans stop in reverse phase order, and the web server
sits in the highest phase. Reversing it would let the API keep enqueuing into a queue
nobody is draining.

The pool's stop signal breaks the *dequeue* loop, but worker threads are never interrupted,
so a job already running completes. Jobs still running when `shutdown_timeout` elapses are
abandoned; their leases lapse and the reclaim sweep requeues them. A slow shutdown is a
latency event, not data loss.

Verified by `WorkerPoolTest.stopWaitsForInFlightJobs`, and by hand: a 5-second job
in flight when `SIGTERM` arrives finishes before the process exits.

### Priority vs. weight

Two different fairness knobs, deliberately kept separate:

- **Priority** orders jobs *within* a queue. Critical goes before normal.
- **Weight** shares worker attention *across* queues. A queue with weight 6 is polled six
  times as often as one with weight 1.

Together they express "critical work goes first, but the low-traffic queue never starves"
— which neither knob can say on its own.

---

## API

| Method | Path | |
|---|---|---|
| `POST` | `/api/v1/jobs` | enqueue, immediately or scheduled |
| `GET` | `/api/v1/jobs` | list, filtered (`queue`, `state`, `task_type`) and paginated (`offset`, `limit`) |
| `GET` | `/api/v1/jobs/{job_id}` | fetch one job with its full attempt trail |
| `POST` | `/api/v1/jobs/{job_id}/retry` | requeue a dead job with a fresh budget |
| `POST` | `/api/v1/jobs/{job_id}/kill` | move a job to the dead-letter queue |
| `DELETE` | `/api/v1/jobs/{job_id}` | remove a job |
| `GET` | `/api/v1/queues` | depth snapshot of every queue |
| `PATCH` | `/api/v1/queues/{queue_name}/pause` | suspend or resume consumption |
| `POST` | `/api/v1/queues/{queue_name}/dead/retry` | requeue every dead job |

Health, readiness, metrics and the dashboard live on the **private** port (9090), separate
from the client-facing API (8080), so a deployment can expose one through an ingress and
keep the other internal. Both ports share one application context; `PublicPortFilter`
answers 404 for anything but `/health` and `/api/v1/**` on the public port.

Every error has the same shape — `{"code": "job_not_found", "message": "..."}` — and
`GlobalExceptionHandler` is the only place that maps an error kind onto a status code.

### Enqueue

```bash
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "task_type": "email.send",
  "queue": "critical",
  "priority": "critical",
  "payload": {"to": "user@example.com", "subject": "Welcome"},
  "max_retries": 5,
  "retry_base_delay_seconds": 2,
  "backoff_strategy": "exponential",
  "timeout_seconds": 30,
  "idempotency_key": "welcome-user-42"
}'
```

Optional: `delay_seconds` or `run_at` (RFC 3339) to schedule — supplying both is rejected
rather than silently resolved.

Repeating a request with the same `idempotency_key` returns `200` with the **original** job
and `"deduplicated": true`, rather than `409`. That is what makes the endpoint safe for a
client to retry after a timeout.

### Adding a task handler

```java
registry.register(TaskType.of("invoice.generate"), job -> {
    InvoicePayload p = json.readValue(job.payload(), InvoicePayload.class);
    generate(p);   // throw to fail the attempt; the job's retry policy decides what happens next
});
```

Returning normally completes the job; throwing fails the attempt. A checked exception or an
`AppError` is an expected failure; any other unchecked exception is treated as a crash. Let
`InterruptedException` propagate — that is how the job timeout reaches the handler.
`tasks/DemoTasks` has four worked examples, including a deliberately flaky one that
exercises the retry and dead-letter paths.

---

## Observability

Four metrics carry the operational story; everything on the Grafana dashboard is derived
from them, and the provisioned Grafana dashboard queries exactly these series.

| Metric | Type | Labels |
|---|---|---|
| `jqueue_jobs_processed_total` | counter | `queue`, `task_type`, `status` |
| `jqueue_job_processing_duration_seconds` | histogram | `queue`, `task_type` |
| `jqueue_job_retries_total` | counter | `queue`, `task_type` |
| `jqueue_jobs_failed_total` | counter | `queue`, `task_type`, `reason` |

Plus `jqueue_queue_depth{queue,state}`, `jqueue_active_workers`, and the standard JVM and
process metrics.

Every label has bounded cardinality by construction. Queue names and task types come from a
registry, states from a closed enum, and failure reasons from the error-code constants — a
raw error message never becomes a label value.

Latency buckets span 5 ms to 60 s rather than using the client defaults, which would
collapse every slow job into `+Inf`. `PrometheusMetricsRecorderTest` pins the exported
names and buckets against the scrape output, because a rename is a silent outage of every
dashboard panel.

Logs are one JSON object per line (Spring Boot structured logging, Logstash format). The
logging context — `request_id`, `job_id`, `queue`, `task_type`, `worker_id` — is attached
through `LogContext` (SLF4J MDC), so every line written while handling a job carries it.

---

## Configuration

`config/config.local.yaml` documents every setting under a `jqueue:` root; environment
variables prefixed `JQUEUE_` override the file. Secrets only ever come from the
environment. Point `JQUEUE_CONFIG_PATH` at a different file to replace it.

```bash
JQUEUE_REDIS_ADDRS=redis-1:6379,redis-2:6379
JQUEUE_REDIS_PASSWORD=...
JQUEUE_WORKER_ENABLED=false     # API-only pod
JQUEUE_SCHEDULER_ENABLED=false  # worker-only pod
JQUEUE_WORKER_CONCURRENCY=50
JQUEUE_PUBLIC_PORT=8080         # also JQUEUE_PRIVATE_PORT, JQUEUE_LOG_LEVEL
```

One jar serves every role. A cluster can run API-only pods behind a load balancer and
worker-only pods on a different node pool, from the same image. Exactly one deployment
needs to run the scheduler.

Configuration is validated at startup, so a bad value fails at boot rather than at 3am
when the first job of that shape arrives.

---

## Testing

```bash
make test              # 119 unit tests, no dependencies
make test-integration  # 21 tests against a live Redis (REDIS_ADDR overrides 127.0.0.1:6379)
make cover             # unit tests + JaCoCo report in target/site/jacoco
```

The unit suite needs nothing installed — that is the point of the fakes. What genuinely
cannot be faked (Lua atomicity, score ordering, TTL behaviour, the full Spring wiring) is
tagged `integration` and skips itself when no Redis is reachable. CI runs both against a
Redis service container.

Worth reading as documentation of intent:

- `RedisBrokerIT.concurrentDequeueNeverDuplicatesAJob` — 20 threads, 100 jobs, one claim each
- `RedisBrokerIT.idempotencyKeyAdmitsExactlyOneClaim` — 50 threads, one winner, 49 told who won
- `RedisBrokerIT.expiredLeaseIsReclaimed` — a worker dies mid-job and the job comes back
- `WorkerPoolTest.stopWaitsForInFlightJobs` — shutdown blocks until running handlers finish
- `DefaultJobProcessorServiceTest.containsAPanickingHandler` — a crashing handler becomes an ordinary failure
- `JQueueApplicationIT` — the whole service booted and driven over HTTP

---

## From Go to Java

| Concern | GoQueue (Go) | JQueue (Java) |
|---|---|---|
| Composition root | Google Wire (`pkg/di`) | Spring `@Configuration` with explicit constructors (`di/`) |
| HTTP | Gin, two engines | Spring MVC on Tomcat, two connectors + `PublicPortFilter` |
| Redis client | go-redis `UniversalClient` | Jedis 7 `UnifiedJedis` (`RedisClient` / `RedisClusterClient`) |
| Lua scripts | `go:embed` | classpath resources, `EVALSHA` with `EVAL` fallback |
| Errors | `*AppError` return values | `AppError extends RuntimeException`, same codes and kinds |
| Concurrency | goroutines + `context` | platform worker threads + a stop latch; handlers on virtual threads |
| Job timeout | `context.WithTimeout` | `Future.get(timeout)` + `cancel(true)` (interrupt) |
| Panic barrier | `recover()` | unchecked-exception classification → `handler_panic` |
| Shutdown order | explicit slice in `main.go` | `SmartLifecycle` phases |
| Metrics | `client_golang` | Micrometer Prometheus registry, same series names |
| Logging | `log/slog` JSON + context fields | SLF4J + Logback structured JSON + MDC |
| Time | `clock.Clock` interface | `java.time.Clock` (`MutableClock` in tests) |
| Tests | testify, `integration` build tag | JUnit 5 + AssertJ + Awaitility, `@Tag("integration")` + Maven profile |

---

## Design decisions, and what they cost

**Polling, not blocking pops.** The pending set is a sorted set, so priority ordering is
possible — but `ZPOPMIN` cannot block, so workers poll on `poll_interval` (500 ms by
default). The cost is up to one interval of pickup latency on an idle queue; the gain is
priority ordering, which a blocking `BLPOP` on a list cannot give.

**Bulk sweeps bypass the aggregate.** `promote.lua` and `reclaim.lua` perform, in one
script, the transition that `Job.promote()` and `Job.reclaimExpiredLease()` express per
job. Running the domain method per job would cost two round trips each. The aggregate
methods remain the specification and are unit-tested; the scripts are their batch form.

**Task type is not indexed.** Filtering a listing by task type happens in memory over a
bounded page. A dedicated index would be the answer if it became a hot query.

**Base64 payloads.** Costs a third more bytes, buys a job document that stays readable in
`redis-cli` during an incident.

**At-least-once, not exactly-once.** Exactly-once across a network and a handler with side
effects is not something a queue can offer. JQueue gives at-least-once delivery plus
idempotency keys at the enqueue boundary, and asks handlers to be idempotent at the
execution boundary.

---

## Known limitations

Found while porting and testing. Each one is inherited unchanged from the original GoQueue
— the port keeps the original's behaviour and data model on purpose — and each has a clear
fix.

1. **Long jobs can be run twice.** `dequeue.lua` scores the active set with
   `now + worker.lease_duration + 30s`, independent of the job's own timeout, and nothing
   calls `JobBroker.extendLease` (there is no heartbeat). A job whose handler runs longer
   than about 90 s by default is reclaimed while it is still running and handed to a
   second worker. *Fix:* lease for `max(lease_duration, timeout)` in the dequeue call, or
   run a heartbeat that calls `extendLease` while a handler is executing.
2. **The job listing can hide the newest jobs.** `RedisJobRepository.list` reads the first
   `offset + limit` members of each lifecycle set, and the pending set is ordered
   oldest-first, so once a set holds more than one page the newest jobs fall outside the
   window. `RedisBrokerIT.listWindowIsTakenFromTheOldestEndOfEachSet` pins the current
   behaviour. *Fix:* a per-queue sorted set scored by `created_at`.
3. **Redis Cluster is not really supported.** Several addresses produce a cluster client,
   but the Lua scripts touch many keys without a shared hash tag, so they fail with
   `CROSSSLOT` on a real cluster. *Fix:* hash-tag the queue in every key (`{q}`) so a
   queue's keys share a slot.
4. `server.write_timeout` is accepted for compatibility but not applied; Tomcat has no
   equivalent of Go's `http.Server.WriteTimeout`.

---

## Project layout

```
src/main/java/com/jqueue/
  JQueueApplication.java     entry point, env aliases, start/stop log lines
  server/                     the private connector and the filter chain
  api/handlers, api/middleware
  application/
    services/                 service interfaces — what each caller is allowed to do
    enqueuer/                 request → validated, deduplicated, queued job
    inspector/                the dashboard and operator actions
    processor/                one execution: resolve, run, record. Plus the registry
  domain/
    job/                      Job root, RetryPolicy, BackoffStrategy, value objects (vo/)
    queue/                    Queue root, QueueStats, Weight, weighted dequeue order
    persistence/              the ports
    metrics/                  the observability port
  infrastructure/
    redis/                    key layout, Lua runner, document mapper, repositories, broker
    metrics/                  the Micrometer recorder and registry
    worker/                   the pool: concurrency and graceful shutdown
    scheduler/                promote, reclaim, janitor, depth sampling
    config/                   JQueueProperties
    web/                      the bundled dashboard page
  contracts/, hydrator/, common/, di/, tasks/
src/main/resources/lua/       the seven Lua scripts
src/test/java/com/jqueue/    unit tests, testing/ fakes, integration/ live-Redis tests
config/config.local.yaml      every setting, documented
deployment/                   Dockerfile, compose, Prometheus, Grafana
scripts/                      seed.sh, load.sh
```

---

## License

MIT — see [LICENSE](LICENSE). Ported from GoQueue:
[anashasan312/goqueue](https://github.com/anashasan312/goqueue).
