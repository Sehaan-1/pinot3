# Slice 6d — Installed job submission, REST and cluster client

## Specification and dependency plan

PR title: `[Slice 6d] Installed job submission REST and cluster CLI`.
Modules: `seatunnel-engine` (`submission`, `rest`) and `seatunnel-cli` (HTTP client).
Engine -> API/common; CLI -> core plus Jackson in production (engine test-only), never API -> engine. CLI uses HTTP
and does not embed Hazelcast/Jetty. Existing local execution remains unchanged.

Adopt pinned `org.eclipse.jetty:jetty-server:11.0.20` (Apache-2.0/EPL-2.0) for HTTP,
existing Jackson BOM 2.17.1 (`jackson-databind`, Apache-2.0) for bounded strict JSON,
JDK 17 HttpClient for CLI HTTP and existing Picocli 4.7.6 for subcommands. Reuse
Hazelcast 5.5.0 dispatch and existing task futures, not a new RPC or scheduler. Wrap the JDK response subscriber
only to bound retained bytes; a timed/cancellable JDK future bounds total client wait.
No servlet framework, reflection-based job loader or homegrown JSON parser.

## Contracts

- Embedders install named JobDefinition factories on the submission server. A factory
  validates string parameters and returns a physical plan plus trusted EngineTask
  bindings (classes already installed on all workers). HTTP accepts definition IDs
  and parameters, never Java serialization, uploaded classes, arbitrary class names
  or connector job HOCON. This is NOT automatically wired distributed ETL.
- JobSubmissionService owns a bounded map of at most 128 JobExecution handles. Capacity includes
  terminal jobs until explicitly deleted; no unbounded history. Status polling observes
  failures and triggers the existing sibling cancellation. No hidden background polling,
  automatic timeout, retry, persistence or submission-server failover. POST is not
  idempotent: clients must not automatically retry ambiguous submission failures.
- GET /v1/jobs returns the bounded job list for inspecting ambiguous submissions.
  POST /v1/jobs with {definition, parameters} returns 202 {jobId,status}; GET
  /v1/jobs/{uuid} returns status; POST /v1/jobs/{uuid}/cancel cooperatively cancels;
  DELETE /v1/jobs/{uuid} removes terminal records, rejects running jobs. Unknown jobs
  return 404; malformed requests 400; unsupported media 415; oversized bodies 413;
  wrong methods 405 with Allow; non-leader/admission/running-delete conflicts 409;
  unexpected internal failures 500 with no stack traces in JSON.
- JSON rejects unknown properties, duplicate keys and trailing tokens. Body size is
  bounded to 64 KiB before parsing; parameter count/value lengths and identifier
  lengths are bounded. Job IDs are server-generated UUIDs. Responses never expose
  task results, exceptions, installed classes or request data.
- REST startup takes an explicit bind address/port (port zero for tests). Jetty has
  bounded threads, header size and idle timeout; no request logging of body data.
  Server owns HTTP resources only; caller owns service and EngineNode, closing in
  HTTP -> service -> node order. Service shutdown cancels owned jobs, never calls
  cluster-wide shutdown. No unauthenticated public deployment: private trusted
  networks/loopback only, no auth, TLS, CORS or multi-tenant claims.
- CLI: `cluster submit --endpoint URL --definition ID [--parameters JSON_FILE]`,
  `cluster list --endpoint URL`, `cluster status --endpoint URL --job UUID`, `cluster cancel ...`, `cluster delete ...`.
  Strict bounded parameter-file parsing; finite connect/request timeouts; no retry;
  stdout is server JSON, stderr diagnostics. HTTP errors/non-JSON responses return
  nonzero; local CLI behavior and its exit codes remain unchanged.

## Verification and Actions budget

Remaining Actions allowance supplied by user: 15–20 minutes for the whole project.
Use the conservative 15-minute ceiling from this instruction forward. Reserve at
most two five-minute runs for this slice, leaving at least five minutes for future
work; do not start another run without checking the ledger. Remove duplicate main
push CI (PR commit is checked already); retain PR checks, concurrency cancellation,
and reduce job timeout to five minutes. No exploratory pushes. A failed/timed-out
run consumes its reservation; no unbounded reruns. Formatting/diagnostic bootstrap,
if needed due to sandbox tool restrictions, must be removed in final check-only run.
Record actual run durations in `docs/CI-BUDGET.md`; billing rounds differently so
reservations, not just observed duration, govern the conservative remaining budget.

Tests: public service bounds/validation; real TCP two-member cluster + real HTTP
submission/status/cancel/delete/error tests; real CLI subprocess against the HTTP
server; preserve existing local example and E2E. Gates: engine test/verify, CLI verify,
full reactor verify with Spotless/license/JaCoCo. Never claim unrun gates passed.

## Out of scope

Generic job-config-to-remote-operator translation, worker plugin installation, data
channels, checkpoint integration, REST checkpoint controls, durable job history,
leader redirects, auth/TLS, automatic recovery, exactly-once and periodic supervision.

## Three-line handoff

Next: review remaining engine integration gaps before starting transforms (Slice 7).
Installed task submission is not distributed connector ETL or durable job submission.
Start only after review/merge, honoring the remaining project-wide Actions budget.
