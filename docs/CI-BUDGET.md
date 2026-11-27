# Project-wide Actions budget (from Slice 6d authorization)

User limit: **15–20 minutes across this project**, not per session. Use the conservative
**15-minute** remaining ceiling from this authorization forward. Earlier runs preceded
this limit; do not treat it as a fresh budget on later turns/slices.

| Reservation | Maximum runner minutes | Run / outcome |
|---|---:|---|
| Slice 6d bootstrap/verification | 5 | Run 45954320178 failed during formatting; 13s observed, full 5m charged to ledger |
| Slice 6d final check-only verification | 5 | Run 45954790986 failed at engine Spotless; 2m23s observed, full 5m charged to ledger |
| Slice 7a1 final verification | 5 | Run 45956999888 hit 5m timeout; 5m13s observed including cleanup |

Two runs maximum for 6d. A failed/cancelled/timed-out run still uses its full
reservation conservatively. Record actual observed duration in the PR, but do not
silently reclaim rounded/billed minutes. No exploratory CI pushes or manual reruns.
PR checks only (no duplicate main push check); each job is capped at five minutes.
This ledger is an operational policy, not an account-wide GitHub billing limiter:
other branches/manual runs also consume minutes and must be accounted for before use.

## Current handoff (Slice 7a11)

Three jobs since the user's authorization: 13s + 2m23s + 5m13s = **7m49s observed**,
not a billing statement. All **15 conservative reservation minutes are allocated**;
the final job's 13-second timeout cleanup overhead is noted explicitly. Do not infer
new minutes from a session reset or reclaim the reservation without user reconciliation.

PR #10 was explicitly merged after its affected transform/core/engine/CLI gates and
both examples passed, but full reactor verify timed out. PR #9's previously unverified
engine/CLI paths were exercised by that #10 run. No full reactor success is claimed.

Slice 7a2 launched zero Actions jobs and was explicitly merged as PR #11 with
Maven/JUnit/packaged CLI verification still unrun. No green build is implied.

**Slice 7a3 launches zero Actions jobs.** Its head uses `[skip ci]`, and the PR stays
draft with supplemental local checks only. No workflow-dispatch, rerun, additional
CI-triggering push or duplicate main build. Future CI requires explicit additional
allocation/reconciliation with the user. Prior exceptions permitting unverified
merges are scoped to those requests, not blanket future merge authorization.


PR #12 / Slice 7a3 was explicitly merged with supplemental checks only, not a green
Maven build. Slice 7a4 uses a `[skip ci]` draft PR with **zero new Actions jobs**.
Existing observed/reserved totals above are unchanged. Maven verification remains
pending; this slice is not authorized for automatic merge or further Actions.


PR #13 / Slice 7a4 was explicitly merged by user request. No new Actions ran and
no Maven/JUnit/packaged CLI success is implied. Slice 7a5 launches **zero Actions**,
uses `[skip ci]` and stays draft. All 15 conservative reservation minutes remain
allocated; 7m49s observed prior runtime is unchanged, not a billing statement.
Local supplemental compilation/operator/integration checks do not grant CI minutes
or automatic merge permission.


PR #14 / Slice 7a5 explicitly merged on request. Its override was not blanket merge
permission. Slice 7a6 launches zero Actions and uses a [skip ci] draft head. The
15-minute conservative reservation remains fully allocated; prior observed 7m49s
is unchanged, not billing evidence or reclaimable CI time. Maven gates stay pending.


## One-time verification authorization after Slice 7a6

The user explicitly authorized Actions "just for this once" to execute the previously
unrun tests. Reserve **one additional job, maximum 5 runner minutes**, for a single
PR-triggered verification of merged slices through 7a6. Conservative reservations
now total **20 minutes**; this is a scoped exception to the prior exhausted 15-minute
ledger, not recurring permission. Do not rerun or dispatch another job automatically.
Existing observed 7m49s remains historical; append this run's actual outcome separately.

Use one `./mvnw -B -ntp verify` reactor invocation (JUnit, packaged CLI Failsafe E2E,
Spotless, license and dependency convergence), replacing repeated overlapping builds
that previously exhausted the timeout. Existing PR-only triggers and five-minute cap
remain unchanged; no push/main/scheduled/manual trigger is enabled. Open a verification
PR without [skip ci] exactly once, then use [skip ci] for any follow-up record/fixes.
This is verification-only, not authorization to merge that PR or begin another slice.


### One-time run outcome — no rerun

- PR #16, tested head `dd9c816`, run **46031639568**:
  https://github.com/Sehaan-1/pinot3/actions/runs/46031639568
- Job ran 2026-11-27T16:12:43Z–16:16:38Z: **3m55s observed**, conclusion
  **failure**, not timeout. The single five-minute reservation is consumed; no
  reclaim/rerun. Historical plus this run = **11m44s observed**, not billing evidence;
  conservative reservations remain **20 minutes total**.
- Reactor reached CLI Failsafe. Reported CLI integration aggregate: 16 tests,
  zero assertion failures, one error, zero skipped. PipelineIT: 15 tests, one error.
  Error: `packagedPipelineExtractsJsonUsingInstalledProvider`, line198,
  Jackson JsonParseException: unexpected ':' (code58), expected space separating
  root-level values while parsing CLI stdout. Process exit code assertion had passed.
  Do not claim full reactor success. Earlier reactor stages did not block CLI verify;
  full individual suite counts are not available from annotations.
- Failure annotation obtained via Checks API. Raw log and report artifact download
  redirects are blocked by sandbox egress; exact stdout and complete reports have
  not been inspected. Default Logback console output is a possible cause (only a
  test logging config exists), but this is not yet a confirmed root cause.
- No automatic fixes/rerun/merge. Existing PR-only triggers unchanged; this follow-up
  record uses [skip ci]. More Actions require another explicit user authorization.

## Second explicit one-time authorization: fix and verification

The user authorized one further Actions run to resolve the JSON-path CLI failure.
Reserve one additional five-minute job; conservative reservations now total **25
minutes**, exceeding the original ceiling only under this new explicit scoped
permission. No automatic rerun or future budget is implied. The previous 11m44s
observed runtime remains historical, not billed time or reclaimable reservation.

Fix plan: ship CLI-owned Logback defaults with INFO root and explicit stderr target.
Pinned Logback1.5.6 source shows default DEBUG root and BasicConfigurator console
appender defaulting to stdout; Jayway2.9.0 CompiledPath logs DEBUG on evaluation.
No production logback.xml existed. That combination contaminates JSON stdout with a
clock timestamp, consistent with the previous unexpected-colon parsing error.
Do not filter log lines out in tests or suppress parse failures. Retain JSON E2E
assertions and add a packaged-JAR subprocess regression proving warnings go to stderr
while stdout remains exact JSON. One non-skip commit triggers the single run on PR16;
any post-run documentation/fixes use [skip ci]. Do not merge automatically.


### Successful fix verification and next-slice status

The second explicitly authorized run **46033295536** passed full reactor
`./mvnw -B -ntp verify` at `032f66f752840182add7b9b5874d6f4d571d4aa4`:
https://github.com/Sehaan-1/pinot3/actions/runs/46033295536
Job duration **3m1s**, including JUnit and packaged CLI Failsafe/logging E2E.
PR16 was explicitly merged; no new Actions were started for its merge.

Historical7m49s + first scoped run3m55s + second scoped run3m1s = **14m45s
observed**, not billing evidence or reclaimed time. Conservative reservations are
**25 minutes** under the two explicit one-time exceptions, fully allocated. Both
exceptions are spent; no future rerun/dispatch allowance exists.

Slice7a7 starts zero Actions, uses [skip ci] and a draft PR. Source-built supplemental
checks are not pinned Maven verification of new code. PR16's green result covers its
own tested head only. No automatic merge or another CI run authorized.


Slice7a7 /PR17 explicitly merged by request without Maven verification. Slice7a8
launches zero Actions and uses [skip ci]. Historical observed14m45s / conservative
reserved25m are unchanged and fully allocated under spent one-time authorizations.
No automatic rerun/dispatch/merge or fresh-session allowance. PR16's successful run
46033295536 at032f66f does not verify slices7a7/7a8.


Slice7a8 /PR18 merged explicitly by user request without new Maven verification.
Slice7a9 launches zero Actions, uses [skip ci], and stays draft. Reserved25m and
observed14m45s remain unchanged, not reclaimed time or fresh authorization. Prior
PR16 success covers its tested032f66f head only, not new slices7a7/7a8/7a9.


PR19 /Slice7a9 explicitly merged on request. Slice7a10 starts zero Actions and uses
[skip ci] draft. Reserved25m/observed14m45s stay unchanged under spent one-time grants;
not billing evidence, fresh allowance or reclaimable CI time. Prior032f66f green result
does not verify subsequent7a7–7a10 changes. No automatic rerun/dispatch/merge.


PR20 /Slice7a10 explicitly merged on request. Slice7a11 starts zero Actions and uses
[skip ci] draft. Reserved25m/observed14m45s unchanged; spent one-time grants confer no
further workflow-dispatch/rerun permission or fresh allowance. Prior032f66f successful
run doesn't verify subsequent7a7–7a11 changes. No automatic merge.


PR21 /7a11 explicitly merged. The user authorized the **full remaining Stage7 scope**,
not additional Actions or automatic merges. The Stage7 planning checkpoint launches
zero Actions and uses [skip ci]. Reserved25m/observed14m45s unchanged; both scoped
exceptions spent. External-service verification is blocked/unrun where Docker/services/
Maven access are unavailable; no mocked/source-built substitute is a real connector pass.


Stage7 planning PR22 merged explicitly. Slice7b1 starts zero Actions; [skip ci] head.
User explicitly authorized the next implementation PR's merge in "merge then next
then merge". This scopes an unverified-merge exception to7b1, not subsequent Stage7
PRs or CI. Reserved25m/observed14m45s unchanged, both one-time Actions grants spent.
New API/transform Maven/JUnit/packaged CLI gates remain unrun despite local evidence.


Slice7b2 starts zero Actions, [skip ci]. User's "next then merge" grants an explicit
unverified-merge exception for this one next PR only, not more CI or Stage7-wide merges.
Reserved25m/observed14m45s unchanged; grants spent. Local shaded-Guava/Jackson checks
are supplemental; new Maven/JUnit/packaged CLI/full reactor gates remain unrun.


Slice7b3: user requested next then merge and all remaining7b. This authorizes this
cohesive bounded TableMerge/multi-input PR merge despite outstanding verification,
not Actions or connector merges. Zero Actions, [skip ci]; reserved25m/observed14m45s
unchanged. 207 offline executable contracts,423 regressions and eleven real LocalRunner pipelines
passed using prior supplemental dependencies. JUnit/packaged CLI/affected Maven/full
reactor gates remain unrun;7b is implemented, not fully verified.


Slice7d1: latest "next then merge" authorizes one next PostgreSQL JDBC PR merge despite
disclosed verification gaps. Zero Actions, [skip ci]; reserved25m/observed14m45s unchanged.
195 proxy/config assertions, real HSQLDB JDBC exercise, resource-free discovery/examples,
630 regressions and11 actual prior pipelines passed. Offline session copy disables
production pool-open for injected JDBC tests; not Hikari/pgjdbc/PostgreSQL evidence.
Gated real PostgreSQL/packaged CLI tests added but UNRUN, as are affected Maven/JUnit/
convergence/full reactor. No connector row checked off; no future merge/CI grant.


Slice7d2: next-then-merge scopes an unverified-merge exception to this next MySQL dialect/
external-driver-policy PR only. Zero Actions, [skip ci], reserved25m/observed14m45s
unchanged; previous CI exceptions spent.102 proxy/dialect assertions,825 regressions,
two JDBC/HSQLDB exercises, no-IO discovery/examples and11 previous pipelines passed.
Offline session copy bypasses Hikari open; no actual MySQL/ConnectorJ/Hikari/Maven/JUnit/
installed CLI/artifact-exclusion/convergence/full reactor evidence. Test profile is
explicit/test-only; driver licensing and real service provision remain operator-owned.
No connector completion checkmark or future CI/merge authorization.


Slice7e0: next-then-merge permits this single streaming/checkpoint bridge PR merge with
disclosed gaps. Zero Actions, [skip ci], reserved25m/observed14m45s unchanged.310 local
protocol assertions,927 regressions andreal Fake/Console restore scenarios plus11 old example executions passed.
Production bridge/coordinator/record/repository subset compiled without substitutions;
local test repository only, not production replication/external service proof. New
Hazelcast/JUnit/affected Maven/store build/installed CLI/full reactor UNRUN. No remaining
CI grant inferred; Stage7 streaming/JDBC acceptance stays unchecked.

Slice7e1 Kafka: zero Actions; no further allowance granted. Local supplemental port tests
only; SDK/Maven/broker/packaged gates remain unrun. See KAFKA.md.
