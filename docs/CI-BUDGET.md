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

## Current handoff (Slice 7a4)

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
