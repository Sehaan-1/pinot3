# Project-wide Actions budget (from Slice 6d authorization)

User limit: **15–20 minutes across this project**, not per session. Use the conservative
**15-minute** remaining ceiling from this authorization forward. Earlier runs preceded
this limit; do not treat it as a fresh budget on later turns/slices.

| Reservation | Maximum runner minutes | Run / outcome |
|---|---:|---|
| Slice 6d bootstrap/verification | 5 | Run 45954320178 failed during formatting; 13s observed, full 5m charged to ledger |
| Slice 6d final check-only verification | 5 | Run 45954790986 failed at engine Spotless; 2m23s observed, full 5m charged to ledger |
| Slice 7a1 final verification | 5 | Reserved for one run after local checks; no reruns |

Two runs maximum for 6d. A failed/cancelled/timed-out run still uses its full
reservation conservatively. Record actual observed duration in the PR, but do not
silently reclaim rounded/billed minutes. No exploratory CI pushes or manual reruns.
PR checks only (no duplicate main push check); each job is capped at five minutes.
This ledger is an operational policy, not an account-wide GitHub billing limiter:
other branches/manual runs also consume minutes and must be accounted for before use.

## Current handoff (Slice 7a1)

Slice 6d used two runner jobs: 13s + 2m23s = **2m36s observed**, not a billing
statement. Its final import-only fix was merged as PR #9 by explicit user request
without further checking. Engine compilation/unit tests passed on the previous
head; CLI verification and full reactor verification were not claimed passing.

Conservatively **10 minutes consumed + 5 minutes reserved for Slice 7a1 = all 15
minutes allocated**. The next PR may launch exactly one five-minute job, after
local checks. That job also exercises the previously unverified 6d code. Stop after
it succeeds/fails/times out: no reruns, no additional CI-triggering pushes, and no
fresh budget on later sessions. Record outcome in the PR (avoid a documentation-only
commit triggering a new run). Future Actions work requires additional user budget.
A failed run leaves a draft PR, not permission to bypass verification.
