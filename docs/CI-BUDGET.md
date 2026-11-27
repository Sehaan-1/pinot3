# Project-wide Actions budget (from Slice 6d authorization)

User limit: **15–20 minutes across this project**, not per session. Use the conservative
**15-minute** remaining ceiling from this authorization forward. Earlier runs preceded
this limit; do not treat it as a fresh budget on later turns/slices.

| Reservation | Maximum runner minutes | Run / outcome |
|---|---:|---|
| Slice 6d bootstrap/verification | 5 | Run 45954320178 failed during formatting; 13s observed, full 5m charged to ledger |
| Slice 6d final check-only verification | 5 | Run 45954790986 failed at engine Spotless; 2m23s observed, full 5m charged to ledger |
| Future project work | 5 | Unallocated; requires checking this ledger first |

Two runs maximum for 6d. A failed/cancelled/timed-out run still uses its full
reservation conservatively. Record actual observed duration in the PR, but do not
silently reclaim rounded/billed minutes. No exploratory CI pushes or manual reruns.
PR checks only (no duplicate main push check); each job is capped at five minutes.
This ledger is an operational policy, not an account-wide GitHub billing limiter:
other branches/manual runs also consume minutes and must be accounted for before use.

## Current handoff

Two runner jobs used: 13s + 2m23s = **2m36s observed**, not a billing statement.
Conservatively **10 minutes reserved/consumed, 5 minutes remaining** under the
15-minute ceiling. Stop: do not automatically spend that remainder on this PR.
The second run passed engine compilation/unit tests but engine `verify` stopped
on import-order formatting. Those imports are fixed in a `[skip ci]` commit;
that commit is intentionally **not verified** and the PR remains draft.
CLI verification, documented example and full reactor gates were skipped by the
failed engine step. A future explicitly budgeted verification is needed before
review/merge readiness. See PR #9 for run links and the verified head SHA.
