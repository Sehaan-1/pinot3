# Project-wide Actions budget (from Slice 6d authorization)

User limit: **15–20 minutes across this project**, not per session. Use the conservative
**15-minute** remaining ceiling from this authorization forward. Earlier runs preceded
this limit; do not treat it as a fresh budget on later turns/slices.

| Reservation | Maximum runner minutes | Run / outcome |
|---|---:|---|
| Slice 6d bootstrap/verification | 5 | Reserved, not started |
| Slice 6d final check-only verification | 5 | Reserved, not started |
| Future project work | 5 | Unallocated; requires checking this ledger first |

Two runs maximum for 6d. A failed/cancelled/timed-out run still uses its full
reservation conservatively. Record actual observed duration in the PR, but do not
silently reclaim rounded/billed minutes. No exploratory CI pushes or manual reruns.
PR checks only (no duplicate main push check); each job is capped at five minutes.
This ledger is an operational policy, not an account-wide GitHub billing limiter:
other branches/manual runs also consume minutes and must be accounted for before use.
