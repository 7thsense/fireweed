# Snorri ladder candidate handoff

**Only the external snorri pipeline may populate RESULT** in
`snorri-ladder-candidate.json`. Local probes are not admissible for parent AC2.

## Candidate

Pinned at release freeze to the tip that passes product-cell gates
(see `log-projection-perf-latest.md`). Update `candidate_rev` in the JSON
sibling to that SHA before asking snorri to measure.

## Baseline (v0.31.2)

| w | tps |
|--:|----:|
| 1 | 2,069 |
| 4 | 3,395 |
| 8 | **3,692** (pass bar) |

In-commit ~0.93 ms/entry; durable_queue_commit ~37.2 s.

## Regressed landing (ed311dff)

| w | tps |
|--:|----:|
| 1 | 988 |
| 4 | 2,247 |
| 8 | 1,540 |

## Run config

- 10k members, claim-batch 500, workers 1/4/8  
- path-override at candidate rev  
- delivery assertions expected green  

## Pass

`tps_w8 >= 3692` and delivery assertions green; `measured_rev` equals `candidate_rev`.
