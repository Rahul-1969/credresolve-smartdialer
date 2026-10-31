# SmartDialer Performance and Load Test Report

**Execution Mode**: Full
**Python Version**: 3.11.9
**Platform**: Windows-10-10.0.26300-SP0 (AMD64)
**Logical CPU Cores**: 16
**Command**: `python -m smartdialer.cli loadtest `

## 1. Dispatch Throughput (`dispatch.start_progressive_call`)
| Agent Pool | Threads | Ops / Sec | Total Reserved | Elapsed (s) |
|---|---|---|---|---|
| 100 | 1 | **725.5** | 100 | 0.138 |
| 100 | 4 | **617.5** | 100 | 0.162 |
| 100 | 8 | **530.0** | 96 | 0.181 |
| 100 | 16 | **174.9** | 96 | 0.549 |
| 1,000 | 1 | **1,233.9** | 1,000 | 0.810 |
| 1,000 | 4 | **1,577.2** | 1,000 | 0.634 |
| 1,000 | 8 | **1,220.2** | 1,000 | 0.820 |
| 1,000 | 16 | **581.6** | 992 | 1.706 |
| 10,000 | 1 | **242.9** | 10,000 | 41.175 |
| 10,000 | 4 | **97.4** | 4,011 | 41.188 |
| 10,000 | 8 | **89.1** | 1,916 | 21.506 |
| 10,000 | 16 | **72.8** | 625 | 8.581 |

## 2. Event Processing Throughput (`apply_event`)
| Threads | Events Processed | Ops / Sec | Elapsed (s) |
|---|---|---|---|
| 1 | 2,000 | **1,786.7** | 1.119 |
| 4 | 2,000 | **652.3** | 3.066 |
| 8 | 2,000 | **766.2** | 2.610 |

## 3. Safety Evaluation Latency (`SafetyController.evaluate`)
| Agents | Historical Calls | p50 Latency (ms) | p95 Latency (ms) | Max Latency (ms) |
|---|---|---|---|---|
| 100 | 5,000 | 15.611 ms | 18.872 ms | 20.495 ms |
| 1,000 | 5,000 | 11.834 ms | 17.174 ms | 19.107 ms |
| 10,000 | 5,000 | 118.611 ms | 144.323 ms | 149.761 ms |

## 4. Telemetry Snapshot Latency (`read_snapshot`)
| Agents | Calls Table Rows | p50 Latency (ms) | p95 Latency (ms) | Max Latency (ms) |
|---|---|---|---|---|
| 100 | 10,000 | 32.670 ms | 36.170 ms | 47.193 ms |
| 100 | 100,000 | 472.084 ms | 501.202 ms | 506.414 ms |
| 1,000 | 10,000 | 29.417 ms | 36.638 ms | 36.724 ms |
| 1,000 | 100,000 | 445.545 ms | 500.318 ms | 585.676 ms |
| 10,000 | 10,000 | 29.299 ms | 33.734 ms | 34.227 ms |
| 10,000 | 100,000 | 448.629 ms | 559.600 ms | 564.899 ms |

### SQLite EXPLAIN QUERY PLAN (read_snapshot queries)

**Agent States**:
```text
  id=6, parent=0, detail=SCAN agents USING COVERING INDEX idx_agents_state_available
```

**Call Tallies**:
```text
  id=3, parent=0, detail=SCAN calls USING COVERING INDEX idx_calls_state_agent
```

**Pending Allotments**:
```text
  id=6, parent=0, detail=SCAN safety_allotments
  id=11, parent=0, detail=USE TEMP B-TREE FOR GROUP BY
```

**RateStats Window**:
```text
  id=4, parent=0, detail=SEARCH calls USING INDEX idx_calls_created_at (created_at>?)
```

**AbandonStats Window**:
```text
  id=3, parent=0, detail=SCAN calls
```

**Avg Talk Time**:
```text
  id=4, parent=0, detail=SEARCH calls USING INDEX idx_calls_state_agent (state=?)
```

## 5. Mathematical Risk Calculation Latency
| Agent Capacity (cap) | `binom_tail_gt` Latency (ms) | `max_total_calls` Latency (ms) |
|---|---|---|
| 100 | 0.0174 ms | 0.4800 ms |
| 1,000 | 0.0186 ms | 7.7755 ms |
| 10,000 | 0.0175 ms | 124.6338 ms |

## 6. Concurrent Safety Lock Contention
- **Threads**: 8 concurrent workers
- **Agents**: 1,000
- **SAFETY_ERROR Count**: **0** (Zero error invariant satisfied)
- **p50 Lock Wait**: 59.033 ms
- **p95 Lock Wait**: 142.588 ms
- **Max Lock Wait**: 142.588 ms
