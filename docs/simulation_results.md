# SmartDialer Campaign Simulation Results

**Parameters**: Agents=50, Duration=1200s, Seed=7

## Comprehensive Benchmark Table

| Scenario | Variant | Mode | Utilization | Implied Util | Created | Initiated | Answered | Connected | Cancelled | Canc % | Obs Ans Rate | Abandoned | Abandon Rate | Placed (A/B) | Failovers | Reaped | Aband Causes (Bind/Lost) | Aband Cred>0 | Mean Req/App | Fallbacks | Fail/Ambig | Max Unb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **A** | clean | progressive | **43.18%** | 43.34% | 1376 | 1376 | 244 | 244 | 0 | 0.00% | 18.02% | 0 | 0.00% | 1364 / 12 | 12 | 0 | 0 / 0 | N/A | 1.1 / 1.1 | 0 | 0 / 0 | 0 |
| **A** | clean | predictive | **51.68%** | 51.71% | 1837 | 1837 | 304 | 304 | 193 | 10.51% | 18.68% | 0 | 0.00% | 1823 / 14 | 14 | 0 | 0 / 0 | 0 | 84.1 / 1.5 | 0 | 0 / 0 | 67 |
| **A** | chaos | progressive | **46.22%** | 46.20% | 1205 | 1195 | 231 | 231 | 0 | 0.00% | 19.63% | 0 | 0.00% | 64 / 1131 | 64 | 9 | 0 / 0 | N/A | 1.0 / 1.0 | 0 | 0 / 9 | 0 |
| **A** | chaos | predictive | **54.48%** | 54.48% | 1618 | 1610 | 269 | 267 | 638 | 39.43% | 28.20% | 2 | 0.74% | 58 / 1552 | 58 | 10 | 2 / 0 | 1 | 50.5 / 1.3 | 129 | 0 / 8 | 64 |
| **B** | clean | progressive | **62.65%** | 62.80% | 977 | 977 | 458 | 458 | 0 | 0.00% | 47.56% | 0 | 0.00% | 974 / 3 | 3 | 0 | 0 / 0 | N/A | 0.8 / 0.8 | 0 | 0 / 0 | 0 |
| **B** | clean | predictive | **66.71%** | 66.92% | 1175 | 1175 | 482 | 482 | 155 | 13.19% | 47.63% | 0 | 0.00% | 1170 / 5 | 5 | 0 | 0 / 0 | 0 | 24.3 / 1.0 | 0 | 0 / 0 | 51 |
| **B** | chaos | progressive | **60.98%** | 61.18% | 921 | 913 | 435 | 435 | 0 | 0.00% | 48.01% | 0 | 0.00% | 31 / 882 | 31 | 15 | 0 / 0 | N/A | 0.8 / 0.8 | 0 | 0 / 8 | 0 |
| **B** | chaos | predictive | **65.43%** | 65.72% | 1110 | 1104 | 478 | 477 | 121 | 10.90% | 49.69% | 1 | 0.21% | 12 / 1092 | 12 | 8 | 1 / 0 | 0 | 22.9 / 0.9 | 0 | 0 / 6 | 50 |
| **C** | clean | progressive | **83.59%** | 83.80% | 475 | 475 | 322 | 322 | 0 | 0.00% | 68.51% | 0 | 0.00% | 474 / 1 | 1 | 0 | 0 / 0 | N/A | 0.4 / 0.4 | 0 | 0 / 0 | 0 |
| **C** | clean | predictive | **85.26%** | 85.45% | 474 | 474 | 321 | 314 | 4 | 0.84% | 69.18% | 7 | 2.18% | 473 / 1 | 1 | 0 | 7 / 0 | 7 | 12.5 / 0.4 | 579 | 0 / 0 | 50 |
| **C** | chaos | progressive | **81.16%** | 81.22% | 476 | 469 | 327 | 327 | 0 | 0.00% | 69.13% | 0 | 0.00% | 8 / 461 | 8 | 5 | 0 / 0 | N/A | 0.4 / 0.4 | 0 | 0 / 7 | 0 |
| **C** | chaos | predictive | **84.44%** | 84.57% | 471 | 465 | 325 | 324 | 0 | 0.00% | 70.04% | 1 | 0.31% | 20 / 445 | 20 | 3 | 1 / 0 | 1 | 12.3 / 0.4 | 0 | 0 / 6 | 50 |
| **D** | clean | progressive | **59.98%** | 60.12% | 1023 | 1023 | 411 | 411 | 0 | 0.00% | 40.53% | 0 | 0.00% | 1020 / 3 | 3 | 0 | 0 / 0 | N/A | 0.8 / 0.8 | 0 | 0 / 0 | 0 |
| **D** | clean | predictive | **65.21%** | 65.39% | 1193 | 1193 | 423 | 422 | 24 | 2.01% | 36.34% | 1 | 0.24% | 1187 / 6 | 6 | 0 | 1 / 0 | 1 | 57.7 / 1.0 | 0 | 0 / 0 | 60 |
| **D** | chaos | progressive | **56.55%** | 56.67% | 992 | 985 | 380 | 380 | 0 | 0.00% | 38.38% | 0 | 0.00% | 71 / 914 | 71 | 6 | 0 / 0 | N/A | 0.8 / 0.8 | 0 | 1 / 6 | 0 |
| **D** | chaos | predictive | **60.77%** | 60.93% | 1218 | 1208 | 415 | 411 | 31 | 2.55% | 35.35% | 4 | 0.96% | 24 / 1184 | 24 | 7 | 4 / 0 | 3 | 61.1 / 1.0 | 0 | 0 / 10 | 66 |

## How to Read This Table
This table benchmarks SmartDialer across varying borrower behaviors and telecom channel conditions:

- **Scenario A (Low Answer Rate 20%, Long Talk 120s)**: Tests predictive pacing under low contact efficiency. Predictive mode significantly increases agent utilization by over-dialing within mathematical risk boundaries.
- **Scenario B (Standard Contact Center 50%, Talk 90s)**: Represents typical debt-collection operations.
- **Scenario C (High Answer Rate 70%, Long Talk 180s)**: High answer probability restricts predictive overdialing; Safety Controller tightly bounds dispatch to prevent abandonment spikes.
- **Scenario D (Dynamic Changing Conditions)**: Non-stationary regime where answer rates drop from 60% to 10% then jump to 70%, and talk time extends from 60s to 150s.
- **Clean vs. Chaos**: Clean runs use primary Provider A. Chaos runs route to Provider B under an active 60s timeout outage (t=200s to t=260s) with delayed event delivery, forcing breaker trips, failover, and post-outage reaper reconciliation.
- **Utilization vs Compliance**: In all scenarios, Predictive pacing achieves superior agent utilization over Progressive pacing while strictly maintaining abandonment below the 3% target used in this prototype.

## Scenario D Jump & Dynamic Regime Audit (Measured Timeline t in [590s, 660s])

### Scenario D (Clean - Predictive)
- **Configured Regime Phases**: Phase 1 [0s-300s] target=60.0% (observed: 59.31%); Phase 2 [300s-600s] target=10.0% (observed: 7.67%); Phase 3 [600s-1200s] target=70.0% (observed: 69.06%).
- **Window t in [590s, 660s] Stats**: Answered=41, Abandoned=0, Window Abandon Rate=0.00%.
- **Safety Verdicts in Window**: REDUCED: 38, REJECTED: 33
- **Binding Rule Invocations in Window**: OVERDIAL_RATIO: 33, CAPACITY_EXCESS: 3, RISK_BOUND: 35
- **Wilson Upper Bound (p_hi) Range**: min=0.2772, max=0.8380.
- **Ticks in Fallback in Window**: 0 ticks.

### Scenario D (Chaos - Predictive)
- **Configured Regime Phases**: Phase 1 [0s-300s] target=60.0% (observed: 56.95%); Phase 2 [300s-600s] target=10.0% (observed: 7.53%); Phase 3 [600s-1200s] target=70.0% (observed: 71.75%).
- **Window t in [590s, 660s] Stats**: Answered=38, Abandoned=0, Window Abandon Rate=0.00%.
- **Safety Verdicts in Window**: REDUCED: 41, REJECTED: 30
- **Binding Rule Invocations in Window**: OVERDIAL_RATIO: 36, CAPACITY_EXCESS: 7, RISK_BOUND: 28
- **Wilson Upper Bound (p_hi) Range**: min=0.2581, max=0.8121.
- **Ticks in Fallback in Window**: 0 ticks.