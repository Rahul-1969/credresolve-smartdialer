# SmartDialer Campaign Simulation Results

**Parameters**: Agents=50, Duration=1200s, Seed=7

## Comprehensive Benchmark Table

| Scenario | Variant | Mode | Utilization | Initiated | Connected | Answered | Abandoned | Abandon Rate | Mean Req/App | Fallbacks | Failures/Ambig | Max Unbound |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **A** | clean | progressive | **43.18%** | 1376 | 1354 | 1354 | 0 | 0.00% | 1.1 / 1.1 | 0 | 0 / 0 | 0 |
| **A** | clean | predictive | **51.68%** | 1837 | 1820 | 1820 | 0 | 0.00% | 84.1 / 1.5 | 0 | 0 / 0 | 67 |
| **A** | chaos | progressive | **46.22%** | 1204 | 1181 | 1181 | 0 | 0.00% | 1.0 / 1.0 | 0 | 0 / 9 | 0 |
| **A** | chaos | predictive | **54.48%** | 1618 | 1594 | 1596 | 2 | 0.13% | 50.5 / 1.3 | 129 | 0 / 8 | 64 |
| **B** | clean | progressive | **62.65%** | 977 | 963 | 963 | 0 | 0.00% | 0.8 / 0.8 | 0 | 0 / 0 | 0 |
| **B** | clean | predictive | **66.71%** | 1175 | 1167 | 1167 | 0 | 0.00% | 24.3 / 1.0 | 0 | 0 / 0 | 51 |
| **B** | chaos | progressive | **60.98%** | 921 | 910 | 910 | 0 | 0.00% | 0.8 / 0.8 | 0 | 0 / 8 | 0 |
| **B** | chaos | predictive | **65.43%** | 1110 | 1083 | 1084 | 1 | 0.09% | 22.9 / 0.9 | 0 | 0 / 6 | 50 |
| **C** | clean | progressive | **83.59%** | 475 | 470 | 470 | 0 | 0.00% | 0.4 / 0.4 | 0 | 0 / 0 | 0 |
| **C** | clean | predictive | **85.26%** | 474 | 461 | 468 | 7 | 1.50% | 12.5 / 0.4 | 579 | 0 / 0 | 50 |
| **C** | chaos | progressive | **81.16%** | 476 | 475 | 475 | 0 | 0.00% | 0.4 / 0.4 | 0 | 0 / 7 | 0 |
| **C** | chaos | predictive | **84.44%** | 471 | 464 | 465 | 1 | 0.22% | 12.3 / 0.4 | 0 | 0 / 6 | 50 |
| **D** | clean | progressive | **59.98%** | 1023 | 1014 | 1014 | 0 | 0.00% | 0.8 / 0.8 | 0 | 0 / 0 | 0 |
| **D** | clean | predictive | **65.21%** | 1193 | 1187 | 1188 | 1 | 0.08% | 57.7 / 1.0 | 0 | 0 / 0 | 60 |
| **D** | chaos | progressive | **56.55%** | 992 | 991 | 991 | 0 | 0.00% | 0.8 / 0.8 | 0 | 1 / 6 | 0 |
| **D** | chaos | predictive | **60.77%** | 1218 | 1203 | 1207 | 4 | 0.33% | 61.1 / 1.0 | 0 | 0 / 10 | 66 |

## How to Read This Table
This table benchmarks SmartDialer across varying contact center borrower behaviors and telecom channel conditions:

- **Scenario A (Low Answer Rate 20%, Long Talk 120s)**: Tests predictive pacing under low contact efficiency. Predictive mode significantly increases agent utilization by over-dialing within mathematical risk boundaries.
- **Scenario B (Standard Contact Center 50%, Talk 90s)**: Represents typical debt-collection operations.
- **Scenario C (High Answer Rate 70%, Long Talk 180s)**: High answer probability restricts predictive overdialing; Safety Controller tightly bounds dispatch to prevent abandonment spikes.
- **Scenario D (Dynamic Changing Conditions)**: Non-stationary regime where answer rates drop from 60% to 10% then jump to 70%, and talk time extends from 60s to 150s.
- **Clean vs. Chaos**: Clean runs have healthy primary provider. Chaos runs route to Provider B under active 60s timeout outage (t=200s to t=260s) with delayed event delivery, forcing breaker trips, failover, and post-outage reaper reconciliation.
- **Utilization vs Compliance**: In all scenarios, Predictive pacing achieves superior agent utilization over Progressive pacing while strictly maintaining abandonment below the statutory 3.0% limit.

## Scenario D Jump & Dynamic Regime Audit
In Scenario D at t=600s, the borrower answer rate jumps abruptly from 10% to 70%. When this occurs:

1. **Safety Controller Action**: The Clopper-Pearson binomial confidence interval and short-window rate trackers observe sudden answers.
   When answers surge, `N0 + approved` exceeds the risk threshold, prompting immediate `REDUCED` or `FALLBACK_PROGRESSIVE` verdicts.
2. **Abandonment Rate Stability**: Because unbound in-flight calls are capped by `max_overdial_ratio` and excess speculative calls are cancelled, the overall abandonment rate remained below 3.0% throughout the jump.
