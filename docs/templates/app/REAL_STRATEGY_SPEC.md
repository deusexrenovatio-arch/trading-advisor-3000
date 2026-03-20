# REAL_STRATEGY_SPEC

## 1. Metadata
- strategy_id:
- strategy_version:
- owner:
- status: `draft | shadow | advisory | live_canary`
- pilot_universe:

## 2. Market and integration scope
- historical source: `MOEX`
- primary live feed: `QUIK`
- publication surface: `Telegram`
- runtime state store: `PostgreSQL`
- optional execution route for 9B: `HTTP gateway -> StockSharp -> QUIK -> Finam`

## 3. Hypothesis
What market behavior this strategy is expected to capture.

## 4. Contract and session assumptions
- instruments:
- contract selection rule:
- session rule:
- roll blackout rule:
- live-feed freshness assumption:

## 5. Timeframes
- primary TF:
- context TF:
- decision timing rule:

## 6. Feature inputs
- required canonical inputs:
- required features:
- historical-data assumptions:
- live-data assumptions:

## 7. Entry rules
Describe rule-based entry conditions.

## 8. Exit rules
- stop model:
- target model:
- cancel / invalidation:
- time-based exit:

## 9. Risk template
- sizing model:
- max_parallel_signals:
- exposure caps:
- cooldown / de-dup:

## 10. External systems and probes

| System | Used for | Version / route | Required secrets | Probe / smoke check |
| --- | --- | --- | --- | --- |
| `MOEX` | historical source |  |  |  |
| `QUIK` | live feed |  |  |  |
| `Telegram` | publication |  |  |  |
| `PostgreSQL` | runtime state |  |  |  |
| `Prometheus / Loki / Grafana` | evidence |  |  |  |
| `HTTP gateway -> StockSharp -> QUIK -> Finam` | optional 9B route |  |  |  |

## 11. Expected operating band
- target signal frequency:
- acceptable dry periods:
- unacceptable overtrading threshold:

## 12. Acceptance
What must be true before the strategy is allowed into shadow/advisory pilot.

## 13. Rejection criteria
What automatically rejects the strategy.

## 14. Evidence links
- backtest artifact:
- replay artifact:
- pilot artifact:
