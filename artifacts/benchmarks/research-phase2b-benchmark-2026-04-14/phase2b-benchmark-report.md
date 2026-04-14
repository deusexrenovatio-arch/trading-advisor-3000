# Phase2B Benchmark Report

## Dataset
- scenario: benchmark_small
- instruments: 6
- bars_per_instrument: 96
- materialized_bar_rows: 576

## Timings
- cold_bootstrap_seconds: 0.379444
- cold_backtest_seconds: 3.75098
- hot_backtest_seconds: 0.73396
- hot_speedup_vs_cold_total: 5.627587

## Thresholds
- no_recompute_indicators_features: True
- hot_path_threshold_pass: True
- param_100_completed: True

## Scalability
| combinations | duration_seconds | cache_hit | run_count |
| --- | --- | --- | --- |
| 10 | 0.816186 | True | 60 |
| 50 | 3.424359 | True | 300 |
| 100 | 6.968866 | True | 600 |
| 250 | 17.282333 | True | 1500 |

## Cache Markers
- cold_backtest.cache_hit=false
- hot_backtest.cache_hit=true
- scale.10.cache_hit=true
- scale.50.cache_hit=true
- scale.100.cache_hit=true
- scale.250.cache_hit=true
