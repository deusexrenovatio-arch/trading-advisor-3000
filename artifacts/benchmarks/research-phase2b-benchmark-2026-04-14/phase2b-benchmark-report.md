# Phase2B Benchmark Report

## Dataset
- scenario: benchmark_small
- instruments: 6
- bars_per_instrument: 96
- materialized_bar_rows: 576

## Timings
- cold_bootstrap_seconds: 0.541712
- cold_backtest_seconds: 4.823155
- hot_backtest_seconds: 0.939261
- hot_speedup_vs_cold_total: 5.711796

## Thresholds
- no_recompute_indicators_features: True
- hot_path_threshold_pass: True
- param_100_completed: True

## Scalability
| combinations | duration_seconds | cache_hit | run_count |
| --- | --- | --- | --- |
| 10 | 0.782157 | True | 60 |
| 50 | 3.765329 | True | 300 |
| 100 | 7.753719 | True | 600 |
| 250 | 20.664886 | True | 1500 |

## Cache Markers
- cold_backtest.cache_hit=false
- hot_backtest.cache_hit=true
- scale.10.cache_hit=true
- scale.50.cache_hit=true
- scale.100.cache_hit=true
- scale.250.cache_hit=true
