# SOAK pilot — 2026-10-04T09:51:21.9629358+05:30

| n | conc | result |
|---|------|--------|
| | | RESULT n=100 conc=1 ok=100 p50=8.07 p95=15.99 p99=20.40 tps=112 |
| | | RESULT n=100 conc=10 ok=100 p50=19.33 p95=50.34 p99=70.04 tps=411 |
| | | RESULT n=100 conc=50 ok=100 p50=24.52 p95=35.98 p99=52.08 tps=1573 |
| | | RESULT n=500 conc=1 ok=500 p50=8.14 p95=21.40 p99=27.93 tps=101 |
| | | RESULT n=500 conc=10 ok=500 p50=18.88 p95=35.41 p99=50.03 tps=486 |
| | | RESULT n=500 conc=50 ok=500 p50=23.31 p95=47.99 p99=61.92 tps=1785 |
| | | RESULT n=2000 conc=1 ok=2000 p50=8.25 p95=53.12 p99=100.73 tps=60 |
| | | RESULT n=2000 conc=10 ok=2000 p50=22.03 p95=253.00 p99=952.25 tps=82 |
| | | RESULT n=2000 conc=50 ok=2000 p50=471.60 p95=2235.06 p99=2596.24 tps=63 |

cargo test: green
test-bank (default + terminal): PASS
mid-run kill-9: survived, zero acked loss

