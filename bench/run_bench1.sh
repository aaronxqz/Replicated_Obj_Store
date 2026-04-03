#!/usr/bin/env bash
# bench/run_bench1.sh — Benchmark 1 launcher: Throughput vs. Concurrency
#
# Runs bench1_throughput.py at concurrency levels 1,2,4,8,16,32 for
# both put and get workloads, collects per-process results, and prints
# a summary table.
#
# Usage:
#   bash bench/run_bench1.sh --cluster localhost:50051
#   bash bench/run_bench1.sh --cluster ilab1.cs.rutgers.edu:50051,ilab2.cs.rutgers.edu:50051,ilab3.cs.rutgers.edu:50051

set -euo pipefail

CLUSTER=""
DURATION=30
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --cluster) CLUSTER="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [[ -z "$CLUSTER" ]]; then
    echo "Usage: bash run_bench1.sh --cluster HOST:PORT[,HOST:PORT,...]"
    exit 1
fi

CONCURRENCY_LEVELS=(1 2 4 8 16 32)
OPS=("put" "get")

echo "============================================================"
echo " Benchmark 1: Throughput vs. Concurrency"
echo " Cluster  : $CLUSTER"
echo " Duration : ${DURATION}s per run"
echo " Object   : 4 KiB"
echo "============================================================"
echo ""

for OP in "${OPS[@]}"; do
    echo "--- $OP workload ---"
    printf "%-12s %12s %10s %10s %10s\n" "concurrency" "total_ops/s" "p50_ms" "p95_ms" "p99_ms"
    printf "%-12s %12s %10s %10s %10s\n" "-----------" "-----------" "------" "------" "------"

    for N in "${CONCURRENCY_LEVELS[@]}"; do
        OUTFILE="$RESULTS_DIR/bench1_${OP}_n${N}.jsonl"
        > "$OUTFILE"   # truncate

        # Launch N worker processes in parallel
        PIDS=()
        for ((i=0; i<N; i++)); do
            python3 "$SCRIPT_DIR/bench1_throughput.py" \
                --cluster "$CLUSTER" \
                --op "$OP" \
                --duration "$DURATION" \
                --output "$OUTFILE" \
                > /dev/null 2>&1 &
            PIDS+=($!)
        done

        # Wait for all workers
        for PID in "${PIDS[@]}"; do
            wait "$PID" || true
        done

        # Aggregate results with Python
        python3 - "$OUTFILE" <<'EOF'
import json, sys, statistics

lines = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
if not lines:
    print(f"{'?':>12} {'?':>10} {'?':>10} {'?':>10}")
    sys.exit()

total_ops_s = sum(l["ops_per_s"] for l in lines)
p50 = statistics.median(l["p50_ms"] for l in lines)
p95 = statistics.median(l["p95_ms"] for l in lines)
p99 = statistics.median(l["p99_ms"] for l in lines)
print(f"{total_ops_s:>12.1f} {p50:>10.2f} {p95:>10.2f} {p99:>10.2f}")
EOF
    done | nl -ba -nrz -w2 | awk -v levels="1 2 4 8 16 32" '
    BEGIN { split(levels, a, " ") }
    { printf "%-12s%s\n", a[NR], $0 }'
    # Note: the nl/awk formatting above prefixes each row with the
    # concurrency level from the array.

    echo ""
done

echo "Raw results saved to: $RESULTS_DIR"