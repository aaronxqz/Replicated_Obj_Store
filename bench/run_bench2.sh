#!/usr/bin/env bash
# bench/run_bench2.sh — Benchmark 2 launcher: Replication Cost
#
# Runs bench2_replication.py with 8 concurrent clients under 3 configurations.
# You must start the server(s) manually before running this script.
#
# Usage:
#   # 1-node run:
#   python3 server.py --listen localhost:50051 --cluster localhost:50051 &
#   bash bench/run_bench2.sh --config 1node --cluster localhost:50051
#
#   # 2-node run:
#   python3 server.py --listen localhost:50051 --cluster localhost:50051,localhost:50052 &
#   python3 server.py --listen localhost:50052 --cluster localhost:50051,localhost:50052 &
#   bash bench/run_bench2.sh --config 2node --cluster localhost:50051,localhost:50052
#
#   # 3-node run:
#   python3 server.py --listen localhost:50051 --cluster localhost:50051,localhost:50052,localhost:50053 &
#   python3 server.py --listen localhost:50052 --cluster localhost:50051,localhost:50052,localhost:50053 &
#   python3 server.py --listen localhost:50053 --cluster localhost:50051,localhost:50052,localhost:50053 &
#   bash bench/run_bench2.sh --config 3node --cluster localhost:50051,localhost:50052,localhost:50053

set -euo pipefail

CLUSTER=""
CONFIG=""
DURATION=30
CLIENTS=8
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
mkdir -p "$RESULTS_DIR"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cluster)  CLUSTER="$2";  shift 2 ;;
        --config)   CONFIG="$2";   shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --clients)  CLIENTS="$2";  shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

if [[ -z "$CLUSTER" || -z "$CONFIG" ]]; then
    echo "Usage: bash run_bench2.sh --config 1node|2node|3node --cluster HOST:PORT[,...]"
    exit 1
fi

OUTFILE="$RESULTS_DIR/bench2_${CONFIG}.jsonl"
> "$OUTFILE"

echo "============================================================"
echo " Benchmark 2: Replication Cost"
echo " Config   : $CONFIG"
echo " Cluster  : $CLUSTER"
echo " Clients  : $CLIENTS"
echo " Duration : ${DURATION}s"
echo " Object   : 4 KiB"
echo "============================================================"

# Launch CLIENTS worker processes in parallel
PIDS=()
for ((i=0; i<CLIENTS; i++)); do
    python3 "$SCRIPT_DIR/bench2_replication.py" \
        --cluster "$CLUSTER" \
        --duration "$DURATION" \
        --output "$OUTFILE" \
        > /dev/null 2>&1 &
    PIDS+=($!)
done

for PID in "${PIDS[@]}"; do
    wait "$PID" || true
done

echo ""
echo "Results for $CONFIG:"
printf "%-10s %12s %10s %10s %10s\n" "config" "total_ops/s" "p50_ms" "p95_ms" "p99_ms"
python3 - "$OUTFILE" "$CONFIG" <<'EOF'
import json, sys, statistics

lines = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
cfg = sys.argv[2]
total_ops_s = sum(l["ops_per_s"] for l in lines)
p50 = statistics.median(l["p50_ms"] for l in lines)
p95 = statistics.median(l["p95_ms"] for l in lines)
p99 = statistics.median(l["p99_ms"] for l in lines)
print(f"{cfg:<10} {total_ops_s:>12.1f} {p50:>10.2f} {p95:>10.2f} {p99:>10.2f}")
EOF

echo ""
echo "Raw results: $OUTFILE"