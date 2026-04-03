#!/usr/bin/env python3
"""
bench/bench2_replication.py — Benchmark 2: Replication Cost

Spec: Run a sustained put workload at 8 concurrent clients with 4 KiB objects
for 30 seconds under three configurations:
  1. Single standalone server (no replication)
  2. 2-node cluster (primary + 1 replica, majority = 2)
  3. 3-node cluster (primary + 2 replicas, majority = 2)

Report throughput and p99 latency for each configuration.

This is the per-process worker. Run it via run_bench2.sh.

Usage (per-process worker):
    python3 bench/bench2_replication.py \
        --cluster HOST:PORT[,...] --duration 30 --output results.jsonl
"""

import argparse
import json
import os
import sys
import time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc


VALUE_SIZE = 4 * 1024   # 4 KiB


def make_stub(endpoint):
    channel = grpc.insecure_channel(
        endpoint,
        options=[
            ("grpc.keepalive_time_ms", 10000),
            ("grpc.keepalive_timeout_ms", 5000),
        ],
    )
    return pb_grpc.ObjectStoreStub(channel)


def percentile(data, p):
    if not data:
        return 0.0
    sd = sorted(data)
    return sd[min(int(len(sd) * p / 100), len(sd) - 1)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster",  required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--output",   required=True)
    args = parser.parse_args()

    endpoints  = sorted(e.lower().strip() for e in args.cluster.split(","))
    primary    = endpoints[0]
    num_nodes  = len(endpoints)
    stub       = make_stub(primary)
    value      = os.urandom(VALUE_SIZE)

    ops       = 0
    errors    = 0
    latencies = []
    counter   = 0
    deadline  = time.monotonic() + args.duration

    while time.monotonic() < deadline:
        key = f"bench2_{os.getpid()}_{counter}"
        counter += 1

        t0 = time.monotonic()
        try:
            stub.Put(pb.PutRequest(key=key, value=value))
            ops += 1
            latencies.append((time.monotonic() - t0) * 1000)
        except grpc.RpcError:
            errors += 1

    elapsed    = args.duration
    throughput = ops / elapsed if elapsed > 0 else 0

    result = {
        "pid":       os.getpid(),
        "num_nodes": num_nodes,
        "ops":       ops,
        "errors":    errors,
        "ops_per_s": round(throughput, 2),
        "p50_ms":    round(percentile(latencies, 50), 3),
        "p95_ms":    round(percentile(latencies, 95), 3),
        "p99_ms":    round(percentile(latencies, 99), 3),
    }

    with open(args.output, "a") as f:
        f.write(json.dumps(result) + "\n")

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()