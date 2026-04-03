#!/usr/bin/env python3
"""
bench/bench1_throughput.py — Benchmark 1: Throughput vs. Concurrency

Spec: Fix object size at 4 KiB. Run a sustained put workload for 30 seconds
at each concurrency level: 1, 2, 4, 8, 16, 32 client processes.
Report operations per second. Repeat for get.

This script is the per-process worker. A separate launcher script
(bench/run_bench1.sh) starts N copies of it in parallel.

Usage (direct, for a single concurrency level):
    python3 bench/bench1_throughput.py --cluster localhost:50051 \
        --op put --duration 30 --output results.jsonl

Usage (via launcher):
    bash bench/run_bench1.sh --cluster localhost:50051
"""

import argparse
import json
import os
import sys
import time
import random
import string
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2


VALUE_SIZE = 4 * 1024       # 4 KiB
PRELOAD_KEYS = 1000         # number of keys to pre-load for get workload


def make_stub(endpoint):
    channel = grpc.insecure_channel(
        endpoint,
        options=[
            ("grpc.keepalive_time_ms", 10000),
            ("grpc.keepalive_timeout_ms", 5000),
        ],
    )
    return pb_grpc.ObjectStoreStub(channel)


def random_key(prefix="bench1_"):
    return f"{prefix}{random.randint(0, PRELOAD_KEYS - 1)}"


def random_value():
    return os.urandom(VALUE_SIZE)


def run_put(stub, duration_s):
    """Continuously put unique keys for `duration_s` seconds."""
    ops       = 0
    errors    = 0
    latencies = []
    deadline  = time.monotonic() + duration_s
    counter   = 0

    while time.monotonic() < deadline:
        key   = f"bench1_put_{os.getpid()}_{counter}"
        value = random_value()
        counter += 1

        t0 = time.monotonic()
        try:
            stub.Put(pb.PutRequest(key=key, value=value))
            ops += 1
            latencies.append((time.monotonic() - t0) * 1000)
        except grpc.RpcError:
            errors += 1

    return ops, errors, latencies


def run_get(stub, duration_s, cluster):
    """Continuously get pre-loaded keys for `duration_s` seconds."""
    # Use any node for reads (reads spread across cluster)
    endpoints = sorted(e.lower().strip() for e in cluster.split(","))
    read_stub = make_stub(endpoints[os.getpid() % len(endpoints)])

    ops       = 0
    errors    = 0
    latencies = []
    deadline  = time.monotonic() + duration_s

    while time.monotonic() < deadline:
        key = random_key()
        t0  = time.monotonic()
        try:
            read_stub.Get(pb.GetRequest(key=key))
            ops += 1
            latencies.append((time.monotonic() - t0) * 1000)
        except grpc.RpcError:
            errors += 1

    return ops, errors, latencies


def percentile(data, p):
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * p / 100)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster",  required=True)
    parser.add_argument("--op",       choices=["put", "get"], required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--output",   required=True,
                        help="JSONL file to append results to")
    args = parser.parse_args()

    endpoints = sorted(e.lower().strip() for e in args.cluster.split(","))
    primary   = endpoints[0]
    stub      = make_stub(primary)

    # For get workload, pre-load keys once
    if args.op == "get":
        for i in range(PRELOAD_KEYS):
            key   = f"bench1_{i}"
            value = random_value()
            try:
                stub.Put(pb.PutRequest(key=key, value=value))
            except grpc.RpcError:
                pass   # key may already exist from a sibling process

    t_start = time.monotonic()

    if args.op == "put":
        ops, errors, latencies = run_put(stub, args.duration)
    else:
        ops, errors, latencies = run_get(stub, args.duration, args.cluster)

    elapsed = time.monotonic() - t_start
    throughput = ops / elapsed if elapsed > 0 else 0

    result = {
        "pid":        os.getpid(),
        "op":         args.op,
        "ops":        ops,
        "errors":     errors,
        "elapsed_s":  round(elapsed, 3),
        "ops_per_s":  round(throughput, 2),
        "p50_ms":     round(percentile(latencies, 50), 3),
        "p95_ms":     round(percentile(latencies, 95), 3),
        "p99_ms":     round(percentile(latencies, 99), 3),
    }

    # Atomic append to shared output file
    with open(args.output, "a") as f:
        f.write(json.dumps(result) + "\n")

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()