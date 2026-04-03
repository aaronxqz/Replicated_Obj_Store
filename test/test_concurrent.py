#!/usr/bin/env python3
"""
test/test_concurrent.py — Concurrent correctness test for the object store.

Requirement (from spec):
  - Launch 20 threads, each performing 200 mixed put/get/delete operations
    on overlapping keys simultaneously.
  - After all threads complete, assert that the final state is consistent:
    no phantom keys, no corrupted values, and stats counters match the
    number of successful operations recorded by the threads.

Usage:
    # Start a single-node server first:
    #   python3 server.py --listen localhost:50051 --cluster localhost:50051
    python3 test/test_concurrent.py --cluster localhost:50051

The test exits with code 0 on success and 1 on any assertion failure.
"""

import argparse
import random
import string
import sys
import threading
import time

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
NUM_THREADS   = 20
OPS_PER_THREAD = 200
KEY_SPACE     = 30      # small key space so threads heavily overlap
KEY_PREFIX    = "cctest_"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_stub(endpoint: str) -> pb_grpc.ObjectStoreStub:
    channel = grpc.insecure_channel(endpoint)
    return pb_grpc.ObjectStoreStub(channel)


def random_value(length: int = 64) -> bytes:
    return "".join(random.choices(string.ascii_letters, k=length)).encode()


def all_keys() -> list:
    return [f"{KEY_PREFIX}{i}" for i in range(KEY_SPACE)]


# ---------------------------------------------------------------------------
# Worker: runs on each thread
# ---------------------------------------------------------------------------

def worker(thread_id: int, stub: pb_grpc.ObjectStoreStub,
           counters: dict, lock: threading.Lock):
    """
    Performs OPS_PER_THREAD mixed put/get/delete operations.
    Records the number of successful puts, gets, and deletes in `counters`
    under the protection of `lock` so the main thread can aggregate them.
    """
    local_puts    = 0
    local_gets    = 0
    local_deletes = 0

    for _ in range(OPS_PER_THREAD):
        key   = f"{KEY_PREFIX}{random.randint(0, KEY_SPACE - 1)}"
        op    = random.choice(["put", "get", "delete"])
        value = random_value()

        try:
            if op == "put":
                stub.Put(pb.PutRequest(key=key, value=value))
                local_puts += 1
            elif op == "get":
                stub.Get(pb.GetRequest(key=key))
                local_gets += 1
            elif op == "delete":
                stub.Delete(pb.DeleteRequest(key=key))
                local_deletes += 1
        except grpc.RpcError as e:
            # ALREADY_EXISTS on put, NOT_FOUND on get/delete are all valid
            # outcomes under concurrency — not test failures.
            code = e.code()
            if code not in (
                grpc.StatusCode.ALREADY_EXISTS,
                grpc.StatusCode.NOT_FOUND,
            ):
                # Any other error IS unexpected — log it.
                print(
                    f"  [thread {thread_id}] Unexpected gRPC error "
                    f"{code.name}: {e.details()}"
                )

    with lock:
        counters["puts"]    += local_puts
        counters["gets"]    += local_gets
        counters["deletes"] += local_deletes


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

def assert_no_phantom_keys(stub: pb_grpc.ObjectStoreStub):
    """
    Every key returned by List() must be one of our known test keys.
    No extra keys should appear.
    """
    response  = stub.List(empty_pb2.Empty())
    known     = set(all_keys())
    live_keys = {e.key for e in response.entries}

    phantoms = live_keys - known
    assert not phantoms, (
        f"FAIL: phantom keys found in store: {phantoms}"
    )
    print(f"  No phantom keys. Live keys after test: {len(live_keys)}")


def assert_values_not_corrupted(stub: pb_grpc.ObjectStoreStub):
    """
    Every key that List() says exists must return a non-empty value via Get().
    The value must be valid UTF-8 ASCII (since we only stored ASCII).
    """
    response = stub.List(empty_pb2.Empty())
    for entry in response.entries:
        get_resp = stub.Get(pb.GetRequest(key=entry.key))
        assert get_resp.value, (
            f"FAIL: key '{entry.key}' listed but returned empty value"
        )
        try:
            get_resp.value.decode("ascii")
        except UnicodeDecodeError:
            assert False, (
                f"FAIL: key '{entry.key}' has corrupted (non-ASCII) value"
            )
    print(f"  All {len(response.entries)} live values are non-empty and valid ASCII.")


def assert_stats_consistent(stub: pb_grpc.ObjectStoreStub,
                             expected_puts: int,
                             expected_gets: int,
                             expected_deletes: int):
    """
    The server's Stats counters must equal the number of successful operations
    that the worker threads recorded.
    """
    stats = stub.Stats(empty_pb2.Empty())

    assert stats.puts == expected_puts, (
        f"FAIL: stats.puts={stats.puts} but threads recorded {expected_puts}"
    )
    assert stats.gets == expected_gets, (
        f"FAIL: stats.gets={stats.gets} but threads recorded {expected_gets}"
    )
    assert stats.deletes == expected_deletes, (
        f"FAIL: stats.deletes={stats.deletes} but threads recorded {expected_deletes}"
    )

    # live_objects must be consistent with total_bytes
    list_resp = stub.List(empty_pb2.Empty())
    real_total = sum(e.size_bytes for e in list_resp.entries)
    assert stats.live_objects == len(list_resp.entries), (
        f"FAIL: stats.live_objects={stats.live_objects} "
        f"but List() returned {len(list_resp.entries)} entries"
    )
    assert stats.total_bytes == real_total, (
        f"FAIL: stats.total_bytes={stats.total_bytes} "
        f"but sum of sizes from List()={real_total}"
    )
    print(
        f"  Stats consistent: puts={stats.puts}, gets={stats.gets}, "
        f"deletes={stats.deletes}, live_objects={stats.live_objects}"
    )


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------

def run_test(cluster_arg: str):
    endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
    primary   = endpoints[0]
    stub      = make_stub(primary)

    # ---- setup: clean slate -----------------------------------------------
    print("Resetting server state...")
    stub.Reset(empty_pb2.Empty())
    time.sleep(0.2)   # let reset propagate in cluster mode

    # ---- launch threads ----------------------------------------------------
    print(f"Launching {NUM_THREADS} threads × {OPS_PER_THREAD} ops each "
          f"(key space: {KEY_SPACE} keys)...")

    counters = {"puts": 0, "gets": 0, "deletes": 0}
    lock     = threading.Lock()
    threads  = []

    t0 = time.time()
    for i in range(NUM_THREADS):
        t = threading.Thread(
            target=worker,
            args=(i, stub, counters, lock),
            daemon=True,
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
    elapsed = time.time() - t0

    total_ops = NUM_THREADS * OPS_PER_THREAD
    print(f"  {total_ops} ops completed in {elapsed:.2f}s "
          f"({total_ops / elapsed:.0f} ops/sec)")
    print(f"  Successful puts={counters['puts']}, "
          f"gets={counters['gets']}, deletes={counters['deletes']}")

    # ---- assertions --------------------------------------------------------
    print("\nRunning consistency assertions...")
    failed = False
    try:
        assert_no_phantom_keys(stub)
        assert_values_not_corrupted(stub)
        assert_stats_consistent(
            stub,
            expected_puts    = counters["puts"],
            expected_gets    = counters["gets"],
            expected_deletes = counters["deletes"],
        )
    except AssertionError as e:
        print(f"\n{e}")
        failed = True

    # ---- cleanup -----------------------------------------------------------
    stub.Reset(empty_pb2.Empty())

    if failed:
        print("\nRESULT: FAIL")
        sys.exit(1)
    else:
        print("\nRESULT: PASS — all consistency checks passed.")
        sys.exit(0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Concurrent correctness test for the object store"
    )
    parser.add_argument(
        "--cluster", required=True,
        help="Cluster endpoints (same format as server.py)"
    )
    args = parser.parse_args()
    run_test(args.cluster)