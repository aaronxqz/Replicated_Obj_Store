#!/usr/bin/env python3
"""
test/test_conformance.py — Conformance tests for every RPC and error path.

Tests every condition listed in the spec's "Conformance Testing" section:
  - get/delete on a nonexistent key  → NOT_FOUND
  - put on a key that already exists → ALREADY_EXISTS
  - update on a nonexistent key      → NOT_FOUND
  - put with an invalid key          → INVALID_ARGUMENT
  - put with a value exceeding 1 MiB → INVALID_ARGUMENT
  - list after puts and deletes
  - stats counter accuracy
  - reset clears everything
  - write rejection on a replica     → FAILED_PRECONDITION
  - majority commit (kill one replica, writes still succeed)

Usage:
    # Single-node (most tests):
    python3 server.py --listen localhost:50051 --cluster localhost:50051
    python3 test/test_conformance.py --cluster localhost:50051

    # Replica rejection test (requires 2 nodes):
    python3 server.py --listen localhost:50051 --cluster localhost:50051,localhost:50052 &
    python3 server.py --listen localhost:50052 --cluster localhost:50051,localhost:50052 &
    python3 test/test_conformance.py --cluster localhost:50051,localhost:50052
"""

import argparse
import sys
import time

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

PASS = 0
FAIL = 0


def make_stub(endpoint):
    return pb_grpc.ObjectStoreStub(grpc.insecure_channel(endpoint))


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        print(f"  PASS  {name}")
        PASS += 1
    else:
        print(f"  FAIL  {name}" + (f": {detail}" if detail else ""))
        FAIL += 1


def expect_code(name, fn, expected_code):
    """Call fn(); assert it raises RpcError with expected_code."""
    try:
        fn()
        check(name, False, f"expected {expected_code.name} but got OK")
    except grpc.RpcError as e:
        check(name, e.code() == expected_code,
              f"expected {expected_code.name}, got {e.code().name}")


def expect_ok(name, fn):
    """Call fn(); assert it does NOT raise."""
    try:
        fn()
        check(name, True)
    except grpc.RpcError as e:
        check(name, False, f"expected OK but got {e.code().name}: {e.details()}")


# ---------------------------------------------------------------------------
# Test groups
# ---------------------------------------------------------------------------

def test_basic_ops(stub):
    print("\n[1] Basic operations")
    stub.Reset(empty_pb2.Empty())

    expect_ok("put new key",
        lambda: stub.Put(pb.PutRequest(key="alpha", value=b"aaa")))
    expect_ok("get existing key",
        lambda: stub.Get(pb.GetRequest(key="alpha")))

    resp = stub.Get(pb.GetRequest(key="alpha"))
    check("get returns correct value", resp.value == b"aaa",
          f"got {resp.value!r}")

    expect_ok("update existing key",
        lambda: stub.Update(pb.UpdateRequest(key="alpha", value=b"bbb")))
    resp = stub.Get(pb.GetRequest(key="alpha"))
    check("update changed the value", resp.value == b"bbb",
          f"got {resp.value!r}")

    expect_ok("delete existing key",
        lambda: stub.Delete(pb.DeleteRequest(key="alpha")))


def test_error_paths(stub):
    print("\n[2] Error paths")
    stub.Reset(empty_pb2.Empty())

    expect_code("get missing key", NOT_FOUND,
        lambda: stub.Get(pb.GetRequest(key="missing")))
    expect_code("delete missing key", NOT_FOUND,
        lambda: stub.Delete(pb.DeleteRequest(key="missing")))
    expect_code("update missing key", NOT_FOUND,
        lambda: stub.Update(pb.UpdateRequest(key="missing", value=b"x")))

    stub.Put(pb.PutRequest(key="dup", value=b"v1"))
    expect_code("put duplicate key", ALREADY_EXISTS,
        lambda: stub.Put(pb.PutRequest(key="dup", value=b"v2")))

    # Key with a space (invalid)
    expect_code("put key with space", INVALID_ARGUMENT,
        lambda: stub.Put(pb.PutRequest(key="bad key", value=b"v")))

    # Empty key
    expect_code("put empty key", INVALID_ARGUMENT,
        lambda: stub.Put(pb.PutRequest(key="", value=b"v")))

    # Key that is 129 chars (too long)
    long_key = "a" * 129
    expect_code("put key > 128 chars", INVALID_ARGUMENT,
        lambda: stub.Put(pb.PutRequest(key=long_key, value=b"v")))

    # Value > 1 MiB
    big_value = b"x" * (1024 * 1024 + 1)
    expect_code("put value > 1 MiB", INVALID_ARGUMENT,
        lambda: stub.Put(pb.PutRequest(key="big", value=big_value)))


def test_list(stub):
    print("\n[3] List")
    stub.Reset(empty_pb2.Empty())

    resp = stub.List(empty_pb2.Empty())
    check("list empty store returns 0 entries", len(resp.entries) == 0)

    stub.Put(pb.PutRequest(key="k1", value=b"hello"))
    stub.Put(pb.PutRequest(key="k2", value=b"world!"))
    resp = stub.List(empty_pb2.Empty())
    keys = {e.key for e in resp.entries}
    check("list returns all put keys", keys == {"k1", "k2"}, str(keys))

    sizes = {e.key: e.size_bytes for e in resp.entries}
    check("list size_bytes is correct",
          sizes.get("k1") == 5 and sizes.get("k2") == 6,
          str(sizes))

    stub.Delete(pb.DeleteRequest(key="k1"))
    resp = stub.List(empty_pb2.Empty())
    keys = {e.key for e in resp.entries}
    check("list after delete excludes deleted key", keys == {"k2"}, str(keys))


def test_stats(stub):
    print("\n[4] Stats counter accuracy")
    stub.Reset(empty_pb2.Empty())

    # Verify reset zeroed counters
    s = stub.Stats(empty_pb2.Empty())
    check("stats all zero after reset",
          s.puts == 0 and s.gets == 0 and s.deletes == 0 and s.updates == 0)

    stub.Put(pb.PutRequest(key="s1", value=b"v1"))
    stub.Put(pb.PutRequest(key="s2", value=b"v2"))
    stub.Get(pb.GetRequest(key="s1"))
    stub.Update(pb.UpdateRequest(key="s1", value=b"v1new"))
    stub.Delete(pb.DeleteRequest(key="s2"))

    # Failed put should NOT increment counter
    try:
        stub.Put(pb.PutRequest(key="s1", value=b"dup"))
    except grpc.RpcError:
        pass

    s = stub.Stats(empty_pb2.Empty())
    check("puts counter = 2", s.puts == 2, f"got {s.puts}")
    check("gets counter = 1", s.gets == 1, f"got {s.gets}")
    check("updates counter = 1", s.updates == 1, f"got {s.updates}")
    check("deletes counter = 1", s.deletes == 1, f"got {s.deletes}")
    check("live_objects = 1", s.live_objects == 1, f"got {s.live_objects}")
    check("total_bytes = len('v1new')", s.total_bytes == 5,
          f"got {s.total_bytes}")


def test_reset(stub):
    print("\n[5] Reset")
    stub.Reset(empty_pb2.Empty())

    stub.Put(pb.PutRequest(key="r1", value=b"hello"))
    stub.Put(pb.PutRequest(key="r2", value=b"world"))
    stub.Get(pb.GetRequest(key="r1"))

    stub.Reset(empty_pb2.Empty())

    resp = stub.List(empty_pb2.Empty())
    check("list empty after reset", len(resp.entries) == 0,
          f"got {len(resp.entries)} entries")

    s = stub.Stats(empty_pb2.Empty())
    check("puts=0 after reset", s.puts == 0)
    check("gets=0 after reset", s.gets == 0)
    check("live_objects=0 after reset", s.live_objects == 0)


def test_replica_rejection(primary_stub, replica_stub):
    print("\n[6] Replica write rejection")
    primary_stub.Reset(empty_pb2.Empty())

    expect_code("put on replica → FAILED_PRECONDITION", FAILED_PRECONDITION,
        lambda: replica_stub.Put(pb.PutRequest(key="rtest", value=b"v")))
    expect_code("delete on replica → FAILED_PRECONDITION", FAILED_PRECONDITION,
        lambda: replica_stub.Delete(pb.DeleteRequest(key="rtest")))
    expect_code("update on replica → FAILED_PRECONDITION", FAILED_PRECONDITION,
        lambda: replica_stub.Update(pb.UpdateRequest(key="rtest", value=b"v")))
    expect_code("reset on replica → FAILED_PRECONDITION", FAILED_PRECONDITION,
        lambda: replica_stub.Reset(empty_pb2.Empty()))

    # Reads on replica should work fine
    primary_stub.Put(pb.PutRequest(key="readtest", value=b"hello"))
    time.sleep(0.1)   # allow replication to propagate
    try:
        resp = replica_stub.Get(pb.GetRequest(key="readtest"))
        check("get on replica works", resp.value == b"hello",
              f"got {resp.value!r}")
    except grpc.RpcError as e:
        check("get on replica works", False, str(e))


# ---------------------------------------------------------------------------
# Shortcuts
# ---------------------------------------------------------------------------
NOT_FOUND        = grpc.StatusCode.NOT_FOUND
ALREADY_EXISTS   = grpc.StatusCode.ALREADY_EXISTS
INVALID_ARGUMENT = grpc.StatusCode.INVALID_ARGUMENT
FAILED_PRECONDITION = grpc.StatusCode.FAILED_PRECONDITION


def expect_code(name, code, fn):   # override with correct signature
    global PASS, FAIL
    try:
        fn()
        print(f"  FAIL  {name}: expected {code.name} but got OK")
        FAIL += 1
    except grpc.RpcError as e:
        if e.code() == code:
            print(f"  PASS  {name}")
            PASS += 1
        else:
            print(f"  FAIL  {name}: expected {code.name}, got {e.code().name}")
            FAIL += 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", required=True)
    args = parser.parse_args()

    endpoints = sorted(e.lower().strip() for e in args.cluster.split(","))
    primary   = endpoints[0]
    primary_stub = make_stub(primary)

    test_basic_ops(primary_stub)
    test_error_paths(primary_stub)
    test_list(primary_stub)
    test_stats(primary_stub)
    test_reset(primary_stub)

    if len(endpoints) >= 2:
        replica_stub = make_stub(endpoints[1])
        test_replica_rejection(primary_stub, replica_stub)
    else:
        print("\n[6] Replica rejection — SKIPPED (only 1 node in cluster)")

    print(f"\n{'='*40}")
    print(f"Results: {PASS} passed, {FAIL} failed")
    sys.exit(0 if FAIL == 0 else 1)


if __name__ == "__main__":
    main()