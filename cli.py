#!/usr/bin/env python3
"""
cli.py - Command-line proxy for the distributed object store.

Reads one command per line from stdin and prints results to stdout.
Forwards write commands to the primary; read commands to any node (round-robin).

Usage:
    python3 cli.py --cluster host1:port1,host2:port2,...

Interactive:
    python3 cli.py --cluster localhost:50051

Batch (pipe a file):
    python3 cli.py --cluster localhost:50051 < testcmds.txt

Supported commands:
    put <key> <value>
    get <key>
    delete <key>
    update <key> <value>
    list
    reset
    stats
"""

import argparse
import shlex
import sys

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2


# ---------------------------------------------------------------------------
# Cluster helpers — same normalization rule as server.py and restproxy.py
# ---------------------------------------------------------------------------

def parse_cluster(cluster_arg: str):
    """
    Parse --cluster and return (primary_endpoint, list_of_all_endpoints).
    Primary = lexicographically smallest normalized endpoint.
    """
    endpoints = sorted(e.lower().strip() for e in cluster_arg.split(","))
    return endpoints[0], endpoints


def make_stub(endpoint: str) -> pb_grpc.ObjectStoreStub:
    """Open an insecure gRPC channel and return a stub."""
    channel = grpc.insecure_channel(endpoint)
    return pb_grpc.ObjectStoreStub(channel)


# ---------------------------------------------------------------------------
# CLI handler class
# ---------------------------------------------------------------------------

class CLI:
    """
    Wraps gRPC stubs and implements one method per command.
    Reads are round-robined across all nodes.
    Writes always go to the primary.
    """

    def __init__(self, primary: str, all_endpoints: list):
        self.primary_stub = make_stub(primary)
        self.all_stubs    = [make_stub(ep) for ep in all_endpoints]
        self._read_idx    = 0

    def _read_stub(self) -> pb_grpc.ObjectStoreStub:
        """Round-robin read stub selection across all cluster nodes."""
        stub = self.all_stubs[self._read_idx % len(self.all_stubs)]
        self._read_idx += 1
        return stub

    # ------------------------------------------------------------------
    # Command implementations
    # ------------------------------------------------------------------

    def cmd_put(self, args: list) -> str:
        """
        put <key> <value>
        Value is everything after the key — supports quoted strings via shlex.
        """
        if len(args) < 2:
            return "ERROR: usage: put <key> <value>"
        key   = args[0]
        value = " ".join(args[1:]).encode()   # store as bytes
        try:
            self.primary_stub.Put(pb.PutRequest(key=key, value=value))
            return f"OK"
        except grpc.RpcError as e:
            return f"ERROR [{e.code().name}]: {e.details()}"

    def cmd_get(self, args: list) -> str:
        """
        get <key>
        Prints the stored value as a UTF-8 string (best-effort decode).
        """
        if len(args) != 1:
            return "ERROR: usage: get <key>"
        key = args[0]
        try:
            response = self._read_stub().Get(pb.GetRequest(key=key))
            # Decode bytes to string for display; replace undecodable bytes.
            return response.value.decode("utf-8", errors="replace")
        except grpc.RpcError as e:
            return f"ERROR [{e.code().name}]: {e.details()}"

    def cmd_delete(self, args: list) -> str:
        """delete <key>"""
        if len(args) != 1:
            return "ERROR: usage: delete <key>"
        key = args[0]
        try:
            self.primary_stub.Delete(pb.DeleteRequest(key=key))
            return "OK"
        except grpc.RpcError as e:
            return f"ERROR [{e.code().name}]: {e.details()}"

    def cmd_update(self, args: list) -> str:
        """update <key> <value>"""
        if len(args) < 2:
            return "ERROR: usage: update <key> <value>"
        key   = args[0]
        value = " ".join(args[1:]).encode()
        try:
            self.primary_stub.Update(pb.UpdateRequest(key=key, value=value))
            return "OK"
        except grpc.RpcError as e:
            return f"ERROR [{e.code().name}]: {e.details()}"

    def cmd_list(self, args: list) -> str:
        """list — returns all keys with their value sizes."""
        try:
            response = self._read_stub().List(empty_pb2.Empty())
            if not response.entries:
                return "(empty)"
            lines = [
                f"  {e.key}  ({e.size_bytes} bytes)"
                for e in response.entries
            ]
            return "\n".join(lines)
        except grpc.RpcError as e:
            return f"ERROR [{e.code().name}]: {e.details()}"

    def cmd_reset(self, args: list) -> str:
        """reset — deletes all objects and zeroes all counters."""
        try:
            self.primary_stub.Reset(empty_pb2.Empty())
            return "OK"
        except grpc.RpcError as e:
            return f"ERROR [{e.code().name}]: {e.details()}"

    def cmd_stats(self, args: list) -> str:
        """stats — prints operational counters."""
        try:
            r = self._read_stub().Stats(empty_pb2.Empty())
            return (
                f"  live_objects : {r.live_objects}\n"
                f"  total_bytes  : {r.total_bytes}\n"
                f"  puts         : {r.puts}\n"
                f"  gets         : {r.gets}\n"
                f"  deletes      : {r.deletes}\n"
                f"  updates      : {r.updates}"
            )
        except grpc.RpcError as e:
            return f"ERROR [{e.code().name}]: {e.details()}"

    # ------------------------------------------------------------------
    # Dispatch table
    # ------------------------------------------------------------------

    COMMANDS = {
        "put":    cmd_put,
        "get":    cmd_get,
        "delete": cmd_delete,
        "update": cmd_update,
        "list":   cmd_list,
        "reset":  cmd_reset,
        "stats":  cmd_stats,
    }

    def run_line(self, line: str) -> str | None:
        """
        Parse and execute a single input line.
        Returns the result string, or None for blank/comment lines.
        """
        line = line.strip()
        if not line or line.startswith("#"):
            return None

        # shlex.split handles quoted values like: put mykey "hello world"
        try:
            tokens = shlex.split(line)
        except ValueError as e:
            return f"ERROR: bad syntax: {e}"

        cmd  = tokens[0].lower()
        args = tokens[1:]

        handler = self.COMMANDS.get(cmd)
        if handler is None:
            known = ", ".join(sorted(self.COMMANDS))
            return f"ERROR: unknown command '{cmd}'. Known commands: {known}"

        return handler(self, args)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CLI proxy for the distributed object store",
    )
    parser.add_argument(
        "--cluster", required=True,
        metavar="HOST:PORT[,HOST:PORT,...]",
        help="Comma-separated cluster endpoints (same format as server.py)",
    )
    args = parser.parse_args()

    primary, all_endpoints = parse_cluster(args.cluster)

    # Print config to stderr so it doesn't pollute stdout output
    print(f"Primary : {primary}", file=sys.stderr)
    print(f"Nodes   : {all_endpoints}", file=sys.stderr)

    cli = CLI(primary, all_endpoints)

    # Determine whether we're interactive (TTY) or batch (pipe/file)
    interactive = sys.stdin.isatty()

    for line in sys.stdin:
        if interactive:
            # In interactive mode the shell already echoed the line;
            # no need to echo it back.
            pass
        else:
            # In batch mode, echo the command so output is easy to follow.
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                print(f">>> {stripped}")

        result = cli.run_line(line)
        if result is not None:
            print(result)

        if interactive:
            sys.stdout.flush()


if __name__ == "__main__":
    main()