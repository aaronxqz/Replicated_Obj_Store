#!/usr/bin/env python3
"""
bench/collect_results.py
========================
Reads the .jsonl result files produced by run_bench1.sh and run_bench2.sh,
computes aggregate statistics, and writes the values directly into report.tex
by replacing every \newcommand value in the DATA BLOCK.

Run this AFTER your benchmarks finish, BEFORE compiling the LaTeX.

Usage:
    python3 bench/collect_results.py

Expected input files (all in bench/results/):
    bench1_put_n1.jsonl   bench1_put_n2.jsonl  ... bench1_put_n32.jsonl
    bench1_get_n1.jsonl   bench1_get_n2.jsonl  ... bench1_get_n32.jsonl
    bench2_1node.jsonl
    bench2_2node.jsonl
    bench2_3node.jsonl

Output:
    report.tex  (in-place patch of the DATA BLOCK section)
"""

import json
import os
import re
import statistics
import sys

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR  = os.path.dirname(SCRIPT_DIR)
RESULTS_DIR  = os.path.join(SCRIPT_DIR, "bench/results")
REPORT_TEX   = os.path.join(PROJECT_DIR, "Replicated_Obj_Store/report.tex")

CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32]

NUMBER_WORDS = {
    1: "One", 2: "Two", 4: "Four", 8: "Eight", 16: "Sixteen", 32: "Thirtytwo"
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_jsonl(path):
    """Load all JSON lines from a file. Return empty list if file missing."""
    if not os.path.exists(path):
        print(f"  [WARN] Missing: {path}", file=sys.stderr)
        return []
    with open(path) as f:
        lines = [json.loads(l) for l in f if l.strip()]
    if not lines:
        print(f"  [WARN] Empty: {path}", file=sys.stderr)
    return lines


def aggregate(lines):
    """
    Aggregate per-process results into cluster-wide summary.
    Returns dict with keys: ops_per_s, p50_ms, p95_ms, p99_ms
    All are rounded to 1 decimal place for LaTeX readability.
    """
    if not lines:
        return {"ops_per_s": "N/A", "p50_ms": "N/A", "p95_ms": "N/A", "p99_ms": "N/A"}

    total_ops_s = sum(l["ops_per_s"] for l in lines)
    p50 = statistics.median(l["p50_ms"] for l in lines)
    p95 = statistics.median(l["p95_ms"] for l in lines)
    p99 = statistics.median(l["p99_ms"] for l in lines)

    return {
        "ops_per_s": f"{total_ops_s:.0f}",
        "p50_ms":    f"{p50:.1f}",
        "p95_ms":    f"{p95:.1f}",
        "p99_ms":    f"{p99:.1f}",
    }


# ---------------------------------------------------------------------------
# Collect all data
# ---------------------------------------------------------------------------

def collect():
    data = {}

    # Benchmark 1 — put and get at each concurrency level
    for n in CONCURRENCY_LEVELS:
        word = NUMBER_WORDS[n]
        for op in ("put", "get"):
            path   = os.path.join(RESULTS_DIR, f"bench1_{op}_n{n}.jsonl")
            lines  = load_jsonl(path)
            agg    = aggregate(lines)
            op_key = op.capitalize()
            data[f"BOne{op_key}{word}"]    = agg["ops_per_s"]
            data[f"BOne{op_key}PNN{word}"] = agg["p99_ms"]

    # Benchmark 2 — replication cost
    for config, label in [("1node", "One"), ("2node", "Two"), ("3node", "Three")]:
        path  = os.path.join(RESULTS_DIR, f"bench2_{config}.jsonl")
        lines = load_jsonl(path)
        agg   = aggregate(lines)
        data[f"BTwoTwo{label}Ops"]      = data.get(f"BTwoTwo{label}Ops", "N/A")  # avoid overwrite
        data[f"BTwo{label}Ops"]         = agg["ops_per_s"]
        data[f"BTwo{label}PFifty"]      = agg["p50_ms"]
        data[f"BTwo{label}PNN"]         = agg["p99_ms"]

    return data


# ---------------------------------------------------------------------------
# Patch report.tex
# ---------------------------------------------------------------------------

def patch_tex(data):
    if not os.path.exists(REPORT_TEX):
        print(f"ERROR: {REPORT_TEX} not found. Make sure report.tex is in the project root.",
              file=sys.stderr)
        sys.exit(1)

    with open(REPORT_TEX) as f:
        original = f.read()

    patched = original
    replaced = 0

    for macro, value in data.items():
        # Match: \newcommand{\MacroName}{anything}
        # Captures everything between the last { and last } on that line.
        pattern     = rf'(\\newcommand{{\\{macro}}}{{)[^}}]*(}})'
        replacement = rf'\g<1>{value}\g<2>'
        new_text, count = re.subn(pattern, replacement, patched)
        if count:
            patched  = new_text
            replaced += count
        else:
            print(f"  [WARN] Macro \\{macro} not found in report.tex", file=sys.stderr)

    with open(REPORT_TEX, "w") as f:
        f.write(patched)

    print(f"Patched {replaced} macros in {REPORT_TEX}")
    return replaced


# ---------------------------------------------------------------------------
# Print summary table to terminal
# ---------------------------------------------------------------------------

def print_summary(data):
    print("\n" + "=" * 60)
    print(" Benchmark 1 — Throughput (ops/s) and p99 latency (ms)")
    print("=" * 60)
    print(f"{'Clients':>8}  {'Put ops/s':>12}  {'Put p99':>8}  {'Get ops/s':>12}  {'Get p99':>8}")
    print("-" * 60)
    for n in CONCURRENCY_LEVELS:
        w    = NUMBER_WORDS[n]
        pops = data.get(f"BOnePut{w}", "N/A")
        pp99 = data.get(f"BOnePutPNN{w}", "N/A")
        gops = data.get(f"BOneGet{w}", "N/A")
        gp99 = data.get(f"BOneGetPNN{w}", "N/A")
        print(f"{n:>8}  {pops:>12}  {pp99:>8}  {gops:>12}  {gp99:>8}")

    print("\n" + "=" * 60)
    print(" Benchmark 2 — Replication Cost (8 clients, 4 KiB)")
    print("=" * 60)
    print(f"{'Config':>12}  {'Ops/s':>10}  {'p50 ms':>8}  {'p99 ms':>8}")
    print("-" * 60)
    for label, name in [("One", "1-node"), ("Two", "2-node"), ("Three", "3-node")]:
        ops = data.get(f"BTwo{label}Ops", "N/A")
        p50 = data.get(f"BTwo{label}PFifty", "N/A")
        p99 = data.get(f"BTwo{label}PNN", "N/A")
        print(f"{name:>12}  {ops:>10}  {p50:>8}  {p99:>8}")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Reading results from: {RESULTS_DIR}")
    print(f"Patching:             {REPORT_TEX}\n")

    data = collect()
    print_summary(data)
    patch_tex(data)

    print("\nNext steps:")
    print("  pdflatex report.tex")
    print("  pdflatex report.tex   # run twice for cross-references")