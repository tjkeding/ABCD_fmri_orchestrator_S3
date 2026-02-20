#!/usr/bin/env python3

# ============================================================================
# PARALLEL BATCH RUNNER FOR fMRI FIRST-LEVEL ORCHESTRATOR
#
# Runs orchestrate_first_level.py in parallel across a list of subjects
# using a thread pool (each thread launches a subprocess). Progress is
# tracked with a rolling status line updated every 2 seconds. A summary
# CSV is written after all subjects complete (or on Ctrl+C).
#
# Usage:
#   python run_orchestrator.py \
#     --orchestrate_config study.yaml \
#     --proc_config example_config.yaml \
#     --subject-list subjects.txt \
#     --n-jobs 8 \
#     --log-dir logs/ \
#     [--dry-run] [--skip-qc] [--skip-first-level] \
#     [--summary-file summary.csv]
#
# Subject list format:
#   Plain text, one sub_id per line.
#   Blank lines and lines starting with '#' are ignored.
#   Duplicate IDs produce a warning and are skipped.
#
# Exit codes:
#   0   — all subjects succeeded
#   1   — one or more subjects failed
#   130 — interrupted by Ctrl+C
#
# Author: Taylor J. Keding, Ph.D.
# Version: 2.0
# Last updated: 02/19/26
# ============================================================================

import os
import sys
import csv
import time
import argparse
import threading
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# Module-level path resolution (validated before any worker is launched)
# ---------------------------------------------------------------------------
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_ORCHESTRATE_SCRIPT = os.path.join(_THIS_DIR, "orchestrate_first_level.py")


# ============================================================================
# Subject list parsing
# ============================================================================

def parse_subject_list(path):
    """
    Parse a plain-text subject list file.

    Blank lines and '#'-prefixed lines are ignored. Duplicate IDs produce a
    warning and are skipped.

    Parameters
    ----------
    path : str
        Path to the subject list file.

    Returns
    -------
    list of str
        Ordered list of unique subject IDs.
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Subject list not found: {path}")

    sub_ids, seen = [], set()

    with open(path) as f:
        for lineno, line in enumerate(f, start=1):
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s in seen:
                print(
                    f"WARNING: Duplicate subject ID on line {lineno}: "
                    f"'{s}' — skipping.",
                    flush=True
                )
                continue
            seen.add(s)
            sub_ids.append(s)

    return sub_ids

# ============================================================================
# Per-subject worker
# ============================================================================

def _run_one(sub_id, orchestrate_config_path, proc_config_path, log_dir, extra_flags, state, lock, cancel_event):
    """
    Run orchestrate_first_level.py for one subject in a subprocess.

    Checks cancel_event before starting so that pending workers can be
    cleanly skipped on KeyboardInterrupt.

    Parameters
    ----------
    sub_id : str
    orchestrate_config_path : str
    proc_config_path : str
    log_dir : str
    extra_flags : list of str
        Extra CLI flags forwarded to orchestrate_first_level.py
        (e.g. ["--dry-run", "--skip-qc"]).
    state : dict
        Shared progress state dict.
    lock : threading.Lock
    cancel_event : threading.Event
        Set on KeyboardInterrupt; prevents not-yet-started workers from
        launching their subprocess.

    Returns
    -------
    tuple of (str, float, str, str)
        (status, runtime_seconds, error_message, log_file)
        status is one of "success", "failed", "cancelled".
    """
    log_file = os.path.join(log_dir, f"{sub_id}.log")

    # Bail out early if a cancel has been requested
    if cancel_event.is_set():
        return "cancelled", 0.0, "Cancelled before start", log_file

    with lock:
        state["running"].add(sub_id)

    t0 = time.time()
    try:
        cmd = [
            sys.executable, _ORCHESTRATE_SCRIPT,
            "--orchestrate_config", orchestrate_config_path,
            "--proc_config", proc_config_path,
            "--subj_id", sub_id,
            "--log-file", log_file,
        ] + extra_flags

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )
        runtime = time.time() - t0

        if result.returncode == 0:
            with lock:
                state["n_success"] += 1
            return "success", runtime, "", log_file
        else:
            # Extract last non-empty stderr line as the error summary
            stderr_lines = [ln for ln in result.stderr.splitlines() if ln.strip()]
            if stderr_lines:
                err_msg = stderr_lines[-1]
            else:
                stdout_lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
                err_msg = stdout_lines[-1] if stdout_lines else f"Exit code {result.returncode}"
            with lock:
                state["n_failed"] += 1
            return "failed", runtime, err_msg, log_file

    except Exception as e:
        runtime = time.time() - t0
        with lock:
            state["n_failed"] += 1
        return "failed", runtime, str(e), log_file

    finally:
        with lock:
            state["running"].discard(sub_id)

# ============================================================================
# Progress display
# ============================================================================

def _progress_loop(state, lock, total, stop_event):
    """
    Daemon thread that prints a rolling progress line every 2 seconds.

    Prints a final newline when stop_event is set so the terminal is left
    clean after the run.
    """
    while not stop_event.is_set():
        with lock:
            running = sorted(state["running"])
            ns = state["n_success"]
            nf = state["n_failed"]
            nsk = state["n_skipped"]

        n_done = ns + nf + nsk

        if len(running) > 6:
            running_str = ", ".join(running[:6]) + f", …+{len(running) - 6}"
        elif running:
            running_str = ", ".join(running)
        else:
            running_str = "none"

        line = (
            f"\r[{n_done:3d}/{total}] Running: {running_str} | "
            f"Done: {ns} success, {nf} failed, {nsk} skipped"
        )
        sys.stdout.write(line[:120])
        sys.stdout.flush()

        stop_event.wait(timeout=2.0)

    sys.stdout.write("\n")
    sys.stdout.flush()

# ============================================================================
# Summary CSV
# ============================================================================

def _write_summary_csv(summary_rows, summary_file):
    """Write summary_rows to a CSV file."""
    os.makedirs(os.path.dirname(os.path.abspath(summary_file)), exist_ok=True)
    fieldnames = ["sub_id", "status", "runtime_seconds", "error_message", "log_file"]
    with open(summary_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)
    print(f"Summary CSV written: {summary_file}", flush=True)

# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Parallel batch runner for the fMRI first-level orchestrator.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run, 2 parallel workers
  python run_orchestrator.py --orchestrate_config study.yaml \\
    --proc_config example_config.yaml \\
    --subject-list subjects.txt --n-jobs 2 --log-dir logs/ --dry-run

  # Full run, 8 parallel workers
  python run_orchestrator.py --orchestrate_config study.yaml \\
    --proc_config example_config.yaml \\
    --subject-list subjects.txt --n-jobs 8 --log-dir logs/
        """,
    )
    parser.add_argument(
        "--orchestrate_config", required=True,
        help="Path to the orchestrator YAML configuration file.",
    )
    parser.add_argument(
        "--proc_config", required=True,
        help="Path to the fmri_first_level_proc YAML template configuration file.",
    )
    parser.add_argument(
        "--subject-list", required=True,
        help="Path to a plain-text subject list (one sub_id per line; # = comment).",
    )
    parser.add_argument(
        "--n-jobs", type=int, default=1,
        help="Number of subjects to process in parallel (default: 1).",
    )
    parser.add_argument(
        "--log-dir", required=True,
        help="Directory for per-subject log files.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Pass --dry-run to each orchestrate_first_level.py call.",
    )
    parser.add_argument(
        "--skip-qc", action="store_true",
        help="Pass --skip-qc to each orchestrate_first_level.py call.",
    )
    parser.add_argument(
        "--skip-first-level", action="store_true",
        help="Pass --skip-first-level to each orchestrate_first_level.py call.",
    )
    parser.add_argument(
        "--session", type=str, default=None,
        help=(
            "Pass --session to each orchestrate_first_level.py call. "
            "Process only this session code (e.g. '00'). Useful for "
            "reprocessing a specific session that previously failed."
        ),
    )
    parser.add_argument(
        "--summary-file", default=None,
        help=(
            "Path to the output summary CSV. "
            "Defaults to {log_dir}/run_summary_{YYYYMMDD_HHMMSS}.csv"
        ),
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Pre-flight checks
    # ------------------------------------------------------------------
    if not os.path.isfile(_ORCHESTRATE_SCRIPT):
        print(
            f"ERROR: orchestrate_first_level.py not found: {_ORCHESTRATE_SCRIPT}",
            file=sys.stderr
        )
        sys.exit(1)

    os.makedirs(args.log_dir, exist_ok=True)

    if args.summary_file is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.summary_file = os.path.join(args.log_dir, f"run_summary_{ts}.csv")

    try:
        sub_ids = parse_subject_list(args.subject_list)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    if not sub_ids:
        print("ERROR: Subject list is empty (no valid entries found).", file=sys.stderr)
        sys.exit(1)

    total = len(sub_ids)
    print(f"Subjects        : {total}", flush=True)
    print(f"Workers         : {args.n_jobs}", flush=True)
    print(f"Orchestrate cfg : {args.orchestrate_config}", flush=True)
    print(f"Proc cfg        : {args.proc_config}", flush=True)
    print(f"Log dir         : {args.log_dir}", flush=True)
    print(f"Summary         : {args.summary_file}", flush=True)
    if args.dry_run:
        print("Mode      : DRY RUN", flush=True)

    # Flags forwarded to each orchestrate_first_level.py call
    extra_flags = []
    if args.dry_run:
        extra_flags.append("--dry-run")
    if args.skip_qc:
        extra_flags.append("--skip-qc")
    if args.skip_first_level:
        extra_flags.append("--skip-first-level")
    if args.session:
        extra_flags.extend(["--session", args.session])

    # ------------------------------------------------------------------
    # Shared state
    # ------------------------------------------------------------------
    state = {"running": set(), "n_success": 0, "n_failed": 0, "n_skipped": 0}
    lock = threading.Lock()
    cancel_event = threading.Event()
    stop_event = threading.Event()

    # Background progress printer (daemon — auto-exits when main thread ends)
    progress_thread = threading.Thread(
        target=_progress_loop,
        args=(state, lock, total, stop_event),
        daemon=True,
    )
    progress_thread.start()

    # ------------------------------------------------------------------
    # Parallel execution
    # ------------------------------------------------------------------
    summary_rows = []
    futures_done = set()
    future_to_sub = {}
    exit_code = 0
    interrupted = False

    try:
        executor = ThreadPoolExecutor(max_workers=args.n_jobs)

        try:
            # Submit all subjects
            for sub_id in sub_ids:
                f = executor.submit(
                    _run_one,
                    sub_id, args.orchestrate_config, args.proc_config,
                    args.log_dir, extra_flags,
                    state, lock, cancel_event,
                )
                future_to_sub[f] = sub_id

            # Collect results as they complete
            for future in as_completed(future_to_sub):
                sub_id = future_to_sub[future]
                futures_done.add(future)
                try:
                    status, runtime, error_message, log_file = future.result()
                except Exception as e:
                    status = "failed"
                    runtime = 0.0
                    error_message = str(e)
                    log_file = os.path.join(args.log_dir, f"{sub_id}.log")
                    with lock:
                        state["n_failed"] += 1

                summary_rows.append({
                    "sub_id": sub_id,
                    "status": status,
                    "runtime_seconds": round(runtime, 2),
                    "error_message": error_message,
                    "log_file": log_file,
                })
                if status == "failed":
                    exit_code = 1

        except KeyboardInterrupt:
            interrupted = True
            print(
                "\nInterrupted (Ctrl+C) — cancelling pending jobs, "
                "waiting for in-flight analyses to finish...",
                flush=True
            )
            cancel_event.set()
            # shutdown(wait=True) waits for threads currently running
            # subprocesses to finish; cancel_event prevents new workers
            # from launching their subprocess.
            executor.shutdown(wait=True)

            # Collect results from futures that completed during the wait
            for f, sub_id in future_to_sub.items():
                if f in futures_done:
                    continue
                if f.done() and not f.cancelled():
                    try:
                        status, runtime, error_message, log_file = f.result()
                    except Exception as e:
                        status = "failed"
                        runtime = 0.0
                        error_message = str(e)
                        log_file = os.path.join(args.log_dir, f"{sub_id}.log")
                    summary_rows.append({
                        "sub_id": sub_id,
                        "status": status,
                        "runtime_seconds": round(runtime, 2),
                        "error_message": error_message,
                        "log_file": log_file,
                    })
                else:
                    # Never started or was cancelled before starting
                    summary_rows.append({
                        "sub_id": sub_id,
                        "status": "cancelled",
                        "runtime_seconds": 0.0,
                        "error_message": "Cancelled by KeyboardInterrupt",
                        "log_file": os.path.join(args.log_dir, f"{sub_id}.log"),
                    })

        else:
            executor.shutdown(wait=False)

    finally:
        stop_event.set()
        progress_thread.join(timeout=3.0)

    # ------------------------------------------------------------------
    # Write summary CSV (always, even on interrupt)
    # ------------------------------------------------------------------
    _write_summary_csv(summary_rows, args.summary_file)

    n_success = sum(1 for r in summary_rows if r["status"] == "success")
    n_failed = sum(1 for r in summary_rows if r["status"] == "failed")
    n_cancelled = sum(1 for r in summary_rows if r["status"] == "cancelled")

    print(
        f"\nBatch complete: {n_success} success, {n_failed} failed, "
        f"{n_cancelled} cancelled",
        flush=True
    )

    if interrupted:
        sys.exit(130)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
