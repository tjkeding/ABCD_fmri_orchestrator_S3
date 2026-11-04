#!/usr/bin/env python3

# ============================================================================
# Per-participant orchestrator for ABCD fMRI first-level processing; one session at a time bounds disk use.
# Author: Taylor J. Keding, Ph.D.
# ============================================================================

import os
import sys
import time
import shutil
import argparse

import yaml

from fmri_first_level_proc.first_level_utils import setup_logging
from fmri_first_level_proc.first_level_config import load_and_validate
from fmri_first_level_proc.run_first_level import DISPATCH
from fmri_first_level_proc import names as upstream_names

# Guarded so an older upstream install reaches the version check in main() instead of failing at import.
try:
    from fmri_first_level_proc.exceptions import (
        ConfigError, InputError, ModelError, GuardError,
        ToolUnavailableError, ToolVersionError, ToolExecutionError,
    )
    from fmri_first_level_proc.runtime import (
        preflight_tool_environments, runtime_check, load_lockfile,
    )
    _UPSTREAM_IMPORT_ERROR = None
except ImportError as _import_err:
    _UPSTREAM_IMPORT_ERROR = _import_err

__version__ = "4.0.0"

from orchestrator_utils import (
    OrchestratorError,
    VALID_TASK_LABELS,
    EXIT_COMPLETE,
    EXIT_FAILED_UNEXPLAINED,
    EXIT_FAILED_KNOWN_CAUSE,
    discover_subject_inputs,
    check_subject_input_completeness,
    download_session_inputs,
    determine_session_routing,
    upload_session_to_s3,
    cleanup_local_inputs,
    verify_upstream_version,
    MIN_UPSTREAM_VERSION,
    json_safe_context,
    build_first_level_qc_from_result,
    load_orchestrator_config,
    validate_proc_template,
    detect_non_steady_state_trs,
    remove_initial_trs_bold,
    extract_motion_regressors,
    derive_calc_n_motion_derivs,
    compute_tissue_mask_regressor,
    fix_nback_cue_labels,
    format_task_timing,
    validate_task_timing,
    check_required_conditions_present,
    compute_preproc_qc,
    consolidate_session_qc,
    is_outcome_terminal,
    classify_session_exit,
    most_severe_exit,
    build_first_level_config,
    write_temp_config,
    compute_first_level_qc,
    attach_afni_version_filter,
    filter_afni_unvalidated_warnings,
)


_EXIT_LABELS = {
    EXIT_COMPLETE: "complete",
    EXIT_FAILED_UNEXPLAINED: "failed, unexplained",
    EXIT_FAILED_KNOWN_CAUSE: "failed, known cause",
}


def _derive_session_status(routing, analysis_outcomes):
    """Session status: "skipped" for skip routing, else "success" (all analyses succeeded), "partial"
    (some succeeded), or "failed" (none, including all skipped)."""
    if routing == "skip":
        return "skipped"
    if not analysis_outcomes:
        return "failed"
    if all(o["status"] == "success" for o in analysis_outcomes):
        return "success"
    if any(o["status"] == "success" for o in analysis_outcomes):
        return "partial"
    return "failed"


def process_participant(config, sub_id, proc_template, skip_qc, skip_first_level,
                        dry_run, logger, session_filter=None):
    """
    Run the pipeline for one participant, finishing each session (download through cleanup) before the next.

    Returns
    -------
    int
        The most severe session exit code (EXIT_COMPLETE, EXIT_FAILED_UNEXPLAINED, EXIT_FAILED_KNOWN_CAUSE).
    """
    if _UPSTREAM_IMPORT_ERROR is not None:
        raise OrchestratorError(
            f"Failed to import fmri_first_level_proc >= {MIN_UPSTREAM_VERSION} "
            f"modules: {_UPSTREAM_IMPORT_ERROR}"
        )

    study = config["study"]
    task_defs = config["tasks"]
    analyses = config["analyses"]
    s3_cfg = config["s3"]

    # Discover sessions (one listing per subject)
    logger.info("Discovering available sessions...")
    discovered = discover_subject_inputs(
        study, s3_cfg, sub_id, task_defs, logger, session_filter=session_filter
    )
    sessions = sorted(discovered.keys())

    if not sessions:
        if session_filter is not None:
            raise OrchestratorError(
                f"Session filter '{session_filter}' not found among sessions with a "
                f"producer completion manifest for sub-{sub_id}."
            )
        raise OrchestratorError(
            f"No sessions discovered for sub-{sub_id}. "
            f"Check that input files exist and match the configured task labels."
        )
    if session_filter is not None:
        logger.info("Session filter applied: processing only session %s", session_filter)

    check_subject_input_completeness(discovered, analyses, proc_template, logger)

    if dry_run:
        logger.info("[DRY RUN] Sessions to process: %s", sessions)
        for session in sessions:
            ses_data = discovered[session]
            logger.info("[DRY RUN] Session %s:", session)
            for task_def in task_defs:
                task = task_def["task_label"]
                n_runs = len(ses_data.get(task, {}))
                logger.info("[DRY RUN]   Task: %s (%d runs)", task, n_runs)
            for a in analyses:
                logger.info(
                    "[DRY RUN]   Analysis: %s (type: %s, task: %s)",
                    a["name"], a["type"], a["task_label"]
                )
        return EXIT_COMPLETE

    session_results = {}

    for session in sessions:
        ses_label = f"ses-{session}A"
        logger.info("=" * 70)
        logger.info("PROCESSING SESSION: sub-%s / %s", sub_id, ses_label)
        logger.info("=" * 70)

        session_start = time.time()

        try:
            session_result = _process_session(
                config, sub_id, session, proc_template,
                skip_qc, skip_first_level, logger,
                discovered_session=discovered[session]
            )
            session_status = _derive_session_status(
                session_result["routing"], session_result["analysis_outcomes"]
            )
            session_results[session] = {
                "status": session_status,
                "routing": session_result["routing"],
                "analyses": session_result["analysis_outcomes"],
                "qc_path": session_result["qc_path"],
                "remote_metadata": session_result["remote_metadata"],
                "exit_category": session_result["exit_category"],
                "preproc_excluded_runs": session_result["preproc_excluded_runs"],
            }

        except OrchestratorError as e:
            logger.error(
                "Session %s failed for sub-%s: %s", ses_label, sub_id, str(e)
            )
            session_results[session] = {
                "status": "failed",
                "routing": "full",
                "analyses": [],
                "qc_path": None,
                "remote_metadata": {},
                "error": str(e),
                "exit_category": EXIT_FAILED_UNEXPLAINED,
                "preproc_excluded_runs": [],
            }

        except Exception as e:
            logger.error(
                "Unexpected error in session %s for sub-%s: %s",
                ses_label, sub_id, str(e),
                exc_info=True
            )
            session_results[session] = {
                "status": "failed",
                "routing": "full",
                "analyses": [],
                "qc_path": None,
                "remote_metadata": {},
                "error": str(e),
                "exit_category": EXIT_FAILED_UNEXPLAINED,
                "preproc_excluded_runs": [],
            }

        elapsed = time.time() - session_start
        ses_status = session_results[session]["status"].upper()
        exit_cat = session_results[session]["exit_category"]
        logger.info(
            "Session %s %s (%.2f seconds, exit category: %s)",
            ses_label, ses_status, elapsed, _EXIT_LABELS.get(exit_cat, str(exit_cat))
        )

    # Summary across sessions
    logger.info("=" * 70)
    logger.info("SESSION SUMMARY for sub-%s:", sub_id)
    n_by_exit = {EXIT_COMPLETE: 0, EXIT_FAILED_UNEXPLAINED: 0, EXIT_FAILED_KNOWN_CAUSE: 0}
    for ses, result in sorted(session_results.items()):
        ses_status = result["status"]
        routing = result.get("routing", "full")
        exit_cat = result["exit_category"]
        n_by_exit[exit_cat] = n_by_exit.get(exit_cat, 0) + 1

        if routing == "skip":
            logger.info("  ses-%sA: %s (skipped, prior outputs intact)", ses, ses_status)
        else:
            logger.info(
                "  ses-%sA: %s (%s)",
                ses, ses_status, _EXIT_LABELS.get(exit_cat, str(exit_cat))
            )

        for outcome in result.get("analyses", []):
            flagged_suffix = " [FLAGGED]" if outcome.get("qc_flagged") else ""
            if outcome["status"] == "success":
                n_high = sum(
                    1 for w in outcome.get("warnings", [])
                    if isinstance(w, dict) and w.get("severity") == "high"
                )
                if n_high > 0:
                    logger.info(
                        "    [OK]     %s (%.2fs) [%d high-severity warning(s)]%s",
                        outcome["name"], outcome["wall_time_seconds"], n_high, flagged_suffix
                    )
                else:
                    logger.info(
                        "    [OK]     %s (%.2fs)%s",
                        outcome["name"], outcome["wall_time_seconds"], flagged_suffix
                    )
            elif outcome["status"] == "skipped":
                logger.info(
                    "    [SKIPPED] %s: %s",
                    outcome["name"], outcome.get("reason", "unknown")
                )
            else:
                logger.info(
                    "    [FAILED] %s: %s%s",
                    outcome["name"], outcome.get("error", "unknown"), flagged_suffix
                )
        if result.get("error"):
            logger.info("    Session-level error: %s", result["error"])

    logger.info(
        "Total: %d complete, %d failed (known cause), %d failed (unexplained) "
        "out of %d session(s)",
        n_by_exit.get(EXIT_COMPLETE, 0),
        n_by_exit.get(EXIT_FAILED_KNOWN_CAUSE, 0),
        n_by_exit.get(EXIT_FAILED_UNEXPLAINED, 0),
        len(session_results)
    )
    logger.info("=" * 70)

    code = most_severe_exit([r["exit_category"] for r in session_results.values()])
    logger.info("Subject exit code %d (%s)", code, _EXIT_LABELS.get(code, str(code)))
    return code


def _process_session(config, sub_id, session, proc_template, skip_qc,
                     skip_first_level, logger, discovered_session=None):
    """
    Process one session: skip if its sentinel exists (unless force_recompute); otherwise download,
    preprocess, analyze, write QC, upload, then write the sentinel unless exit category 1 and clean up.
    """
    study = config["study"]
    task_defs = config["tasks"]
    analyses = config["analyses"]
    qc_cfg = config.get("qc", {"preproc": {"enabled": False}, "first_level": {"enabled": False}})
    s3_cfg = config["s3"]

    input_dir = study["input_dir"]
    output_dir = study["output_dir"]
    TR = study["TR"]
    force_recompute = study.get("force_recompute", False)
    ses_label = f"ses-{session}A"
    session_start_time = time.time()

    session_out = os.path.join(output_dir, f"sub-{sub_id}", ses_label)
    preproc_dir = os.path.join(session_out, "preproc")
    qc_dir = os.path.join(session_out, "qc")
    qc_preproc_dir = os.path.join(qc_dir, "preproc")
    fl_out_dir = os.path.join(session_out, "first_level_out")

    downloaded_paths = []
    preproc_qc_by_run = {}
    preproc_excluded_runs = []
    fl_file_paths = []

    try:
        routing_result = determine_session_routing(
            s3_cfg, sub_id, session, force_recompute, logger
        )
        routing = routing_result["routing"]
        remote_metadata = routing_result["remote_metadata"]

        if routing == "skip":
            logger.info(
                "Session %s for sub-%s already complete on S3 "
                "(sentinel last_modified=%s); skipping.",
                ses_label, sub_id,
                remote_metadata.get("sentinel_last_modified"),
            )
            return {
                "routing": "skip",
                "analysis_outcomes": [],
                "qc_path": None,
                "remote_metadata": remote_metadata,
                "exit_category": EXIT_COMPLETE,
                "preproc_excluded_runs": [],
            }

        # Full path: recompute from an empty local session output directory
        if os.path.isdir(session_out):
            shutil.rmtree(session_out, ignore_errors=True)
            logger.info("Wiped pre-existing session output directory: %s", session_out)

        for d in [session_out, preproc_dir, qc_dir, qc_preproc_dir, fl_out_dir]:
            os.makedirs(d, exist_ok=True)

        if discovered_session is None:
            disc_all = discover_subject_inputs(
                study, s3_cfg, sub_id, task_defs, logger, session_filter=session
            )
            if session not in disc_all:
                raise OrchestratorError(
                    f"Session '{session}' not found in discovery for sub-{sub_id}."
                )
            check_subject_input_completeness(
                {session: disc_all[session]}, analyses, proc_template, logger
            )
            discovered_session = disc_all[session]

        # Tasks whose analyses request tissue/GS regressors
        tmpl_by_name = {b["name"]: b for b in proc_template.get("analyses", [])}
        needs_csf = set()
        needs_wm = set()
        needs_gs = set()
        for a in analyses:
            block = tmpl_by_name.get(a["name"], {})
            paths = block.get("paths") or {}
            if paths.get("CSF_paths") is not None:
                needs_csf.add(a["task_label"])
            if paths.get("WM_paths") is not None:
                needs_wm.add(a["task_label"])
            if paths.get("GS_paths") is not None:
                needs_gs.add(a["task_label"])

        logger.info("Downloading session inputs from S3...")
        local_session, downloaded_paths = download_session_inputs(
            s3_cfg, input_dir, discovered_session, logger
        )

        # Per-task preprocessing
        processed_files = {}
        n_derivs = derive_calc_n_motion_derivs(analyses, proc_template)

        # Pre-exclude non-rest runs without events
        for task_def in task_defs:
            task_label = task_def["task_label"]
            is_rest = (task_label == "rest")
            runs = local_session.get(task_label, {})
            if is_rest:
                continue
            for run_int in sorted(runs):
                if runs[run_int].get("events") is None:
                    run_label = f"{ses_label}_task-{task_label}_run-{run_int}"
                    preproc_excluded_runs.append({
                        "run_label": run_label,
                        "task": task_label,
                        "reason": "events_missing",
                        "error_type": None,
                        "message": "no events file in discovery listing",
                    })
                    logger.warning(
                        "Run %s excluded: no events file in discovery listing", run_label
                    )

        excluded_keys = set()
        for excl in preproc_excluded_runs:
            rl = excl["run_label"]  # "ses-NNA_task-X_run-N"
            parts = rl.split("_task-")
            if len(parts) == 2:
                task_run = parts[1].split("_run-")
                if len(task_run) == 2:
                    excluded_keys.add((excl["task"], int(task_run[1])))

        for task_def in task_defs:
            task_label = task_def["task_label"]
            if task_label not in VALID_TASK_LABELS:
                raise OrchestratorError(
                    f"Unrecognized task label '{task_label}' for sub-{sub_id} "
                    f"{ses_label}. Valid labels for this orchestrator: "
                    f"{sorted(VALID_TASK_LABELS)}."
                )
            is_rest = (task_label == "rest")
            runs = local_session.get(task_label, {})
            if not runs:
                logger.warning(
                    "No runs discovered for task '%s' sub-%s %s; skipping task.",
                    task_label, sub_id, ses_label
                )
                continue

            logger.info("-" * 60)
            logger.info("Processing task: %s (%d runs)", task_label, len(runs))
            logger.info("-" * 60)

            task_needs_csf = task_label in needs_csf
            task_needs_wm = task_label in needs_wm
            task_needs_gs = task_label in needs_gs

            anat = local_session.get("anat", {})
            csf_mask = anat.get("csf_mask")
            wm_mask = anat.get("wm_mask")

            survivor_records = []
            skipped_runs = []

            for run_int in sorted(runs):
                if (task_label, run_int) in excluded_keys:
                    continue

                rd = runs[run_int]
                run_label = f"{ses_label}_task-{task_label}_run-{run_int}"
                file_prefix = f"sub-{sub_id}_{ses_label}_task-{task_label}_run-{run_int:02d}"

                try:
                    n_remove = detect_non_steady_state_trs(rd["confounds"], logger)

                    # Trim the producer BOLD as shipped (no orchestrator masking)
                    logger.info("Handling non-steady-state TRs for %s...", run_label)
                    trimmed_bold, n_trs = remove_initial_trs_bold(
                        rd["bold"], n_remove, preproc_dir, file_prefix, logger
                    )
                    if n_trs <= 0:
                        raise OrchestratorError(
                            f"Could not determine TR count for {run_label} "
                            f"(3dinfo returned {n_trs}). Cannot proceed without "
                            f"a valid TR count for onset offset computation."
                        )

                    if not skip_qc and qc_cfg.get("preproc", {}).get("enabled", False):
                        logger.info("Computing preprocessing QC for %s...", run_label)
                        run_info = {
                            "session": session, "task_label": task_label,
                            "run": run_int, "run_label": run_label,
                        }
                        qc_metrics = compute_preproc_qc(
                            run_info, rd["confounds"],
                            trimmed_bold, rd["brainmask"],
                            n_remove, qc_cfg.get("preproc", {}),
                            qc_preproc_dir, f"sub-{sub_id}", logger
                        )
                        preproc_qc_by_run[run_label] = qc_metrics

                    logger.info("Extracting motion regressors for %s...", run_label)
                    motion_out = os.path.join(preproc_dir, f"{file_prefix}_motion.1D")
                    motion_path, motion_units_check = extract_motion_regressors(
                        rd["confounds"], n_remove, n_derivs,
                        motion_out, logger,
                        expected_n_frames=(n_trs + n_remove)
                    )
                    preproc_qc_by_run.setdefault(run_label, {})["motion_units_check"] = motion_units_check

                    run_csf = run_wm = run_gs = None
                    if task_needs_csf and csf_mask:
                        csf_out = os.path.join(preproc_dir, f"{file_prefix}_csf.1D")
                        csf_path, csf_nvox = compute_tissue_mask_regressor(
                            trimmed_bold, rd["brainmask"], csf_mask,
                            csf_out, logger
                        )
                        run_csf = csf_path
                        preproc_qc_by_run.setdefault(run_label, {})["csf_mask_voxels"] = csf_nvox
                    if task_needs_wm and wm_mask:
                        wm_out = os.path.join(preproc_dir, f"{file_prefix}_wm.1D")
                        wm_path, wm_nvox = compute_tissue_mask_regressor(
                            trimmed_bold, rd["brainmask"], wm_mask,
                            wm_out, logger
                        )
                        run_wm = wm_path
                        preproc_qc_by_run.setdefault(run_label, {})["wm_mask_voxels"] = wm_nvox
                    if task_needs_gs:
                        gs_out = os.path.join(preproc_dir, f"{file_prefix}_gs.1D")
                        gs_path, gs_nvox = compute_tissue_mask_regressor(
                            trimmed_bold, rd["brainmask"], rd["brainmask"],
                            gs_out, logger
                        )
                        run_gs = gs_path
                        preproc_qc_by_run.setdefault(run_label, {})["gs_mask_voxels"] = gs_nvox

                    # Task timing (non-rest)
                    run_timing = None
                    run_conditions = None
                    if not is_rest:
                        logger.info("Formatting task timing for %s...", run_label)
                        condition_col = task_def.get("condition_column", "trial_type")

                        events_for_timing = rd["events"]
                        if task_def.get("fix_nback_cues", False):
                            fixed_events_out = os.path.join(
                                preproc_dir, f"{file_prefix}_events_fixed.tsv"
                            )
                            events_for_timing = fix_nback_cue_labels(
                                rd["events"], condition_col,
                                fixed_events_out, logger
                            )

                        timing_out = os.path.join(preproc_dir, f"{file_prefix}_timing.tsv")
                        timing_path, n_dropped, n_dropped_straddling = format_task_timing(
                            events_for_timing,
                            condition_col,
                            task_def.get("conditions_exclude"),
                            n_remove, TR, timing_out, logger
                        )

                        # Events validation
                        timing_result = validate_task_timing(
                            timing_path, n_trs, TR
                        )
                        run_timing = timing_path
                        run_conditions = timing_result["conditions_present"]

                        # Record event QC
                        rqc = preproc_qc_by_run.setdefault(run_label, {})
                        rqc["n_events_dropped_before_start"] = n_dropped
                        rqc["n_events_dropped_straddling"] = n_dropped_straddling
                        rqc["n_events_written"] = timing_result["n_events_written"]
                        rqc["conditions_present"] = sorted(timing_result["conditions_present"])

                    survivor_records.append({
                        "run": run_int,
                        "bold": trimmed_bold,
                        "motion": motion_path,
                        "timing": run_timing,
                        "csf": run_csf,
                        "wm": run_wm,
                        "gs": run_gs,
                        "conditions": run_conditions,
                    })

                except OrchestratorError as e:
                    reason = (e.context or {}).get("reason", "unexpected_error")
                    if reason == "producer_fd_inconsistent":
                        preproc_qc_by_run.setdefault(run_label, {})["motion_units_check"] = reason
                    preproc_excluded_runs.append({
                        "run_label": run_label,
                        "task": task_label,
                        "reason": reason,
                        "error_type": "OrchestratorError",
                        "message": str(e),
                    })
                    logger.warning(
                        "Skipping run '%s' for task '%s': %s (reason: %s)",
                        run_label, task_label, str(e), reason
                    )
                    skipped_runs.append(run_label)
                    continue
                except Exception as e:
                    preproc_excluded_runs.append({
                        "run_label": run_label,
                        "task": task_label,
                        "reason": "unexpected_error",
                        "error_type": type(e).__name__,
                        "message": str(e),
                    })
                    logger.warning(
                        "Skipping run '%s' for task '%s': unexpected error: %s",
                        run_label, task_label, str(e),
                        exc_info=True
                    )
                    skipped_runs.append(run_label)
                    continue

            if skipped_runs:
                n_surviving = len(runs) - len(skipped_runs) - sum(
                    1 for ek in excluded_keys if ek[0] == task_label
                )
                logger.warning(
                    "Task '%s' sub-%s %s: %d of %d run(s) failed preprocessing "
                    "(%d surviving). Skipped: %s. Downstream analyses will use "
                    "only surviving run(s).",
                    task_label, sub_id, ses_label,
                    len(skipped_runs), len(runs), n_surviving,
                    ", ".join(skipped_runs)
                )

            if not survivor_records:
                logger.warning(
                    "No runs successfully processed for task '%s' sub-%s %s; skipping task.",
                    task_label, sub_id, ses_label
                )
                continue

            # Collect per-run files from the survivor record list
            pf = {
                "run_labels": [r["run"] for r in survivor_records],
                "bolds": [r["bold"] for r in survivor_records],
                "motions": [r["motion"] for r in survivor_records],
            }
            if not is_rest:
                pf["timings"] = [r["timing"] for r in survivor_records]
                pf["conditions_by_run"] = [r["conditions"] for r in survivor_records]
            pf["csf"] = [r["csf"] for r in survivor_records] if task_needs_csf else None
            pf["wm"] = [r["wm"] for r in survivor_records] if task_needs_wm else None
            pf["gs"] = [r["gs"] for r in survivor_records] if task_needs_gs else None

            processed_files[task_label] = pf
            logger.info(
                "Collecting per-run files for task '%s' (%d run(s)).",
                task_label, len(survivor_records)
            )

        # Build the first-level config and run analyses
        analysis_outcomes = []

        if not skip_first_level:
            attach_afni_version_filter(logger)

            logger.info("=" * 60)
            logger.info("Building first-level config and running analyses...")
            logger.info("=" * 60)

            for orch_analysis in analyses:
                analysis_name = orch_analysis["name"]
                analysis_type = orch_analysis["type"]
                task_label = orch_analysis["task_label"]
                pf = processed_files.get(task_label)

                # No usable runs for this task: skip the analysis
                if pf is None:
                    outcome = {
                        "name": analysis_name,
                        "type": analysis_type,
                        "status": "skipped",
                        "reason": "no_usable_runs",
                        "upstream_status": None,
                        "qc_flagged": False,
                        "error": None,
                        "error_type": None,
                        "error_context": {},
                        "warnings": [],
                        "wall_time_seconds": 0.0,
                        "run_labels": [],
                        "terminal": True,
                    }
                    logger.info(
                        "Analysis '%s' skipped: no usable runs for task '%s'.",
                        analysis_name, task_label
                    )
                    analysis_outcomes.append(outcome)
                    continue

                # Per-analysis condition presence check (task types only)
                if analysis_type in ("task_act", "task_conn"):
                    tmpl_block = tmpl_by_name.get(analysis_name, {})
                    if analysis_type == "task_act":
                        required = tmpl_block.get("cond_labels", [])
                    else:
                        required = tmpl_block.get("cond_beta_labels", [])

                    conditions_by_run = pf.get("conditions_by_run", [])
                    missing = check_required_conditions_present(required, conditions_by_run)
                    if missing:
                        outcome = {
                            "name": analysis_name,
                            "type": analysis_type,
                            "status": "skipped",
                            "reason": "events_missing_conditions",
                            "upstream_status": None,
                            "qc_flagged": False,
                            "error": None,
                            "error_type": None,
                            "error_context": {"missing_conditions": missing},
                            "warnings": [],
                            "wall_time_seconds": 0.0,
                            "run_labels": pf["run_labels"],
                            "terminal": True,
                        }
                        logger.info(
                            "Analysis '%s' skipped: required conditions missing "
                            "from all surviving runs: %s",
                            analysis_name, missing
                        )
                        analysis_outcomes.append(outcome)
                        continue

            # Collect active (non-skipped) analyses
            skipped_names = {o["name"] for o in analysis_outcomes}
            active_analyses = [
                a for a in analyses
                if a["name"] not in skipped_names
                and a["task_label"] in processed_files
            ]

            if active_analyses:
                fl_config = build_first_level_config(
                    sub_id, session, study, processed_files,
                    active_analyses, proc_template, logger
                )

                config_path = write_temp_config(fl_config, session_out, sub_id, session, logger)

                configs = load_and_validate(config_path, logger)
                logger.info("First-level config validated: %d analysis block(s).", len(configs))

                for i, cfg in enumerate(configs):
                    atype = cfg.analysis_type
                    name = cfg.analysis_name
                    logger.info("-" * 60)
                    logger.info("Running analysis [%d]: %s (type: %s)", i, name, atype)
                    logger.info("-" * 60)

                    os.makedirs(cfg.out_dir, exist_ok=True)

                    block_start = time.time()
                    result = None
                    analysis_error = None
                    error_type = None
                    error_context = {}
                    try:
                        run_fn = DISPATCH[atype]
                        result = run_fn(cfg, logger)
                    except GuardError as e:
                        analysis_error = str(e)
                        error_type = type(e).__name__
                        error_context = json_safe_context(getattr(e, "context", None))
                        logger.critical("Analysis '%s' aborted by guard (%s): %s", name, error_type, analysis_error)
                    except (ConfigError, InputError, ModelError,
                            ToolUnavailableError, ToolVersionError, ToolExecutionError) as e:
                        analysis_error = str(e)
                        error_type = type(e).__name__
                        error_context = json_safe_context(getattr(e, "context", None))
                        logger.error("Analysis '%s' failed (%s): %s", name, error_type, analysis_error)
                    except Exception as e:
                        analysis_error = str(e)
                        error_type = type(e).__name__
                        error_context = json_safe_context(getattr(e, "context", None))
                        logger.exception("Analysis '%s' raised an unexpected error (%s): %s", name, error_type, analysis_error)

                    elapsed = time.time() - block_start
                    status = "failed" if analysis_error else "success"

                    # Collect first-level file paths for the upload set
                    pre = cfg.out_file_pre
                    if result is not None:
                        fl_file_paths.extend(result.output_files)
                        ledger_path = os.path.join(cfg.out_dir, upstream_names.ledger(pre))
                        if os.path.isfile(ledger_path):
                            fl_file_paths.append(ledger_path)
                    else:
                        for infra_fn in (upstream_names.timing_errors, upstream_names.qc_summary):
                            infra_path = os.path.join(cfg.out_dir, infra_fn(pre))
                            if os.path.isfile(infra_path):
                                fl_file_paths.append(infra_path)

                    # Filter AFNI-specific warnings from the upstream result
                    warnings = []
                    if result is not None:
                        warnings = filter_afni_unvalidated_warnings(list(result.warnings))

                    if analysis_error is None:
                        logger.info("Analysis '%s' completed in %.2f seconds.", name, elapsed)
                    else:
                        logger.warning(
                            "Analysis '%s' did not complete (%.2f s); "
                            "QC will reflect failure.",
                            name, elapsed
                        )

                    task_label_for_analysis = next(
                        (a["task_label"] for a in analyses if a["name"] == name), None
                    )
                    run_labels_for_analysis = []
                    if task_label_for_analysis and task_label_for_analysis in processed_files:
                        run_labels_for_analysis = processed_files[task_label_for_analysis]["run_labels"]

                    outcome = {
                        "name": name,
                        "type": atype,
                        "status": status,
                        "reason": None,
                        "upstream_status": result.status if result is not None else None,
                        "qc_flagged": (result is not None and result.status == "warning"),
                        "error": analysis_error,
                        "error_type": error_type,
                        "error_context": error_context,
                        "warnings": warnings,
                        "wall_time_seconds": round(elapsed, 2),
                        "run_labels": run_labels_for_analysis,
                        "terminal": is_outcome_terminal(status, None, error_type),
                    }

                    if not skip_qc and qc_cfg.get("first_level", {}).get("enabled", False):
                        if analysis_error is None:
                            try:
                                qc = build_first_level_qc_from_result(
                                    name, atype, f"sub-{sub_id}", result, cfg.out_dir, cfg.out_file_pre
                                )
                            except Exception as e:
                                logger.error("QC builder failed for analysis '%s': %s", name, e)
                                qc = {
                                    "analysis_name": name,
                                    "analysis_type": atype,
                                    "completed_successfully": False,
                                    "error": "QC builder failed",
                                }
                        else:
                            orch_a = next((a for a in active_analyses if a["name"] == name), None)
                            out_file_pre = (
                                f"sub-{sub_id}_{ses_label}_{orch_a['post_id_out_pre']}"
                                if orch_a else None
                            )
                            qc = compute_first_level_qc(
                                name, atype, fl_out_dir, f"sub-{sub_id}",
                                out_file_pre, logger, error_msg=analysis_error
                            )
                        qc["session"] = ses_label
                        outcome["qc"] = qc

                    analysis_outcomes.append(outcome)

        qc_path = None
        if not skip_qc and (preproc_qc_by_run or analysis_outcomes or preproc_excluded_runs):
            qc_session_status = _derive_session_status("full", analysis_outcomes)

            qc_json_path = os.path.join(
                session_out, "qc",
                f"sub-{sub_id}_{ses_label}_orchestrator_qc.json"
            )
            session_wall_time = time.time() - session_start_time
            consolidate_session_qc(
                sub_id, ses_label, qc_session_status, session_wall_time,
                preproc_qc_by_run, analysis_outcomes, preproc_excluded_runs,
                qc_json_path, logger,
                producer_manifest=local_session.get("_manifest"),
            )
            qc_path = qc_json_path

        exit_category = classify_session_exit(
            None, analysis_outcomes, preproc_excluded_runs, skip_first_level
        )
        logger.info(
            "Session %s exit category: %d (%s)",
            ses_label, exit_category, _EXIT_LABELS.get(exit_category, str(exit_category))
        )

        # No sentinel for unexplained failures, so the session is retried on the next run
        write_sentinel = (
            not skip_first_level
            and exit_category != EXIT_FAILED_UNEXPLAINED
        )
        logger.info("Uploading session outputs (per-file) to S3...")
        upload_result = upload_session_to_s3(
            s3_cfg, sub_id, session, session_out, logger,
            first_level_file_paths=fl_file_paths,
            write_sentinel=write_sentinel,
        )
        logger.info(
            "Upload complete: %d files uploaded, sentinel_written=%s, "
            "%d stale key(s) swept",
            upload_result["n_files_uploaded"],
            upload_result["sentinel_written"],
            len(upload_result["swept_keys"]),
        )

        if s3_cfg.get("cleanup_after_upload", True):
            logger.info("Cleaning up local files...")
            cleanup_local_inputs(downloaded_paths, logger)
            if os.path.isdir(session_out):
                shutil.rmtree(session_out, ignore_errors=True)
                logger.info("Removed session output directory: %s", session_out)

    except Exception:
        # Clean up downloaded files on error
        if s3_cfg.get("cleanup_after_upload", True):
            if downloaded_paths:
                logger.info("Cleaning up downloaded files after session failure...")
                cleanup_local_inputs(downloaded_paths, logger)
        raise

    logger.info("Session %s complete for sub-%s", ses_label, sub_id)
    return {
        "routing": "full",
        "analysis_outcomes": analysis_outcomes,
        "qc_path": qc_path,
        "remote_metadata": remote_metadata,
        "exit_category": exit_category,
        "preproc_excluded_runs": preproc_excluded_runs,
    }


def main():
    """Run startup checks, process one participant, and exit with its outcome code."""
    parser = argparse.ArgumentParser(
        description="Per-participant orchestrator for fMRI first-level processing (ABCD).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exit codes:
  0  All sessions complete (success, documented skip, documented run exclusion).
  1  At least one session has an unexplained failure (non-terminal outcome,
     unexpected_error or producer_fd_inconsistent run exclusion, or
     session-level exception). No sentinel.
  2  At least one session has a terminal ModelError failure (data-quality
     floor). Sentinel written; researcher review needed.

Parallel invocation (one process per participant):
  parallel -j 8 python orchestrate_first_level.py \\
    --orchestrate_config study.yaml --proc_config example_proc_config.yaml \\
    --subj_id {} --log-file logs/{}.log \\
    ::: SUBJ01 SUBJ02 SUBJ03
        """,
    )
    parser.add_argument(
        "--orchestrate_config", type=str, required=True,
        help="Path to the orchestrator YAML configuration file."
    )
    parser.add_argument(
        "--proc_config", type=str, required=True,
        help="Path to the fmri_first_level_proc YAML template configuration file."
    )
    parser.add_argument(
        "--subj_id", type=str, required=True,
        help="Participant ID (e.g. SUBJ01)."
    )
    parser.add_argument(
        "--session", type=str, default=None,
        help="Process only this session code (e.g. '00'). Useful for reprocessing "
             "a specific session that previously failed."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate config and print processing plan without executing."
    )
    parser.add_argument(
        "--log-file", type=str, default=None,
        help="Optional path to a log file."
    )
    parser.add_argument(
        "--skip-qc", action="store_true",
        help="Skip all QC computations."
    )
    parser.add_argument(
        "--skip-first-level", action="store_true",
        help="Run preprocessing only, skip first-level analyses."
    )

    args = parser.parse_args()

    logger = setup_logging("orchestrate_first_level", log_file=args.log_file)
    attach_afni_version_filter(logger)

    start_time = time.time()
    logger.info("=" * 60)
    logger.info("fMRI First-Level Orchestrator (ABCD Session-Centric)")
    logger.info("Participant: %s", args.subj_id)
    logger.info("Orchestrate config: %s", args.orchestrate_config)
    logger.info("Proc config: %s", args.proc_config)
    if args.session:
        logger.info("Session filter: %s", args.session)
    logger.info("=" * 60)

    try:
        # Verify upstream package version (runs under --dry-run as well)
        verify_upstream_version(logger)
        if _UPSTREAM_IMPORT_ERROR is not None:
            raise OrchestratorError(
                f"Failed to import fmri_first_level_proc >= {MIN_UPSTREAM_VERSION} "
                f"modules: {_UPSTREAM_IMPORT_ERROR}"
            )

        # Verify required AFNI binaries and log provenance (not under --dry-run)
        if not args.dry_run:
            try:
                preflight_tool_environments(logger=logger)
            except ToolUnavailableError as e:
                raise OrchestratorError(f"AFNI preflight failed: {e}")

            # Log installed vs validated AFNI version and the warning-suppression policy
            try:
                versions = runtime_check(logger)
                lock = load_lockfile()
                afni_validated = lock["tools"]["afni"]["validated"]
                afni_minimum = lock["tools"]["afni"]["minimum"]
                logger.info(
                    "AFNI provenance: installed %s, validated %s, minimum %s; "
                    "'[UNVALIDATED_VERSION] AFNI' warnings are suppressed by "
                    "orchestrator policy (AFNI >= minimum accepted)",
                    versions.get("afni_version", "unknown"),
                    afni_validated, afni_minimum
                )
            except ToolVersionError as e:
                raise OrchestratorError(f"AFNI version check failed: {e}")

        config = load_orchestrator_config(args.orchestrate_config, logger)

        if not os.path.isfile(args.proc_config):
            raise OrchestratorError(f"Proc config not found: {args.proc_config}")
        with open(args.proc_config) as f:
            try:
                proc_template = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise OrchestratorError(f"YAML parse error in proc config: {e}")

        validate_proc_template(config, proc_template, logger)

        code = process_participant(
            config, args.subj_id, proc_template,
            skip_qc=args.skip_qc,
            skip_first_level=args.skip_first_level,
            dry_run=args.dry_run,
            logger=logger,
            session_filter=args.session,
        )

    except OrchestratorError as e:
        logger.error("Fatal: %s", e)
        sys.exit(1)

    logger.info("Total runtime: %.2f seconds", time.time() - start_time)

    if code != EXIT_COMPLETE:
        sys.exit(code)

if __name__ == "__main__":
    main()
