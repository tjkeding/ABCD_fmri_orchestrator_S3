#!/usr/bin/env python3

# ============================================================================
# PER-PARTICIPANT ORCHESTRATOR FOR fMRI FIRST-LEVEL PROCESSING (ABCD)
#
# Session-centric pipeline: processes one session at a time through the full
# lifecycle (download -> preprocess -> analyze -> compress -> upload -> cleanup)
# to minimize EC2 disk usage across ~11,000 ABCD subjects.
#
# Bridges fMRIPrep BIDS derivatives (from S3 archives) and the first-level
# analysis scripts. Handles: S3 data transfer, archive extraction, file
# discovery, brain masking, QC, non-steady-state TR removal, confounds
# extraction, run concatenation, optional smoothing, dynamic first-level
# config building, output compression, and S3 upload.
#
# Designed to be called in parallel (one invocation per participant):
#   parallel -j 8 python orchestrate_first_level.py \
#     --orchestrate_config study.yaml --proc_config example_config.yaml \
#     --subj_id {} --log-file logs/{}.log \
#     ::: NDARABC123 NDARDEF456 NDARGHI789
#
# Author: Taylor J. Keding, Ph.D.
# Version: 3.1
# Last updated: 03/16/26
# ============================================================================

import os
import sys
import time
import shutil
import argparse
from collections import defaultdict

import yaml
import numpy as np

from fmri_first_level_proc.first_level_utils import setup_logging
from fmri_first_level_proc.first_level_config import load_and_validate
from fmri_first_level_proc.run_first_level import DISPATCH

__version__ = "3.1"

from orchestrator_utils import (
    OrchestratorError,
    VALID_TASK_LABELS,
    discover_available_sessions,
    download_session_data,
    discover_local_mmps_files,
    extract_session_archive,
    upload_to_s3,
    compress_session_outputs,
    cleanup_local_inputs,
    verify_afni_installation,
    load_orchestrator_config,
    validate_proc_template,
    discover_session_files,
    decompress_if_needed,
    apply_brain_mask,
    detect_non_steady_state_trs,
    remove_initial_trs_bold,
    remove_initial_trs_tabular,
    extract_motion_regressors,
    extract_tissue_signals,
    fix_nback_cue_labels,
    format_task_timing,
    concatenate_bolds,
    concatenate_tabular_files,
    concatenate_task_timing,
    apply_smoothing,
    compute_mask_intersection,
    compute_preproc_qc,
    consolidate_session_qc,
    build_first_level_config,
    write_temp_config,
    compute_first_level_qc,
)


def _derive_session_status(analysis_outcomes):
    """
    Derive a qualified session status from per-analysis outcome dicts.

    Parameters
    ----------
    analysis_outcomes : list of dict
        Each dict has at minimum a 'status' key with value "success" or "failed".

    Returns
    -------
    str
        "success"  — all analyses succeeded
        "partial"  — at least one analysis succeeded
        "failed"   — no analyses ran or all analyses failed
    """
    if not analysis_outcomes:
        return "failed"
    elif all(o["status"] == "success" for o in analysis_outcomes):
        return "success"
    elif any(o["status"] == "success" for o in analysis_outcomes):
        return "partial"
    else:
        return "failed"


def process_participant(config, sub_id, proc_template, skip_qc, skip_first_level, dry_run, logger, session_filter=None):
    """
    Run the full preprocessing + first-level pipeline for one participant.

    Processing is session-centric: each session is fully processed (download,
    preprocess, analyze, compress, upload, cleanup) before moving to the next,
    minimizing disk usage on EC2.

    Parameters
    ----------
    config : dict
        Validated orchestrator config.
    sub_id : str
        Participant ID (e.g. "NDARABC123").
    proc_template : dict
        The fmri_first_level_proc config template.
    skip_qc : bool
        If True, skip all QC computations.
    skip_first_level : bool
        If True, run preprocessing only.
    dry_run : bool
        If True, validate and print plan without executing.
    logger : logging.Logger
    session_filter : str or None
        If set, only process this session code (e.g. "00"). Used for
        reprocessing a specific session that previously failed.
    """
    study = config["study"]
    task_defs = config["tasks"]
    analyses = config["analyses"]
    smoothing_cfg = config.get("smoothing", {"enabled": False})
    qc_cfg = config.get("qc", {"preproc": {"enabled": False}, "first_level": {"enabled": False}})
    s3_cfg = config.get("s3", {"enabled": False})

    fmriprep_dir = study["fmriprep_dir"]
    output_dir = study["output_dir"]
    space = study["space"]
    TR = study["TR"]

    # ================================================================
    # Step 0: Discover available sessions
    # ================================================================
    if s3_cfg.get("enabled", False):
        logger.info("Step 0: Discovering available sessions on S3...")
        sessions = discover_available_sessions(s3_cfg, sub_id, logger)
        if dry_run:
            logger.info("[DRY RUN] Found sessions on S3: %s", sessions)
    else:
        # Local mode: use available_sessions from config as-is
        sessions = s3_cfg.get("available_sessions", ["00"])
        logger.info("S3 disabled — using session list: %s", sessions)

    # Apply session filter if specified
    if session_filter is not None:
        if session_filter in sessions:
            sessions = [session_filter]
            logger.info("Session filter applied — processing only session: %s", session_filter)
        else:
            raise OrchestratorError(
                f"Session filter '{session_filter}' not found in available sessions "
                f"for sub-{sub_id}: {sessions}"
            )

    if dry_run:
        logger.info("[DRY RUN] Sessions to process: %s", sessions)
        for session in sessions:
            logger.info("[DRY RUN] Session %s:", session)
            for task_def in task_defs:
                logger.info("[DRY RUN]   Task: %s", task_def["task_label"])
            for a in analyses:
                logger.info(
                    "[DRY RUN]   Analysis: %s (type: %s, task: %s, fd: %.2f)",
                    a["name"], a["type"], a["task_label"], a["fd_threshold"]
                )
        return

    # ================================================================
    # Process each session
    # ================================================================
    session_results = {}

    for session in sessions:
        ses_label = f"ses-{session}A"
        logger.info("=" * 70)
        logger.info("PROCESSING SESSION: sub-%s / %s", sub_id, ses_label)
        logger.info("=" * 70)

        session_start = time.time()
        session_error = None

        try:
            analysis_outcomes = _process_session(
                config, sub_id, session, proc_template,
                skip_qc, skip_first_level, logger
            )

            # Derive qualified session status from per-analysis outcomes
            session_status = _derive_session_status(analysis_outcomes)

            session_results[session] = {
                "status": session_status,
                "analyses": analysis_outcomes,
            }

        except OrchestratorError as e:
            session_error = str(e)
            logger.error(
                "Session %s failed for sub-%s: %s", ses_label, sub_id, session_error
            )
            session_results[session] = {
                "status": "failed",
                "analyses": [],
                "error": session_error,
            }

        except Exception as e:
            session_error = str(e)
            logger.error(
                "Unexpected error in session %s for sub-%s: %s",
                ses_label, sub_id, session_error,
                exc_info=True
            )
            session_results[session] = {
                "status": "failed",
                "analyses": [],
                "error": session_error,
            }

        elapsed = time.time() - session_start
        ses_status = session_results[session]["status"].upper()
        logger.info(
            "Session %s %s (%.2f seconds)", ses_label, ses_status, elapsed
        )

    # ================================================================
    # Summary across all sessions
    # ================================================================
    logger.info("=" * 70)
    logger.info("SESSION SUMMARY for sub-%s:", sub_id)
    n_success = 0
    n_partial = 0
    n_failed = 0
    for ses, result in session_results.items():
        ses_status = result["status"]
        logger.info("  ses-%sA: %s", ses, ses_status)

        # Log per-analysis breakdown
        for outcome in result.get("analyses", []):
            if outcome["status"] == "success":
                logger.info("    [OK]     %s (%.2fs)", outcome["name"], outcome["wall_time_seconds"])
            else:
                logger.info("    [FAILED] %s: %s", outcome["name"], outcome["error"])
        if result.get("error"):
            logger.info("    Session-level error: %s", result["error"])

        if ses_status == "success":
            n_success += 1
        elif ses_status == "partial":
            n_partial += 1
        else:
            n_failed += 1
    logger.info(
        "Total: %d success, %d partial, %d failed out of %d session(s)",
        n_success, n_partial, n_failed, len(session_results)
    )
    logger.info("=" * 70)

    if n_success == 0 and n_partial == 0 and n_failed > 0:
        raise OrchestratorError(
            f"All {n_failed} session(s) failed for sub-{sub_id}. "
            f"Check log for details."
        )


def _process_session(config, sub_id, session, proc_template, skip_qc, skip_first_level, logger):
    """
    Process a single session through the full pipeline.

    This is the core session-centric workflow:
    1. Download session data from S3 (archive + events)
    2. Extract archive
    3. Discover files per task
    4. Per-task preprocessing (mask, QC, NSS, motion, tissue, timing)
    5. Concatenate or collect runs
    6. Build first-level config
    7. Run first-level analyses
    8. Compress session outputs
    9. Upload to S3
    10. Cleanup local files
    """
    study = config["study"]
    task_defs = config["tasks"]
    analyses = config["analyses"]
    smoothing_cfg = config.get("smoothing", {"enabled": False})
    qc_cfg = config.get("qc", {"preproc": {"enabled": False}, "first_level": {"enabled": False}})
    s3_cfg = config.get("s3", {"enabled": False})

    fmriprep_dir = study["fmriprep_dir"]
    output_dir = study["output_dir"]
    space = study["space"]
    TR = study["TR"]
    force_recompute = study.get("force_recompute", False)
    ses_label = f"ses-{session}A"
    session_start_time = time.time()

    # Session output directories
    session_out = os.path.join(output_dir, f"sub-{sub_id}", ses_label)
    preproc_dir = os.path.join(session_out, "preproc")
    concat_dir = os.path.join(session_out, "concat")
    qc_dir = os.path.join(session_out, "qc")
    qc_preproc_dir = os.path.join(qc_dir, "preproc")
    fl_out_dir = os.path.join(session_out, "first_level_out")

    for d in [session_out, preproc_dir, concat_dir, qc_dir, qc_preproc_dir, fl_out_dir]:
        os.makedirs(d, exist_ok=True)

    downloaded_paths = []
    extracted_dir = None
    preproc_qc_by_run = {}

    try:
        # ============================================================
        # Step 1: Download session data from S3
        # ============================================================
        events_files = {}
        motion_files = {}
        if s3_cfg.get("enabled", False):
            logger.info("Step 1: Downloading session data from S3...")
            download_result = download_session_data(
                s3_cfg, sub_id, session, task_defs, fmriprep_dir, logger
            )
            downloaded_paths = download_result["all_downloaded_paths"]
            events_files = download_result["events_files"]
            motion_files = download_result["motion_files"]

            # Step 2: Extract archive
            logger.info("Step 2: Extracting fMRIPrep archive...")
            archive_path = download_result["archive_path"]
            extract_target = os.path.join(fmriprep_dir, f"sub-{sub_id}", ses_label, "extracted")
            extracted_dir = extract_session_archive(archive_path, extract_target, logger)
        else:
            # Local mode: files already on disk (from prior S3 download)
            extracted_dir = os.path.join(fmriprep_dir, f"sub-{sub_id}", ses_label)
            logger.info("S3 disabled — using local files at: %s", extracted_dir)
            try:
                local_mmps = discover_local_mmps_files(
                    fmriprep_dir, sub_id, session, task_defs, logger
                )
            except FileNotFoundError as e:
                raise OrchestratorError(str(e))
            events_files = local_mmps["events_files"]
            motion_files = local_mmps["motion_files"]

        # ============================================================
        # Step 3: Discover files per task
        # ============================================================
        logger.info("Step 3: Discovering files for all tasks...")
        discovered = discover_session_files(
            extracted_dir, sub_id, session, task_defs, events_files, motion_files, space, logger
        )

        anat_mask_path = discovered.pop("_anat_mask", None)

        if not discovered:
            raise OrchestratorError(
                f"No task files found for sub-{sub_id} {ses_label}. "
                f"Check that the archive contains expected BOLD files."
            )

        # ============================================================
        # Steps 4-10: Per-task preprocessing
        # ============================================================
        processed_files = {}

        # Collect unique FD thresholds per task for preproc QC carpet plot
        # threshold selection (censoring itself is now handled by upstream)
        task_fd_thresholds = defaultdict(set)
        for a in analyses:
            task_fd_thresholds[a["task_label"]].add(round(a["fd_threshold"], 4))

        for task_def in task_defs:
            task_label = task_def["task_label"]
            if task_label not in VALID_TASK_LABELS:
                raise OrchestratorError(
                    f"Unrecognized task label '{task_label}' for sub-{sub_id} "
                    f"{ses_label}. Valid labels for this orchestrator: "
                    f"{sorted(VALID_TASK_LABELS)}. Check the 'tasks' section "
                    f"of the orchestrator config."
                )
            is_rest = (task_label == "rest")
            should_concat = task_def.get("concatenate_runs", not is_rest)

            run_dicts = discovered.get(task_label)
            if not run_dicts:
                logger.warning(
                    "No runs discovered for task '%s' sub-%s %s — skipping task.",
                    task_label, sub_id, ses_label
                )
                continue

            logger.info("-" * 60)
            logger.info("Processing task: %s (%d runs)", task_label, len(run_dicts))
            logger.info("-" * 60)

            fd_thresholds_for_task = task_fd_thresholds.get(task_label, set())

            per_run_bolds = []
            per_run_motions = []
            per_run_timings = []
            per_run_csf = []
            per_run_wm = []
            per_run_gs = []
            per_run_tr_counts = []
            per_run_timing_tr_counts = []  # TR counts aligned with per_run_timings
            per_run_masks = []  # masks from successfully processed runs
            per_run_labels = []  # run labels from successfully processed runs
            skipped_runs = []

            for rd in run_dicts:
                run_label = rd["run_label"]
                run_prefix = f"sub-{sub_id}_{run_label}"

                try:
                    # Step 4: Decompress if needed
                    rd["bold_path"] = decompress_if_needed(rd["bold_path"], logger)
                    rd["confounds_path"] = decompress_if_needed(rd["confounds_path"], logger)
                    rd["mask_path"] = decompress_if_needed(rd["mask_path"], logger)
                    rd["motion_tsv_path"] = decompress_if_needed(rd["motion_tsv_path"], logger)

                    # Detect non-steady-state TRs once; reused for QC (Step 6) and trimming (Step 7)
                    n_remove = detect_non_steady_state_trs(rd["confounds_path"], logger)

                    # Step 5: Brain mask
                    logger.info("Step 5: Applying brain mask for %s...", run_label)
                    masked_bold = apply_brain_mask(
                        rd["bold_path"], rd["mask_path"],
                        preproc_dir, run_prefix, logger,
                        force_recompute=force_recompute
                    )

                    # Step 6: Preprocessing QC
                    if not skip_qc and qc_cfg.get("preproc", {}).get("enabled", False):
                        logger.info("Step 6: Computing preprocessing QC for %s...", run_label)
                        qc_metrics = compute_preproc_qc(
                            rd, rd["confounds_path"],
                            masked_bold, rd["mask_path"],
                            n_remove, qc_cfg.get("preproc", {}),
                            qc_preproc_dir, f"sub-{sub_id}", space, logger,
                            anat_mask_path=anat_mask_path
                        )
                        preproc_qc_by_run[run_label] = qc_metrics

                    # Step 7: Remove non-steady-state TRs
                    logger.info("Step 7: Handling non-steady-state TRs for %s...", run_label)
                    trimmed_bold, n_trs = remove_initial_trs_bold(
                        masked_bold, n_remove, preproc_dir, run_prefix, logger,
                        force_recompute=force_recompute
                    )
                    if n_trs <= 0:
                        raise OrchestratorError(
                            f"Could not determine TR count for {run_label} "
                            f"(3dinfo returned {n_trs}). Cannot proceed without "
                            f"a valid TR count for onset offset computation."
                        )
                    per_run_tr_counts.append(n_trs)

                    # Step 8: Extract motion regressors
                    logger.info("Step 8: Extracting motion regressors for %s...", run_label)
                    motion_out = os.path.join(preproc_dir, f"{run_prefix}_motion.1D")
                    motion_path, rot_unit_ambiguous = extract_motion_regressors(
                        rd["motion_tsv_path"], n_remove,
                        study.get("calc_n_motion_derivs", 1),
                        motion_out, logger,
                        force_recompute=force_recompute
                    )
                    per_run_motions.append(motion_path)
                    # Inject rotation unit flag into preproc QC for this run.
                    # Uses setdefault so the flag is recorded whether or not
                    # preproc QC (Step 6) is enabled.
                    preproc_qc_by_run.setdefault(run_label, {})["rotation_unit_ambiguous"] = rot_unit_ambiguous

                    # Step 9: Extract tissue signals (rest only)
                    if is_rest:
                        logger.info("Step 9: Extracting tissue signals for %s...", run_label)
                        csf_out = os.path.join(preproc_dir, f"{run_prefix}_csf.1D")
                        csf_path = extract_tissue_signals(
                            rd["confounds_path"], n_remove, "csf", csf_out, logger,
                            force_recompute=force_recompute
                        )
                        per_run_csf.append(csf_path)

                        wm_out = os.path.join(preproc_dir, f"{run_prefix}_wm.1D")
                        wm_path = extract_tissue_signals(
                            rd["confounds_path"], n_remove, "white_matter", wm_out, logger,
                            force_recompute=force_recompute
                        )
                        per_run_wm.append(wm_path)

                        gs_out = os.path.join(preproc_dir, f"{run_prefix}_gs.1D")
                        gs_path = extract_tissue_signals(
                            rd["confounds_path"], n_remove, "global_signal", gs_out, logger,
                            force_recompute=force_recompute
                        )
                        per_run_gs.append(gs_path)

                    # Step 10: Format task timing (task only)
                    if not is_rest and rd["events_path"] is not None:
                        logger.info("Step 10: Formatting task timing for %s...", run_label)
                        condition_col = task_def.get("condition_column", "trial_type")

                        # For n-back tasks, relabel generic "cue" entries with
                        # the stimulus condition inferred from subsequent trials
                        events_for_timing = rd["events_path"]
                        if task_def.get("fix_nback_cues", False):
                            fixed_events_out = os.path.join(
                                preproc_dir, f"{run_prefix}_events_fixed.tsv"
                            )
                            events_for_timing = fix_nback_cue_labels(
                                rd["events_path"], condition_col,
                                fixed_events_out, logger,
                                force_recompute=force_recompute
                            )

                        timing_out = os.path.join(preproc_dir, f"{run_prefix}_timing.csv")
                        timing_path, n_dropped = format_task_timing(
                            events_for_timing,
                            condition_col,
                            task_def.get("conditions_exclude"),
                            n_remove, TR, timing_out, logger,
                            force_recompute=force_recompute
                        )
                        per_run_timings.append(timing_path)
                        per_run_timing_tr_counts.append(n_trs)

                    per_run_bolds.append(trimmed_bold)
                    per_run_masks.append(rd["mask_path"])
                    per_run_labels.append(run_label)

                except FileNotFoundError as e:
                    logger.warning(
                        "Skipping run '%s' for task '%s' — missing file: %s",
                        run_label, task_label, str(e)
                    )
                    skipped_runs.append(run_label)
                    continue
                except Exception as e:
                    logger.warning(
                        "Skipping run '%s' for task '%s' — unexpected error: %s",
                        run_label, task_label, str(e)
                    )
                    skipped_runs.append(run_label)
                    continue

            if skipped_runs:
                n_surviving = len(run_dicts) - len(skipped_runs)
                logger.warning(
                    "Task '%s' sub-%s %s: %d of %d run(s) failed preprocessing "
                    "(%d surviving). Skipped: %s. Downstream analyses will use "
                    "only surviving run(s).",
                    task_label, sub_id, ses_label,
                    len(skipped_runs), len(run_dicts), n_surviving,
                    ", ".join(skipped_runs)
                )

            if not per_run_bolds:
                logger.warning(
                    "No runs successfully processed for task '%s' sub-%s %s — skipping task.",
                    task_label, sub_id, ses_label
                )
                continue

            # ============================================================
            # Step 11: Concatenate runs or collect per-run files
            # ============================================================
            task_prefix = f"sub-{sub_id}_{ses_label}_task-{task_label}"

            if should_concat and len(per_run_bolds) > 0:
                logger.info("Step 11: Concatenating runs for task '%s'...", task_label)

                concat_bold = concatenate_bolds(
                    per_run_bolds,
                    os.path.join(concat_dir, f"{task_prefix}_concat_bold.nii.gz"),
                    logger,
                    force_recompute=force_recompute
                )
                concat_motion = concatenate_tabular_files(
                    per_run_motions,
                    os.path.join(concat_dir, f"{task_prefix}_concat_motion.1D"),
                    logger,
                    force_recompute=force_recompute
                )

                # Compute intersection mask across all surviving runs
                concat_mask_path = os.path.join(
                    concat_dir, f"{task_prefix}_concat_mask.nii.gz"
                )
                concat_mask = compute_mask_intersection(
                    per_run_masks, concat_mask_path, logger,
                    force_recompute=force_recompute
                )

                concat_timing = None
                if per_run_timings:
                    concat_timing = concatenate_task_timing(
                        per_run_timings, per_run_timing_tr_counts, TR,
                        os.path.join(concat_dir, f"{task_prefix}_concat_timing.csv"),
                        logger,
                        force_recompute=force_recompute
                    )

                # Apply smoothing if enabled
                if smoothing_cfg.get("enabled", False):
                    logger.info("Applying smoothing to concatenated BOLD...")
                    concat_bold = apply_smoothing(
                        concat_bold, concat_mask,
                        smoothing_cfg["method"], smoothing_cfg["fwhm"],
                        concat_dir, f"{task_prefix}_concat", logger,
                        force_recompute=force_recompute
                    )

                processed_files[task_label] = {
                    "bold": concat_bold,
                    "motion": concat_motion,
                    "timing": concat_timing,
                }

            else:
                # Rest: keep per-run files
                logger.info("Step 11: Collecting per-run files for task '%s'.", task_label)

                # Apply smoothing per-run if enabled
                if smoothing_cfg.get("enabled", False):
                    logger.info("Applying smoothing to per-run BOLDs...")
                    smoothed_bolds = []
                    for i, bold in enumerate(per_run_bolds):
                        smoothed = apply_smoothing(
                            bold, per_run_masks[i],
                            smoothing_cfg["method"], smoothing_cfg["fwhm"],
                            preproc_dir, f"sub-{sub_id}_{per_run_labels[i]}", logger,
                            force_recompute=force_recompute
                        )
                        smoothed_bolds.append(smoothed)
                    per_run_bolds = smoothed_bolds

                processed_files[task_label] = {
                    "bolds": per_run_bolds,
                    "motions": per_run_motions,
                    "csf": per_run_csf,
                    "wm": per_run_wm,
                    "gs": per_run_gs if per_run_gs else None,
                }

        # Guard: if no tasks produced usable runs, raise an error
        if not processed_files:
            raise OrchestratorError(
                f"No tasks produced usable runs for sub-{sub_id} {ses_label}. "
                f"All tasks were skipped during preprocessing."
            )

        # ============================================================
        # Step 12: Build config & run first-level analyses
        # ============================================================
        analysis_outcomes = []

        if not skip_first_level and processed_files:
            logger.info("=" * 60)
            logger.info("Step 12: Building first-level config and running analyses...")
            logger.info("=" * 60)

            # Filter analyses to only those whose task was successfully processed
            active_analyses = [
                a for a in analyses
                if a["task_label"] in processed_files
            ]

            if not active_analyses:
                logger.warning(
                    "No analyses to run for sub-%s %s — no analyses match the processed tasks.",
                    sub_id, ses_label
                )
            else:
                fl_config = build_first_level_config(
                    sub_id, session, study, task_defs, processed_files,
                    active_analyses, proc_template, logger
                )

                config_path = write_temp_config(fl_config, session_out, sub_id, session, logger)

                # Change to session output dir to avoid 3dDeconvolve.err collision
                original_dir = os.getcwd()
                os.chdir(session_out)

                try:
                    analysis_list = load_and_validate(config_path, logger)
                    logger.info("First-level config validated: %d analysis block(s).", len(analysis_list))

                    for i, (atype, ns, name) in enumerate(analysis_list):
                        logger.info("-" * 60)
                        logger.info("Running analysis [%d]: %s (type: %s)", i, name, atype)
                        logger.info("-" * 60)

                        os.makedirs(ns.out_dir, exist_ok=True)

                        run_fn = DISPATCH[atype]
                        block_start = time.time()
                        analysis_error = None
                        try:
                            run_fn(ns, logger)
                        except SystemExit as e:
                            if e.code != 0:
                                analysis_error = f"AFNI exited with code {e.code}"
                                logger.error("Analysis '%s' failed — %s", name, analysis_error)
                        except Exception as e:
                            analysis_error = str(e)
                            logger.error("Analysis '%s' raised an error: %s", name, analysis_error)

                        elapsed = time.time() - block_start
                        if analysis_error is None:
                            logger.info("Analysis '%s' completed in %.2f seconds.", name, elapsed)
                        else:
                            logger.warning(
                                "Analysis '%s' did not complete (%.2f s); "
                                "QC will reflect failure.",
                                name, elapsed
                            )

                        # Build per-analysis outcome dict
                        outcome = {
                            "name": name,
                            "type": atype,
                            "status": "failed" if analysis_error else "success",
                            "error": analysis_error,
                            "wall_time_seconds": round(elapsed, 2),
                        }

                        # First-level QC (reads upstream QC JSON from fmri_first_level_proc)
                        if not skip_qc and qc_cfg.get("first_level", {}).get("enabled", False):
                            orch_a = next((a for a in active_analyses if a["name"] == name), None)
                            if orch_a:
                                out_file_pre = f"sub-{sub_id}_{ses_label}_{orch_a['post_id_out_pre']}"
                            else:
                                out_file_pre = None

                            fl_qc = compute_first_level_qc(
                                name, atype, fl_out_dir, f"sub-{sub_id}",
                                out_file_pre, logger, error_msg=analysis_error
                            )
                            fl_qc["session"] = ses_label
                            outcome["fl_qc"] = fl_qc

                        analysis_outcomes.append(outcome)
                finally:
                    os.chdir(original_dir)

        # ============================================================
        # Step 12b: Consolidated session QC JSON
        # ============================================================
        if not skip_qc and (preproc_qc_by_run or analysis_outcomes):
            # Derive session status for QC JSON
            qc_session_status = _derive_session_status(analysis_outcomes)

            qc_json_path = os.path.join(
                session_out, "qc",
                f"sub-{sub_id}_{ses_label}_orchestrator_qc.json"
            )
            session_wall_time = time.time() - session_start_time
            consolidate_session_qc(
                sub_id, ses_label, qc_session_status, session_wall_time,
                preproc_qc_by_run, analysis_outcomes, qc_json_path, logger
            )

        # ============================================================
        # Step 13: Compress, upload, cleanup
        # ============================================================
        if s3_cfg.get("enabled", False) and processed_files:
            logger.info("Step 13a: Compressing session outputs...")
            archive_path = compress_session_outputs(sub_id, session, session_out, logger)

            logger.info("Step 13b: Uploading results archive to S3...")
            upload_to_s3(s3_cfg, sub_id, session, archive_path, logger)

            if s3_cfg.get("cleanup_after_upload", True):
                logger.info("Step 13c: Cleaning up local files...")
                cleanup_local_inputs(downloaded_paths, logger)
                # Clean up extracted fmriprep directory
                if extracted_dir and os.path.isdir(extracted_dir):
                    shutil.rmtree(extracted_dir, ignore_errors=True)
                    logger.info("Removed extracted directory: %s", extracted_dir)
                # Clean up session output directory (all outputs are in S3 archive)
                if os.path.isdir(session_out):
                    shutil.rmtree(session_out, ignore_errors=True)
                    logger.info("Removed session output directory: %s", session_out)

    except Exception:
        # Always clean up extracted directory on error (working copy, not source data)
        if extracted_dir and os.path.isdir(extracted_dir):
            shutil.rmtree(extracted_dir, ignore_errors=True)
            logger.info("Removed extracted directory after error: %s", extracted_dir)
        # Clean up downloaded S3 files if applicable
        if s3_cfg.get("enabled", False) and s3_cfg.get("cleanup_after_upload", True):
            if downloaded_paths:
                logger.info("Cleaning up downloaded files after session failure...")
                cleanup_local_inputs(downloaded_paths, logger)
        raise

    logger.info("Session %s complete for sub-%s", ses_label, sub_id)
    return analysis_outcomes


def main():
    parser = argparse.ArgumentParser(
        description="Per-participant orchestrator for fMRI first-level processing (ABCD)."
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
        help="Participant ID (e.g. NDARABC123)."
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

    # Setup logging
    logger = setup_logging("orchestrate_first_level", log_file=args.log_file)

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
        # Verify AFNI installation
        if not args.dry_run:
            verify_afni_installation(logger)

        # Load and validate orchestrator config
        config = load_orchestrator_config(args.orchestrate_config, logger)

        # Load proc template
        if not os.path.isfile(args.proc_config):
            raise OrchestratorError(f"Proc config not found: {args.proc_config}")
        with open(args.proc_config) as f:
            try:
                proc_template = yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise OrchestratorError(f"YAML parse error in proc config: {e}")

        # Cross-validate orchestrator config against proc template
        validate_proc_template(config, proc_template, logger)

        # Run the pipeline
        process_participant(
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

if __name__ == "__main__":
    main()
