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
# discovery, brain masking, QC, non-steady-state TR removal, per-analysis
# FD censor generation, confounds extraction, run concatenation, optional
# smoothing, dynamic first-level config building, output compression, and
# S3 upload.
#
# Designed to be called in parallel (one invocation per participant):
#   parallel -j 8 python orchestrate_first_level.py \
#     --orchestrate_config study.yaml --proc_config example_config.yaml \
#     --subj_id {} --log-file logs/{}.log \
#     ::: NDARABC123 NDARDEF456 NDARGHI789
#
# Author: Taylor J. Keding, Ph.D.
# Version: 3.0
# Last updated: 02/19/26
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

from orchestrator_utils import (
    OrchestratorError,
    discover_available_sessions,
    download_session_data,
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
    generate_censor_file,
    extract_motion_regressors,
    extract_tissue_signals,
    fix_nback_cue_labels,
    format_task_timing,
    concatenate_bolds,
    concatenate_tabular_files,
    concatenate_task_timing,
    apply_smoothing,
    compute_preproc_qc,
    save_qc_json,
    build_first_level_config,
    write_temp_config,
    compute_first_level_qc,
)


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
            _process_session(
                config, sub_id, session, proc_template,
                skip_qc, skip_first_level, logger
            )
            session_results[session] = "success"

        except OrchestratorError as e:
            session_error = str(e)
            logger.error(
                "Session %s failed for sub-%s: %s", ses_label, sub_id, session_error
            )
            session_results[session] = f"failed: {session_error}"

        except Exception as e:
            session_error = str(e)
            logger.error(
                "Unexpected error in session %s for sub-%s: %s",
                ses_label, sub_id, session_error,
                exc_info=True
            )
            session_results[session] = f"failed: {session_error}"

        elapsed = time.time() - session_start
        status = "FAILED" if session_error else "SUCCESS"
        logger.info(
            "Session %s %s (%.2f seconds)", ses_label, status, elapsed
        )

    # ================================================================
    # Summary across all sessions
    # ================================================================
    logger.info("=" * 70)
    logger.info("SESSION SUMMARY for sub-%s:", sub_id)
    n_success = 0
    n_failed = 0
    for ses, result in session_results.items():
        logger.info("  ses-%sA: %s", ses, result)
        if result == "success":
            n_success += 1
        else:
            n_failed += 1
    logger.info(
        "Total: %d success, %d failed out of %d session(s)",
        n_success, n_failed, len(session_results)
    )
    logger.info("=" * 70)

    if n_success == 0 and n_failed > 0:
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
    4. Per-task preprocessing (mask, QC, NSS, censor, motion, tissue, timing)
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
    ses_label = f"ses-{session}A"

    # Session output directories
    session_out = os.path.join(output_dir, f"sub-{sub_id}", ses_label)
    preproc_dir = os.path.join(session_out, "preproc")
    concat_dir = os.path.join(session_out, "concat")
    qc_preproc_dir = os.path.join(session_out, "qc", "preproc")
    qc_fl_dir = os.path.join(session_out, "qc", "first_level")
    fl_out_dir = os.path.join(session_out, "first_level_out")

    for d in [session_out, preproc_dir, concat_dir, qc_preproc_dir, qc_fl_dir, fl_out_dir]:
        os.makedirs(d, exist_ok=True)

    downloaded_paths = []
    extracted_dir = None

    try:
        # ============================================================
        # Step 1: Download session data from S3
        # ============================================================
        events_files = {}
        if s3_cfg.get("enabled", False):
            logger.info("Step 1: Downloading session data from S3...")
            download_result = download_session_data(
                s3_cfg, sub_id, session, task_defs, fmriprep_dir, logger
            )
            downloaded_paths = download_result["all_downloaded_paths"]
            events_files = download_result["events_files"]

            # Step 2: Extract archive
            logger.info("Step 2: Extracting fMRIPrep archive...")
            archive_path = download_result["archive_path"]
            extract_target = os.path.join(fmriprep_dir, f"sub-{sub_id}", ses_label, "extracted")
            extracted_dir = extract_session_archive(archive_path, extract_target, logger)
        else:
            # Local mode: files already on disk
            extracted_dir = os.path.join(fmriprep_dir, f"sub-{sub_id}", ses_label)
            logger.info("S3 disabled — using local files at: %s", extracted_dir)

        # ============================================================
        # Step 3: Discover files per task
        # ============================================================
        logger.info("Step 3: Discovering files for all tasks...")
        discovered = discover_session_files(
            extracted_dir, sub_id, session, task_defs, events_files, space, logger
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

        # Collect unique FD thresholds needed per task (from analyses)
        task_fd_thresholds = defaultdict(set)
        for a in analyses:
            task_fd_thresholds[a["task_label"]].add(round(a["fd_threshold"], 4))

        for task_def in task_defs:
            task_label = task_def["task_label"]
            is_rest = task_label.lower().startswith("rest")
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
            # Per-FD-threshold censor files: {fd_thresh: [path, ...]}
            per_run_censors = defaultdict(list)
            per_run_timings = []
            per_run_csf = []
            per_run_wm = []
            per_run_gs = []
            per_run_tr_counts = []
            skipped_runs = []

            for rd in run_dicts:
                run_label = rd["run_label"]
                run_prefix = f"sub-{sub_id}_{run_label}"

                try:
                    # Step 4: Decompress if needed
                    rd["bold_path"] = decompress_if_needed(rd["bold_path"], logger)
                    rd["confounds_path"] = decompress_if_needed(rd["confounds_path"], logger)
                    rd["mask_path"] = decompress_if_needed(rd["mask_path"], logger)

                    # Step 5: Brain mask
                    logger.info("Step 5: Applying brain mask for %s...", run_label)
                    masked_bold = apply_brain_mask(
                        rd["bold_path"], rd["mask_path"],
                        preproc_dir, run_prefix, logger
                    )

                    # Step 6: Preprocessing QC
                    if not skip_qc and qc_cfg.get("preproc", {}).get("enabled", False):
                        logger.info("Step 6: Computing preprocessing QC for %s...", run_label)
                        n_remove_for_qc = detect_non_steady_state_trs(rd["confounds_path"], logger)
                        # Use the minimum FD threshold for this task for QC carpet plots
                        qc_fd = min(fd_thresholds_for_task) if fd_thresholds_for_task else 0.5
                        qc_metrics = compute_preproc_qc(
                            rd, rd["confounds_path"], masked_bold, rd["mask_path"],
                            n_remove_for_qc, TR, qc_fd, qc_cfg.get("preproc", {}),
                            qc_preproc_dir, f"sub-{sub_id}", space, logger
                        )
                        qc_out = os.path.join(qc_preproc_dir, f"{run_prefix}_preproc_qc.json")
                        save_qc_json(qc_metrics, qc_out, logger)

                    # Step 7: Detect & remove non-steady-state TRs
                    logger.info("Step 7: Handling non-steady-state TRs for %s...", run_label)
                    n_remove = detect_non_steady_state_trs(rd["confounds_path"], logger)
                    trimmed_bold, n_trs = remove_initial_trs_bold(
                        masked_bold, n_remove, preproc_dir, run_prefix, logger
                    )
                    per_run_tr_counts.append(n_trs)

                    # Step 8: Generate censor files (one per FD threshold for this task)
                    logger.info("Step 8: Generating censor file(s) for %s...", run_label)
                    for fd_thresh in sorted(fd_thresholds_for_task):
                        fd_thresh = round(fd_thresh, 4)
                        censor_out = os.path.join(
                            preproc_dir, f"{run_prefix}_censor_fd{fd_thresh}.1D"
                        )
                        censor_path, n_cens, n_tot = generate_censor_file(
                            rd["confounds_path"], n_remove, fd_thresh, censor_out, logger
                        )
                        per_run_censors[fd_thresh].append(censor_path)

                    # Step 9: Extract motion regressors
                    logger.info("Step 9: Extracting motion regressors for %s...", run_label)
                    motion_out = os.path.join(preproc_dir, f"{run_prefix}_motion.1D")
                    motion_path = extract_motion_regressors(
                        rd["confounds_path"], n_remove,
                        study.get("calc_n_motion_derivs", 1),
                        motion_out, logger
                    )
                    per_run_motions.append(motion_path)

                    # Step 10: Extract tissue signals (rest only)
                    if is_rest:
                        logger.info("Step 10: Extracting tissue signals for %s...", run_label)
                        csf_out = os.path.join(preproc_dir, f"{run_prefix}_csf.1D")
                        csf_path = extract_tissue_signals(
                            rd["confounds_path"], n_remove, "csf", csf_out, logger
                        )
                        per_run_csf.append(csf_path)

                        wm_out = os.path.join(preproc_dir, f"{run_prefix}_wm.1D")
                        wm_path = extract_tissue_signals(
                            rd["confounds_path"], n_remove, "white_matter", wm_out, logger
                        )
                        per_run_wm.append(wm_path)

                        gs_out = os.path.join(preproc_dir, f"{run_prefix}_gs.1D")
                        gs_path = extract_tissue_signals(
                            rd["confounds_path"], n_remove, "global_signal", gs_out, logger
                        )
                        per_run_gs.append(gs_path)

                    # Step 11: Format task timing (task only)
                    if not is_rest and rd["events_path"] is not None:
                        logger.info("Step 11: Formatting task timing for %s...", run_label)
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
                                fixed_events_out, logger
                            )

                        timing_out = os.path.join(preproc_dir, f"{run_prefix}_timing.csv")
                        timing_path, n_dropped = format_task_timing(
                            events_for_timing,
                            condition_col,
                            None,  # conditions_include removed
                            task_def.get("conditions_exclude"),
                            n_remove, TR, timing_out, logger
                        )
                        per_run_timings.append(timing_path)

                    per_run_bolds.append(trimmed_bold)

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
                logger.warning(
                    "Task '%s' sub-%s %s: skipped %d of %d run(s): %s",
                    task_label, sub_id, ses_label,
                    len(skipped_runs), len(run_dicts),
                    ", ".join(skipped_runs)
                )

            if not per_run_bolds:
                logger.warning(
                    "No runs successfully processed for task '%s' sub-%s %s — skipping task.",
                    task_label, sub_id, ses_label
                )
                continue

            # ============================================================
            # Step 12: Concatenate runs or collect per-run files
            # ============================================================
            task_prefix = f"sub-{sub_id}_{ses_label}_task-{task_label}"

            if should_concat and len(per_run_bolds) > 0:
                logger.info("Step 12: Concatenating runs for task '%s'...", task_label)

                concat_bold = concatenate_bolds(
                    per_run_bolds,
                    os.path.join(concat_dir, f"{task_prefix}_concat_bold.nii.gz"),
                    logger
                )
                concat_motion = concatenate_tabular_files(
                    per_run_motions,
                    os.path.join(concat_dir, f"{task_prefix}_concat_motion.1D"),
                    logger
                )

                # Concatenate censor files per FD threshold
                concat_censors = {}
                for fd_thresh in sorted(per_run_censors.keys()):
                    concat_censor = concatenate_tabular_files(
                        per_run_censors[fd_thresh],
                        os.path.join(concat_dir, f"{task_prefix}_concat_censor_fd{fd_thresh}.1D"),
                        logger
                    )
                    concat_censors[fd_thresh] = concat_censor

                # For smoothing, use mask from first run
                concat_mask = run_dicts[0]["mask_path"]

                concat_timing = None
                if per_run_timings:
                    concat_timing = concatenate_task_timing(
                        per_run_timings, per_run_tr_counts, TR,
                        os.path.join(concat_dir, f"{task_prefix}_concat_timing.csv"),
                        logger
                    )

                # Apply smoothing if enabled
                if smoothing_cfg.get("enabled", False):
                    logger.info("Applying smoothing to concatenated BOLD...")
                    concat_bold = apply_smoothing(
                        concat_bold, concat_mask,
                        smoothing_cfg["method"], smoothing_cfg["fwhm"],
                        concat_dir, f"{task_prefix}_concat", logger
                    )

                processed_files[task_label] = {
                    "bold": concat_bold,
                    "motion": concat_motion,
                    "censor": concat_censors,
                    "timing": concat_timing,
                }

            else:
                # Rest: keep per-run files
                logger.info("Step 12: Collecting per-run files for task '%s'.", task_label)

                # Apply smoothing per-run if enabled
                if smoothing_cfg.get("enabled", False):
                    logger.info("Applying smoothing to per-run BOLDs...")
                    smoothed_bolds = []
                    for i, bold in enumerate(per_run_bolds):
                        smoothed = apply_smoothing(
                            bold, run_dicts[i]["mask_path"],
                            smoothing_cfg["method"], smoothing_cfg["fwhm"],
                            preproc_dir, f"sub-{sub_id}_{run_dicts[i]['run_label']}", logger
                        )
                        smoothed_bolds.append(smoothed)
                    per_run_bolds = smoothed_bolds

                processed_files[task_label] = {
                    "bolds": per_run_bolds,
                    "motions": per_run_motions,
                    "censors": dict(per_run_censors),
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
        # Step 13: Build config & run first-level analyses
        # ============================================================
        if not skip_first_level and processed_files:
            logger.info("=" * 60)
            logger.info("Step 13: Building first-level config and running analyses...")
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

                        # First-level QC
                        if not skip_qc and qc_cfg.get("first_level", {}).get("enabled", False):
                            # Find the matching orchestrator analysis for censor proportion
                            orch_a = next((a for a in active_analyses if a["name"] == name), None)
                            censor_prop = None
                            if orch_a:
                                task_label = orch_a["task_label"]
                                fd_thresh = round(orch_a["fd_threshold"], 4)
                                pf = processed_files.get(task_label, {})
                                # Count censored volumes for this threshold
                                if "censor" in pf:
                                    # Concatenated task
                                    censor_file = pf["censor"].get(fd_thresh)
                                    if censor_file:
                                        cdata = np.loadtxt(censor_file)
                                        n_tot = len(cdata)
                                        n_cens = int(np.sum(cdata == 0))
                                        censor_prop = n_cens / n_tot if n_tot > 0 else None
                                elif "censors" in pf:
                                    # Per-run rest
                                    censor_files = pf["censors"].get(fd_thresh, [])
                                    if censor_files:
                                        total_cens = 0
                                        total_vols = 0
                                        for cf in censor_files:
                                            cdata = np.loadtxt(cf)
                                            total_vols += len(cdata)
                                            total_cens += int(np.sum(cdata == 0))
                                        censor_prop = total_cens / total_vols if total_vols > 0 else None

                            fl_qc = compute_first_level_qc(
                                name, atype, fl_out_dir, f"sub-{sub_id}",
                                censor_prop, logger, error_msg=analysis_error
                            )
                            fl_qc["session"] = ses_label
                            fl_qc_out = os.path.join(
                                qc_fl_dir, f"sub-{sub_id}_{ses_label}_{name}_first_level_qc.json"
                            )
                            save_qc_json(fl_qc, fl_qc_out, logger)
                finally:
                    os.chdir(original_dir)

        # ============================================================
        # Step 14: Compress, upload, cleanup
        # ============================================================
        if s3_cfg.get("enabled", False) and processed_files:
            logger.info("Step 14a: Compressing session outputs...")
            archive_path = compress_session_outputs(sub_id, session, session_out, logger)

            logger.info("Step 14b: Uploading results archive to S3...")
            upload_to_s3(s3_cfg, sub_id, session, archive_path, logger)

            if s3_cfg.get("cleanup_after_upload", True):
                logger.info("Step 14c: Cleaning up local files...")
                cleanup_local_inputs(downloaded_paths, logger)
                # Also clean up extracted directory
                if extracted_dir and os.path.isdir(extracted_dir):
                    shutil.rmtree(extracted_dir, ignore_errors=True)
                    logger.info("Removed extracted directory: %s", extracted_dir)

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
