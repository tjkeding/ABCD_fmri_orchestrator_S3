#!/usr/bin/env python3

# ============================================================================
# ORCHESTRATOR UTILITIES FOR fMRI FIRST-LEVEL PROCESSING
# Helper functions for the per-participant orchestration pipeline.
# Called by orchestrate_first_level.py in pipeline order.
#
# Author: Taylor J. Keding, Ph.D.
# Version: 3.0
# Last updated: 02/19/26
# ============================================================================

import os
import re
import bz2
import copy
import glob as globmod
import gzip
import json
import shutil
import tarfile
import subprocess

import yaml
import numpy as np
import pandas as pd

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError, NoCredentialsError


class OrchestratorError(Exception):
    """Raised for unrecoverable orchestrator errors."""
    pass


# ============================================================================
# S3 Placeholders
# ============================================================================

def _get_s3_client():
    """Create a boto3 S3 client with standard retry config."""
    try:
        return boto3.client(
            "s3",
            config=BotocoreConfig(retries={"max_attempts": 3, "mode": "standard"}),
        )
    except NoCredentialsError:
        raise OrchestratorError(
            "AWS credentials not found. Configure credentials via environment "
            "variables, ~/.aws/credentials, or EC2 instance role."
        )


def discover_available_sessions(s3_config, sub_id, logger):
    """
    Probe S3 to discover which sessions exist for a subject.

    Checks for the fMRIPrep archive at each possible session code in
    s3_config['available_sessions'].

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID (e.g. "NDARABC123").
    logger : logging.Logger

    Returns
    -------
    list of str
        Session codes that exist on S3 (e.g. ["00", "02"]).

    Raises
    ------
    OrchestratorError
        If no sessions are found for this subject.
    """
    s3_client = _get_s3_client()
    bucket = s3_config["bucket"]
    fmriprep_prefix = s3_config["fmriprep_s3_prefix"]

    available = []
    for session in s3_config["available_sessions"]:
        key = (
            f"{fmriprep_prefix}/sub-{sub_id}/ses-{session}A/"
            f"sub-{sub_id}_ses-{session}A_fmriprep-output.tar.gz"
        )
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            available.append(session)
            logger.debug(
                "Session %s found for sub-%s: s3://%s/%s",
                session, sub_id, bucket, key
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("404", "NoSuchKey"):
                logger.debug(
                    "Session %s not found for sub-%s: s3://%s/%s",
                    session, sub_id, bucket, key
                )
            else:
                logger.error(
                    "S3 error (%s) probing session %s for sub-%s: %s",
                    code, session, sub_id, str(e)
                )
                raise

    if not available:
        raise OrchestratorError(
            f"No sessions found on S3 for sub-{sub_id}. "
            f"Checked session codes {s3_config['available_sessions']} "
            f"at s3://{bucket}/{fmriprep_prefix}/sub-{sub_id}/ses-{{code}}A/"
        )

    logger.info(
        "Discovered %d session(s) for sub-%s: %s",
        len(available), sub_id, available
    )
    return available


def download_session_data(s3_config, sub_id, session, task_defs, local_base_dir, logger):
    """
    Download all data for one session from S3.

    Downloads:
    1. fMRIPrep archive: sub-{ID}_ses-{session}A_fmriprep-output.tar.gz
    2. Events files (non-rest tasks only): probes for runs 1-9 per task

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    task_defs : list of dict
        Task definitions from the orchestrator config.
    local_base_dir : str
        Local directory to download files into.
    logger : logging.Logger

    Returns
    -------
    dict
        - archive_path: path to downloaded tar.gz
        - events_files: {task_label: [path1, path2, ...]} ordered by run
        - all_downloaded_paths: flat list of all files downloaded (for cleanup)
    """
    s3_client = _get_s3_client()
    bucket = s3_config["bucket"]
    fmriprep_prefix = s3_config["fmriprep_s3_prefix"]
    mmps_prefix = s3_config["mmps_mproc_s3_prefix"]

    ses_label = f"ses-{session}A"
    all_downloaded = []

    # --- 1. Download fMRIPrep archive ---
    archive_s3_key = (
        f"{fmriprep_prefix}/sub-{sub_id}/{ses_label}/"
        f"sub-{sub_id}_{ses_label}_fmriprep-output.tar.gz"
    )
    local_ses_dir = os.path.join(local_base_dir, f"sub-{sub_id}", ses_label)
    os.makedirs(local_ses_dir, exist_ok=True)
    archive_local = os.path.join(
        local_ses_dir, f"sub-{sub_id}_{ses_label}_fmriprep-output.tar.gz"
    )

    if os.path.isfile(archive_local):
        logger.info("Archive already present locally: %s", archive_local)
    else:
        logger.info(
            "Downloading fMRIPrep archive: s3://%s/%s", bucket, archive_s3_key
        )
        try:
            s3_client.download_file(bucket, archive_s3_key, archive_local)
            logger.info("Downloaded archive: %s", archive_local)
        except ClientError as e:
            raise OrchestratorError(
                f"Failed to download fMRIPrep archive for sub-{sub_id} "
                f"{ses_label}: {e}"
            )
    all_downloaded.append(archive_local)

    # --- 2. Download events files (non-rest tasks only) ---
    events_files = {}
    for task_def in task_defs:
        task_label = task_def["task_label"]
        is_rest = task_label.lower().startswith("rest")
        if is_rest:
            continue

        task_events = []
        # Probe for runs 1-9, stop at first missing run
        for run_num in range(1, 10):
            events_s3_key = (
                f"{mmps_prefix}/sub-{sub_id}/{ses_label}/func/"
                f"sub-{sub_id}_{ses_label}_task-{task_label}_run-0{run_num}_events.tsv"
            )
            events_local_dir = os.path.join(local_ses_dir, "events")
            os.makedirs(events_local_dir, exist_ok=True)
            events_local = os.path.join(
                events_local_dir,
                f"sub-{sub_id}_{ses_label}_task-{task_label}_run-0{run_num}_events.tsv"
            )

            if os.path.isfile(events_local):
                task_events.append(events_local)
                all_downloaded.append(events_local)
                continue

            try:
                s3_client.head_object(Bucket=bucket, Key=events_s3_key)
            except ClientError:
                # No more runs for this task
                break

            try:
                s3_client.download_file(bucket, events_s3_key, events_local)
                task_events.append(events_local)
                all_downloaded.append(events_local)
                logger.debug(
                    "Downloaded events: s3://%s/%s", bucket, events_s3_key
                )
            except ClientError as e:
                logger.warning(
                    "Failed to download events file for sub-%s %s task-%s run-%d: %s",
                    sub_id, ses_label, task_label, run_num, str(e)
                )
                break

        if task_events:
            events_files[task_label] = task_events
            logger.info(
                "Downloaded %d events file(s) for task '%s' %s",
                len(task_events), task_label, ses_label
            )
        else:
            logger.warning(
                "No events files found on S3 for task '%s' sub-%s %s",
                task_label, sub_id, ses_label
            )

    logger.info(
        "S3 download summary for sub-%s %s: archive + %d events file(s)",
        sub_id, ses_label,
        sum(len(v) for v in events_files.values())
    )

    return {
        "archive_path": archive_local,
        "events_files": events_files,
        "all_downloaded_paths": all_downloaded,
    }


def extract_session_archive(archive_path, target_dir, logger):
    """
    Extract a session fMRIPrep tar.gz archive.

    Parameters
    ----------
    archive_path : str
        Path to the tar.gz archive.
    target_dir : str
        Directory to extract into.
    logger : logging.Logger

    Returns
    -------
    str
        Path to the extracted session directory.

    Raises
    ------
    OrchestratorError
        If extraction fails or expected subdirectories are missing.
    """
    logger.info("Extracting archive: %s -> %s", archive_path, target_dir)
    os.makedirs(target_dir, exist_ok=True)

    # Disk space check: require at least 10x archive size free
    archive_size = os.path.getsize(archive_path)
    free_space = shutil.disk_usage(target_dir).free
    required = archive_size * 10  # conservative estimate for extraction
    if free_space < required:
        raise OrchestratorError(
            f"Insufficient disk space for extraction. "
            f"Archive: {archive_size / 1e9:.1f} GB, "
            f"Free: {free_space / 1e9:.1f} GB, "
            f"Required (est): {required / 1e9:.1f} GB"
        )

    try:
        with tarfile.open(archive_path, "r:gz") as tar:
            # Validate tar members to prevent path traversal attacks
            target_real = os.path.realpath(target_dir) + os.sep
            all_members = tar.getmembers()
            safe_members = []
            for member in all_members:
                member_path = os.path.realpath(os.path.join(target_dir, member.name))
                if member_path.startswith(target_real) or member_path == target_real.rstrip(os.sep):
                    safe_members.append(member)
            if len(safe_members) < len(all_members):
                n_skipped = len(all_members) - len(safe_members)
                logger.warning(
                    "Skipped %d unsafe tar member(s) with path traversal in %s",
                    n_skipped, archive_path
                )
            tar.extractall(path=target_dir, members=safe_members)
    except tarfile.TarError as e:
        raise OrchestratorError(
            f"Failed to extract archive {archive_path}: {e}"
        )

    # Find the extracted directory — look for func/ subdirectory
    # The archive may contain sub-{ID}/ses-{session}A/func/ or just func/
    # Walk one or two levels to find func/
    extracted_dir = target_dir
    for root, dirs, files in os.walk(target_dir):
        if "func" in dirs:
            extracted_dir = root
            break

    func_dir = os.path.join(extracted_dir, "func")
    if not os.path.isdir(func_dir):
        raise OrchestratorError(
            f"Expected 'func/' subdirectory not found after extracting "
            f"{archive_path} to {target_dir}"
        )

    n_files = sum(len(f) for _, _, f in os.walk(extracted_dir))
    logger.info(
        "Archive extracted: %s (%d files, func/ found)",
        extracted_dir, n_files
    )
    return extracted_dir


def upload_to_s3(s3_config, sub_id, session, archive_path, logger):
    """
    Upload a session results archive to S3.

    Uses boto3's multipart upload automatically for large files. Verifies the
    upload by comparing the S3 object ContentLength to the local file size.

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    archive_path : str
        Local path to the .tar.gz archive created by compress_session_outputs().
    logger : logging.Logger
    """
    if not os.path.isfile(archive_path):
        raise FileNotFoundError(f"Archive not found for S3 upload: {archive_path}")

    bucket = s3_config["bucket"]
    upload_prefix = s3_config["upload_prefix"]
    ses_label = f"ses-{session}A"
    s3_key = f"{upload_prefix}/sub-{sub_id}/{ses_label}/first_level_out.tar.gz"

    s3_client = _get_s3_client()

    size_mb = os.path.getsize(archive_path) / (1024 * 1024)
    logger.info(
        "Uploading %s (%.1f MB) → s3://%s/%s",
        os.path.basename(archive_path), size_mb, bucket, s3_key
    )

    try:
        s3_client.upload_file(archive_path, bucket, s3_key)
    except NoCredentialsError:
        raise OrchestratorError(
            "AWS credentials not found. Configure credentials before enabling S3."
        )
    except ClientError as e:
        logger.error("S3 upload failed for %s: %s", archive_path, str(e))
        raise

    # Verify upload: compare remote ContentLength to local file size
    response = s3_client.head_object(Bucket=bucket, Key=s3_key)
    remote_size = response["ContentLength"]
    local_size = os.path.getsize(archive_path)

    if remote_size != local_size:
        raise RuntimeError(
            f"Upload size mismatch for {sub_id}: "
            f"local={local_size} bytes, S3={remote_size} bytes "
            f"(s3://{bucket}/{s3_key})"
        )

    logger.info(
        "Upload verified: s3://%s/%s (%d bytes)", bucket, s3_key, remote_size
    )

# ============================================================================
# Section B: AFNI Check
# ============================================================================

def verify_afni_installation(logger):
    """
    Verify that AFNI is installed and on PATH by running '3dinfo -ver'.
    Exits with error if AFNI is not found.
    """
    try:
        result = subprocess.run(
            ["3dinfo", "-ver"],
            capture_output=True, text=True, check=True,
        )
        logger.info("AFNI version: %s", result.stdout.strip())
    except FileNotFoundError:
        raise OrchestratorError(
            "AFNI not found on PATH. Please install AFNI and ensure it is "
            "available in your environment."
        )
    except subprocess.CalledProcessError as e:
        raise OrchestratorError(
            f"AFNI check failed: {e.stderr.strip() if e.stderr else str(e)}"
        )

# ============================================================================
# Section C: File Discovery
# ============================================================================

def discover_session_files(extracted_dir, sub_id, session, task_defs, events_files, space, logger):
    """
    Discover fMRIPrep outputs from an extracted session archive.

    Globs the extracted func/ directory for all runs per task, matching
    each discovered run with its corresponding events file (from mmps_mproc).

    Parameters
    ----------
    extracted_dir : str
        Path to the extracted session directory (contains func/, anat/).
    sub_id : str
        Participant ID (e.g. "NDARABC123").
    session : str
        Session code (e.g. "00").
    task_defs : list of dict
        Task definitions from the orchestrator config.
    events_files : dict
        {task_label: [events_path_run1, events_path_run2, ...]} from
        download_session_data(). Missing tasks have no entry.
    space : str
        Template space string (e.g. "MNI152NLin2009cAsym").
    logger : logging.Logger

    Returns
    -------
    dict
        {task_label: [run_dict, ...], "_anat_mask": anat_mask_path or None}
        Each run_dict has keys: bold_path, confounds_path, mask_path,
        events_path (None for rest), session, task_label, run, run_label.
    """
    ses_label = f"ses-{session}A"
    func_dir = os.path.join(extracted_dir, "func")
    result = {}

    for task_def in task_defs:
        task_label = task_def["task_label"]
        is_rest = task_label.lower().startswith("rest")

        # Glob for BOLD files to discover available runs
        bold_pattern = os.path.join(
            func_dir,
            f"sub-{sub_id}_{ses_label}_task-{task_label}_run-*"
            f"_space-{space}_desc-preproc_bold.nii.gz"
        )
        bold_files = sorted(globmod.glob(bold_pattern))

        if not bold_files:
            logger.warning(
                "No BOLD files found for task '%s' sub-%s %s (pattern: %s)",
                task_label, sub_id, ses_label, bold_pattern
            )
            continue

        # Get task events list (empty for rest)
        task_events = events_files.get(task_label, [])

        # Build events lookup dict keyed by run number (not position)
        task_events_by_run = {}
        if not is_rest:
            for evt_path in task_events:
                evt_match = re.search(r"_run-0?(\d+)_events\.tsv$", os.path.basename(evt_path))
                if evt_match:
                    task_events_by_run[int(evt_match.group(1))] = evt_path

        run_dicts = []
        for bold_path in bold_files:
            # Extract run number from filename
            basename = os.path.basename(bold_path)
            # Pattern: ..._run-0{N}_space-...
            run_match = re.search(r"_run-(\d+)_", basename)
            if not run_match:
                logger.warning("Could not parse run number from: %s", basename)
                continue
            run_num = int(run_match.group(1))

            # Build expected paths for confounds and mask
            confounds_name = (
                f"sub-{sub_id}_{ses_label}_task-{task_label}_run-{run_match.group(1)}"
                f"_desc-confounds_timeseries.tsv"
            )
            confounds_path = os.path.join(func_dir, confounds_name)

            mask_name = (
                f"sub-{sub_id}_{ses_label}_task-{task_label}_run-{run_match.group(1)}"
                f"_space-{space}_desc-brain_mask.nii.gz"
            )
            mask_path = os.path.join(func_dir, mask_name)

            # Verify confounds exist
            if not os.path.isfile(confounds_path):
                logger.warning(
                    "Missing confounds for sub-%s %s task-%s run-%d: %s — skipping run",
                    sub_id, ses_label, task_label, run_num, confounds_path
                )
                continue

            # Verify mask exists
            if not os.path.isfile(mask_path):
                logger.warning(
                    "Missing mask for sub-%s %s task-%s run-%d: %s — skipping run",
                    sub_id, ses_label, task_label, run_num, mask_path
                )
                continue

            # Match events file by run number (for non-rest tasks)
            events_path = None
            if not is_rest:
                events_path = task_events_by_run.get(run_num)
                if events_path is None:
                    logger.warning(
                        "No events file for sub-%s %s task-%s run-%d — skipping run",
                        sub_id, ses_label, task_label, run_num
                    )
                    continue
                if not os.path.isfile(events_path):
                    logger.warning(
                        "Events file missing for sub-%s %s task-%s run-%d: %s — skipping run",
                        sub_id, ses_label, task_label, run_num, events_path
                    )
                    continue

            run_label = f"{ses_label}_task-{task_label}_run-{run_num}"

            run_dicts.append({
                "bold_path": bold_path,
                "confounds_path": confounds_path,
                "mask_path": mask_path,
                "events_path": events_path,
                "session": session,
                "task_label": task_label,
                "run": run_num,
                "run_label": run_label,
            })

            logger.info("Discovered files for sub-%s, %s", sub_id, run_label)

        if run_dicts:
            result[task_label] = run_dicts
        else:
            logger.warning(
                "No valid runs discovered for task '%s' sub-%s %s",
                task_label, sub_id, ses_label
            )

    # Discover anatomical brain mask for registration QC
    anat_dir = os.path.join(extracted_dir, "anat")
    anat_mask_path = None
    if os.path.isdir(anat_dir):
        space_tag = f"space-{space}"
        candidates = [
            f for f in os.listdir(anat_dir)
            if "desc-brain_mask" in f
            and space_tag in f
            and f.endswith(".nii.gz")
        ]
        if candidates:
            anat_mask_path = os.path.join(anat_dir, sorted(candidates)[0])
            logger.info("Found anatomical brain mask: %s", anat_mask_path)

    result["_anat_mask"] = anat_mask_path
    return result

# ============================================================================
# Section D: Decompression
# ============================================================================

def decompress_if_needed(file_path, logger):
    """
    Locate and decompress a file that may have been compressed for S3 transfer.

    Probing order:
      1. file_path exists as-is           → returned unchanged (covers .nii.gz)
      2. file_path + ".bz2" exists        → decompressed with bz2
      3. file_path + ".gz" exists         → decompressed with gzip
         (only for non-.nii.gz targets; .nii.gz files are already handled by #1)
      4. A .tar.gz archive in the same    → extracted to the target directory;
         directory that contains the        the member whose basename matches
         target filename                    file_path's basename is used
      5. None of the above               → FileNotFoundError

    Parameters
    ----------
    file_path : str
        The expected path of the decompressed file.
    logger : logging.Logger

    Returns
    -------
    str
        Path to the (now-decompressed) file.
    """
    # Case 1: already present (handles .nii.gz and any uncompressed file)
    if os.path.isfile(file_path):
        return file_path

    out_dir = os.path.dirname(file_path) or "."
    basename = os.path.basename(file_path)

    # Case 2: .bz2
    bz2_path = file_path + ".bz2"
    if os.path.isfile(bz2_path):
        logger.info("Decompressing .bz2: %s", bz2_path)
        with bz2.open(bz2_path, "rb") as f_in, open(file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        logger.info("Decompressed → %s", file_path)
        return file_path

    # Case 3: standalone .gz (skip if the target already ends in .gz, to
    # avoid trying to gunzip a .nii.gz into a second .nii.gz layer)
    gz_path = file_path + ".gz"
    if os.path.isfile(gz_path) and not file_path.endswith(".gz"):
        logger.info("Decompressing .gz: %s", gz_path)
        with gzip.open(gz_path, "rb") as f_in, open(file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        logger.info("Decompressed → %s", file_path)
        return file_path

    # Case 4: .tar.gz archive in the same directory containing this file
    for entry in os.listdir(out_dir):
        if not (entry.endswith(".tar.gz") or entry.endswith(".tgz")):
            continue
        archive_path = os.path.join(out_dir, entry)
        try:
            with tarfile.open(archive_path, "r:gz") as tar:
                # Find a member whose basename matches the target
                match = next(
                    (m for m in tar.getmembers()
                     if os.path.basename(m.name) == basename and m.isfile()),
                    None
                )
                if match is None:
                    continue
                logger.info(
                    "Extracting '%s' from archive %s", match.name, archive_path
                )
                # Extract to out_dir, then move to exact file_path if needed
                tar.extract(match, path=out_dir)
                extracted = os.path.join(out_dir, match.name)
                if os.path.abspath(extracted) != os.path.abspath(file_path):
                    shutil.move(extracted, file_path)
                logger.info("Extracted → %s", file_path)
                return file_path
        except tarfile.TarError as e:
            logger.warning("Could not read archive %s: %s", archive_path, e)
            continue

    raise FileNotFoundError(
        f"File not found and no compressed version located "
        f"(checked .bz2, .gz, .tar.gz): {file_path}"
    )

# ============================================================================
# Section E: Brain Masking
# ============================================================================

def apply_brain_mask(bold_path, mask_path, out_dir, out_prefix, logger):
    """
    Apply a brain mask to a BOLD file using AFNI's 3dcalc.

    Returns
    -------
    str
        Path to the masked BOLD file.
    """
    out_path = os.path.join(out_dir, f"{out_prefix}_masked.nii.gz")

    if os.path.isfile(out_path):
        logger.info("Masked BOLD already exists: %s", out_path)
        return out_path

    cmd = [
        "3dcalc",
        "-a", bold_path,
        "-b", mask_path,
        "-expr", "a*step(b)",
        "-prefix", out_path,
    ]

    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise OrchestratorError(
            f"Brain masking failed for {bold_path}: {e.stderr.strip() if e.stderr else str(e)}"
        )

    if not os.path.isfile(out_path):
        raise OrchestratorError(f"Brain masking produced no output: {out_path}")

    logger.info("Brain masking complete: %s", out_path)
    return out_path

# ============================================================================
# Section F: Non-Steady-State TR Handling
# ============================================================================

def detect_non_steady_state_trs(confounds_path, logger):
    """
    Detect non-steady-state TRs from fMRIPrep confounds file.

    Counts columns matching 'non_steady_state_outlier_XX'.

    Returns
    -------
    int
        Number of non-steady-state TRs to remove.
    """
    confounds_df = pd.read_csv(confounds_path, sep="\t")
    nss_cols = [c for c in confounds_df.columns if c.startswith("non_steady_state_outlier")]
    n_remove = len(nss_cols)

    if n_remove > 0:
        logger.info("Detected %d non-steady-state TR(s) in %s", n_remove, confounds_path)
    else:
        logger.info("No non-steady-state TRs detected in %s", confounds_path)

    return n_remove

def remove_initial_trs_bold(bold_path, n_remove, out_dir, out_prefix, logger):
    """
    Remove initial TRs from a BOLD file using AFNI's 3dTcat.

    Returns
    -------
    tuple of (str, int)
        (path to trimmed BOLD, number of TRs remaining)
    """
    if n_remove == 0:
        # Get TR count from 3dinfo
        try:
            result = subprocess.run(
                ["3dinfo", "-nv", bold_path],
                capture_output=True, text=True, check=True,
            )
            n_trs = int(result.stdout.strip())
        except (subprocess.CalledProcessError, ValueError):
            logger.warning("Could not determine TR count for %s; returning path as-is.", bold_path)
            n_trs = -1
        return bold_path, n_trs

    out_path = os.path.join(out_dir, f"{out_prefix}_trimmed.nii.gz")

    if os.path.isfile(out_path):
        try:
            result = subprocess.run(
                ["3dinfo", "-nv", out_path],
                capture_output=True, text=True, check=True,
            )
            n_trs = int(result.stdout.strip())
        except (subprocess.CalledProcessError, ValueError):
            n_trs = -1
        logger.info("Trimmed BOLD already exists: %s (%d TRs)", out_path, n_trs)
        return out_path, n_trs

    cmd = [
        "3dTcat",
        "-prefix", out_path,
        f"{bold_path}[{n_remove}..$]",
    ]

    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise OrchestratorError(
            f"3dTcat failed for {bold_path}: {e.stderr.strip() if e.stderr else str(e)}"
        )

    if not os.path.isfile(out_path):
        raise OrchestratorError(f"3dTcat produced no output: {out_path}")

    try:
        result = subprocess.run(
            ["3dinfo", "-nv", out_path],
            capture_output=True, text=True, check=True,
        )
        n_trs = int(result.stdout.strip())
    except (subprocess.CalledProcessError, ValueError):
        n_trs = -1

    logger.info("Removed %d initial TR(s): %s (%d TRs remaining)", n_remove, out_path, n_trs)
    return out_path, n_trs

def remove_initial_trs_tabular(file_path, n_remove, out_path, logger):
    """
    Remove initial rows from a tabular (text/tsv) file.

    Returns
    -------
    tuple of (str, int)
        (output path, number of rows remaining)
    """
    if n_remove == 0:
        # Copy file as-is
        if file_path != out_path:
            shutil.copy2(file_path, out_path)
        data = np.loadtxt(file_path)
        n_rows = data.shape[0] if data.ndim > 0 else 1
        return out_path, n_rows

    data = np.loadtxt(file_path)
    if data.ndim == 1:
        trimmed = data[n_remove:]
    else:
        trimmed = data[n_remove:, :]

    n_rows = trimmed.shape[0] if trimmed.ndim > 0 else 1
    np.savetxt(out_path, trimmed, fmt="%.10g", delimiter="\t")

    logger.info("Removed %d initial row(s) from %s → %s (%d rows remaining)",
                n_remove, os.path.basename(file_path), os.path.basename(out_path), n_rows)
    return out_path, n_rows

# ============================================================================
# Section G: Confounds Extraction
# ============================================================================

def extract_motion_regressors(confounds_path, n_remove, calc_n_motion_derivs, out_path, logger):
    """
    Extract motion regressors from fMRIPrep confounds file.

    Always extracts the 6 base motion parameters (trans_x/y/z, rot_x/y/z).
    For each requested derivative degree (1..calc_n_motion_derivs):
      - Uses the column from the confounds TSV if fMRIPrep already computed it.
      - Otherwise computes it numerically via finite differences on the
        previous degree (padded with 0.0 at the first row to preserve length).

    Total output columns = 6 * (1 + calc_n_motion_derivs).

    fmri_first_level_proc will truncate extra columns as needed; the file
    therefore contains the maximum number of columns any downstream analysis
    may request.

    Parameters
    ----------
    confounds_path : str
        Path to fMRIPrep confounds TSV.
    n_remove : int
        Number of non-steady-state TRs to trim from the start.
    calc_n_motion_derivs : int
        Number of temporal derivative sets to include (>= 0).
    out_path : str
        Destination path for the output motion file.
    logger : logging.Logger

    Returns
    -------
    str
        Path to the motion regressors file.
    """
    if os.path.isfile(out_path):
        logger.info("Motion regressors already exist: %s", out_path)
        return out_path

    confounds_df = pd.read_csv(confounds_path, sep="\t")

    if len(confounds_df) == 0:
        raise OrchestratorError(f"Confounds file is empty (0 rows): {confounds_path}")

    base_cols = ["trans_x", "trans_y", "trans_z", "rot_x", "rot_y", "rot_z"]

    # Verify base columns exist
    missing_base = [c for c in base_cols if c not in confounds_df.columns]
    if missing_base:
        raise OrchestratorError(
            f"Missing base motion columns in {confounds_path}: {missing_base}. "
            f"Available columns: {list(confounds_df.columns)}"
        )

    # Verify base columns contain valid data (not entirely NaN)
    base_data = confounds_df[base_cols]
    if base_data.isna().all().any():
        all_nan_cols = [c for c in base_cols if base_data[c].isna().all()]
        raise OrchestratorError(
            f"Motion columns are entirely NaN in {confounds_path}: {all_nan_cols}. "
            f"fMRIPrep output may be corrupted."
        )

    # Build the motion array column by column
    # Start with base 6 parameters
    arrays = [confounds_df[base_cols].values]  # shape (n_trs, 6)
    prev_degree_data = arrays[0]

    # fMRIPrep derivative column name pattern:
    #   degree 1 → trans_x_derivative1
    #   degree 2 → trans_x_derivative1_power2  (NOT used here; we want the
    #              temporal 2nd derivative, i.e. diff of 1st derivative)
    # We use a simple naming convention: check for "_derivative{d}" suffix.
    fmriprep_deriv_suffixes = {
        1: "_derivative1",
    }

    for degree in range(1, calc_n_motion_derivs + 1):
        suffix = fmriprep_deriv_suffixes.get(degree)
        fmriprep_cols = [f"{c}{suffix}" for c in base_cols] if suffix else []
        available = fmriprep_cols and all(c in confounds_df.columns for c in fmriprep_cols)

        if available:
            deriv_data = confounds_df[fmriprep_cols].values
            logger.info(
                "Using fMRIPrep-computed derivative (degree %d) from confounds: %s",
                degree, fmriprep_cols
            )
        else:
            # Compute numerically: forward difference of the previous degree
            # diff shape is (n_trs-1, 6); prepend a row of zeros for alignment
            diff = np.diff(prev_degree_data, axis=0)
            deriv_data = np.vstack([np.zeros((1, prev_degree_data.shape[1])), diff])
            logger.info(
                "fMRIPrep derivative (degree %d) not found in confounds — "
                "computing numerically via finite differences.",
                degree
            )

        arrays.append(deriv_data)
        prev_degree_data = deriv_data

    motion_data = np.hstack(arrays)  # shape (n_trs, 6 * (1 + calc_n_motion_derivs))

    # Remove initial non-steady-state TRs
    if n_remove > 0:
        motion_data = motion_data[n_remove:, :]

    # Replace any remaining NaN with 0.0 (e.g. first-row fMRIPrep derivatives)
    motion_data = np.nan_to_num(motion_data, nan=0.0)

    np.savetxt(out_path, motion_data, fmt="%.10g", delimiter="\t")
    logger.info(
        "Motion regressors saved: %d columns, %d rows → %s",
        motion_data.shape[1], motion_data.shape[0], out_path
    )
    return out_path

def generate_censor_file(confounds_path, n_remove, fd_threshold, out_path, logger):
    """
    Generate a binary censor file from framewise displacement.

    FD <= threshold → 1 (include), FD > threshold → 0 (exclude).
    First volume (NaN FD) is always included (1).

    Returns
    -------
    tuple of (str, int, int)
        (path, n_censored, n_total)
    """
    if os.path.isfile(out_path):
        censor_data = np.loadtxt(out_path)
        n_total = len(censor_data)
        n_censored = int(np.sum(censor_data == 0))
        logger.info("Censor file already exists: %s (%d/%d censored)", out_path, n_censored, n_total)
        return out_path, n_censored, n_total

    confounds_df = pd.read_csv(confounds_path, sep="\t")

    if "framewise_displacement" not in confounds_df.columns:
        raise OrchestratorError(
            f"Column 'framewise_displacement' not found in {confounds_path}. "
            f"Available columns: {list(confounds_df.columns)}"
        )

    fd = confounds_df["framewise_displacement"].values

    # Remove initial TRs
    if n_remove > 0:
        fd = fd[n_remove:]

    # Build censor array: 1 = include, 0 = exclude
    censor = np.ones(len(fd), dtype=int)
    for i in range(len(fd)):
        if np.isnan(fd[i]):
            # First volume after trim typically has NaN FD — always include
            censor[i] = 1
        elif fd[i] > fd_threshold:
            censor[i] = 0

    n_total = len(censor)
    n_censored = int(np.sum(censor == 0))
    pct_censored = (n_censored / n_total * 100) if n_total > 0 else 0

    # All thresholds are informational warnings only. Motion-based exclusion
    # is a post-hoc research decision made at the group level; the pipeline
    # always attempts first-level analysis for any subject where AFNI can run.
    if pct_censored > 50:
        logger.warning(
            "%.1f%% of volumes censored (%d/%d) in %s — exceeds 50%%. "
            "First-level analysis will still be attempted; review QC output.",
            pct_censored, n_censored, n_total, confounds_path
        )
    elif pct_censored > 25:
        logger.warning(
            "%.1f%% of volumes censored (%d/%d) in %s — exceeds 25%%.",
            pct_censored, n_censored, n_total, confounds_path
        )
    else:
        logger.info(
            "%.1f%% of volumes censored (%d/%d) in %s",
            pct_censored, n_censored, n_total, confounds_path
        )

    np.savetxt(out_path, censor, fmt="%d")
    return out_path, n_censored, n_total

def extract_tissue_signals(confounds_path, n_remove, tissue_type, out_path, logger):
    """
    Extract a tissue/nuisance signal time series from fMRIPrep confounds.

    Parameters
    ----------
    tissue_type : str
        Column name in the confounds TSV. Common values:
        "csf", "white_matter", "global_signal"

    Returns
    -------
    str
        Path to the tissue signal file.
    """
    if os.path.isfile(out_path):
        logger.info("Tissue signal (%s) already exists: %s", tissue_type, out_path)
        return out_path

    confounds_df = pd.read_csv(confounds_path, sep="\t")

    col_name = tissue_type  # fMRIPrep uses "csf" and "white_matter"
    if col_name not in confounds_df.columns:
        raise OrchestratorError(
            f"Column '{col_name}' not found in {confounds_path}. "
            f"Available columns: {list(confounds_df.columns)}"
        )

    signal = confounds_df[col_name].values

    if n_remove > 0:
        signal = signal[n_remove:]

    np.savetxt(out_path, signal, fmt="%.10g")
    logger.info("Extracted %s signal → %s (%d timepoints)", tissue_type, out_path, len(signal))
    return out_path

# ============================================================================
# Section H: Task Timing
# ============================================================================

def fix_nback_cue_labels(events_path, condition_column, out_path, logger):
    """
    Relabel generic "cue"/"Cue" trial types in n-back events files.

    In the ABCD n-back task, cue trials are labeled with a non-descriptive
    "cue" (or "Cue") trial_type. The actual stimulus condition shown during
    the cue is only identifiable from the block of trials that follows it
    (e.g. "0_back_posface", "2_back_place"). This function replaces each
    cue's trial_type based on the n-back level of the following block:

    - **0-back cues** are passive viewing events where the subject sees the
      target stimulus. These are relabeled with the bare condition name
      (e.g. "posface", "place", "neutface", "negface").
    - **2-back cues** are instruction screens that tell the subject to begin
      the 2-back recall task. These are relabeled as "instruction".

    The subsequent recall trials (e.g. "0_back_posface", "2_back_place")
    are left unchanged.

    Parameters
    ----------
    events_path : str
        Path to the raw BIDS events TSV file.
    condition_column : str
        Column name containing trial types (typically "trial_type").
    out_path : str
        Destination path for the relabeled events file.
    logger : logging.Logger

    Returns
    -------
    str
        Path to the relabeled events file.

    Raises
    ------
    OrchestratorError
        If a cue trial has no subsequent non-cue trial to infer its condition.
    """
    if os.path.isfile(out_path):
        logger.info("Relabeled n-back events file already exists: %s", out_path)
        return out_path

    events_df = pd.read_csv(events_path, sep="\t")

    if condition_column not in events_df.columns:
        raise OrchestratorError(
            f"Condition column '{condition_column}' not found in {events_path}. "
            f"Available columns: {list(events_df.columns)}"
        )

    # Pattern to extract n-back level and condition from trial types like "0_back_posface"
    nback_pattern = re.compile(r"^(\d+)_back_(.+)$")

    n_relabeled = 0
    n_instruction = 0
    for i in range(len(events_df)):
        trial_type = str(events_df.at[i, condition_column]).strip().strip('"')
        if trial_type.lower() != "cue":
            continue

        # Look ahead for the first non-cue, non-dummy trial to infer condition
        match = None
        for j in range(i + 1, len(events_df)):
            next_type = str(events_df.at[j, condition_column]).strip().strip('"')
            match = nback_pattern.match(next_type)
            if match:
                break

        if match is None:
            raise OrchestratorError(
                f"Cannot determine cue condition for row {i} in {events_path}: "
                f"no subsequent n-back trial found after cue at onset "
                f"{events_df.at[i, 'onset']}."
            )

        level = match.group(1)      # "0" or "2"
        condition = match.group(2)  # "posface", "place", etc.

        if level == "0":
            # 0-back cues are passive viewing — label with bare condition
            events_df.at[i, condition_column] = condition
        else:
            # 2-back (and any other level) cues are instruction screens
            events_df.at[i, condition_column] = "instruction"
            n_instruction += 1

        n_relabeled += 1

    events_df.to_csv(out_path, sep="\t", index=False)
    logger.info(
        "Relabeled %d cue trial(s) (%d as stimulus conditions, %d as 'instruction') "
        "in n-back events file: %s → %s",
        n_relabeled, n_relabeled - n_instruction, n_instruction,
        os.path.basename(events_path), os.path.basename(out_path)
    )
    return out_path


def format_task_timing(events_path, condition_column, conditions_include, conditions_exclude, n_remove, TR, out_path, logger):

    """
    Convert BIDS events.tsv to first-level timing CSV (CONDITION, ONSET, DURATION).

    Adjusts onsets for removed non-steady-state TRs and drops events that
    occur during removed timepoints.

    Returns
    -------
    tuple of (str, int)
        (path to timing CSV, number of events dropped due to onset adjustment)
    """
    if os.path.isfile(out_path):
        logger.info("Task timing already exists: %s", out_path)
        existing = pd.read_csv(out_path)
        return out_path, 0

    events_df = pd.read_csv(events_path, sep="\t")

    if condition_column not in events_df.columns:
        raise OrchestratorError(
            f"Condition column '{condition_column}' not found in {events_path}. "
            f"Available columns: {list(events_df.columns)}"
        )

    if "onset" not in events_df.columns or "duration" not in events_df.columns:
        raise OrchestratorError(
            f"Events file {events_path} must contain 'onset' and 'duration' columns. "
            f"Available: {list(events_df.columns)}"
        )

    # Filter conditions
    if conditions_include is not None:
        events_df = events_df[events_df[condition_column].isin(conditions_include)]

    if conditions_exclude is not None:
        events_df = events_df[~events_df[condition_column].isin(conditions_exclude)]

    if len(events_df) == 0:
        raise OrchestratorError(f"No events remain after filtering in {events_path}")

    # Build output timing dataframe
    timing_df = pd.DataFrame({
        "CONDITION": events_df[condition_column].values,
        "ONSET": events_df["onset"].values,
        "DURATION": events_df["duration"].values,
    })

    # Adjust onsets for removed TRs
    time_offset = n_remove * TR
    timing_df["ONSET"] = timing_df["ONSET"] - time_offset

    # Drop events where adjusted onset < 0
    negative_mask = timing_df["ONSET"] < 0
    n_dropped = int(negative_mask.sum())
    if n_dropped > 0:
        logger.warning(
            "%d event(s) dropped from %s because onset < 0 after removing "
            "%d non-steady-state TR(s) (%.2fs offset)",
            n_dropped, events_path, n_remove, time_offset
        )
        timing_df = timing_df[~negative_mask]

    timing_df = timing_df.sort_values("ONSET").reset_index(drop=True)
    timing_df.to_csv(out_path, index=False)
    logger.info("Formatted task timing → %s (%d events)", out_path, len(timing_df))
    return out_path, n_dropped

# ============================================================================
# Section I: Run Concatenation
# ============================================================================

def concatenate_bolds(bold_paths, out_path, logger):
    """
    Concatenate multiple BOLD runs using AFNI's 3dTcat.
    Single-run: copies file instead of running 3dTcat.

    Returns
    -------
    str
        Path to the concatenated BOLD file.
    """
    if os.path.isfile(out_path):
        logger.info("Concatenated BOLD already exists: %s", out_path)
        return out_path

    if len(bold_paths) == 1:
        shutil.copy2(bold_paths[0], out_path)
        logger.info("Single run — copied BOLD to %s", out_path)
        return out_path

    cmd = ["3dTcat", "-prefix", out_path] + list(bold_paths)

    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise OrchestratorError(
            f"3dTcat concatenation failed: {e.stderr.strip() if e.stderr else str(e)}"
        )

    if not os.path.isfile(out_path):
        raise OrchestratorError(f"3dTcat produced no output: {out_path}")

    logger.info("Concatenated %d BOLD runs → %s", len(bold_paths), out_path)
    return out_path

def concatenate_tabular_files(file_paths, out_path, logger):
    """
    Concatenate tabular files (motion, censor, tissue signals) by row stacking.

    Handles both multi-column files (motion regressors) and single-column
    files (censor vectors, tissue signals) without format corruption:
      - Multi-column: written tab-delimited (AFNI 1D multi-column format)
      - Single-column: written as a plain integer or float column vector
        with no trailing delimiter, compatible with AFNI censor 1D format

    Returns
    -------
    str
        Path to the concatenated file.
    """
    if os.path.isfile(out_path):
        logger.info("Concatenated tabular file already exists: %s", out_path)
        return out_path

    if len(file_paths) == 1:
        shutil.copy2(file_paths[0], out_path)
        logger.info("Single run — copied tabular file to %s", out_path)
        return out_path

    arrays = []
    for fp in file_paths:
        data = np.loadtxt(fp)
        # Normalise to 2D for consistent stacking regardless of input shape
        if data.ndim == 1:
            data = data.reshape(-1, 1)
        arrays.append(data)

    stacked = np.vstack(arrays)
    n_cols = stacked.shape[1]

    if n_cols == 1:
        # Single-column file: write as plain column vector.
        # Use integer format if all values are integers (e.g. censor files),
        # otherwise float format (e.g. tissue signal regressors).
        col = stacked[:, 0]
        if np.all(col == col.astype(int)):
            np.savetxt(out_path, col.astype(int), fmt="%d")
        else:
            np.savetxt(out_path, col, fmt="%.10g")
    else:
        np.savetxt(out_path, stacked, fmt="%.10g", delimiter="\t")

    logger.info(
        "Concatenated %d tabular files → %s (%d rows, %d col(s))",
        len(file_paths), out_path, stacked.shape[0], n_cols
    )
    return out_path

def concatenate_task_timing(timing_paths, run_tr_counts, TR, out_path, logger):
    """
    Concatenate task timing CSVs, adjusting onsets for cumulative run lengths.

    Parameters
    ----------
    timing_paths : list of str
        Per-run timing CSV paths (CONDITION, ONSET, DURATION).
    run_tr_counts : list of int
        Number of TRs in each run (after trimming).
    TR : float
        Repetition time in seconds.

    Returns
    -------
    str
        Path to the concatenated timing CSV.
    """
    if os.path.isfile(out_path):
        logger.info("Concatenated timing already exists: %s", out_path)
        return out_path

    if len(timing_paths) == 1:
        shutil.copy2(timing_paths[0], out_path)
        logger.info("Single run — copied timing to %s", out_path)
        return out_path

    all_dfs = []
    cumulative_offset = 0.0

    for i, tp in enumerate(timing_paths):
        df = pd.read_csv(tp)
        df["ONSET"] = df["ONSET"] + cumulative_offset
        all_dfs.append(df)
        cumulative_offset += run_tr_counts[i] * TR

    concat_df = pd.concat(all_dfs, ignore_index=True)
    concat_df = concat_df.sort_values("ONSET").reset_index(drop=True)
    concat_df.to_csv(out_path, index=False)
    logger.info("Concatenated %d timing files → %s (%d events)", len(timing_paths), out_path, len(concat_df))
    return out_path

# ============================================================================
# Section J: Smoothing
# ============================================================================

def apply_smoothing(bold_path, mask_path, method, fwhm, out_dir, out_prefix, logger):
    """
    Apply spatial smoothing to BOLD data.

    Parameters
    ----------
    method : str
        "3dmerge" or "3dBlurToFWHM"
    fwhm : float
        Target FWHM in mm.

    Returns
    -------
    str
        Path to the smoothed BOLD file.
    """
    out_path = os.path.join(out_dir, f"{out_prefix}_smoothed.nii.gz")

    if os.path.isfile(out_path):
        logger.info("Smoothed BOLD already exists: %s", out_path)
        return out_path

    if method == "3dmerge":
        cmd = [
            "3dmerge",
            "-1blur_fwhm", str(fwhm),
            "-doall",
            "-prefix", out_path,
            bold_path,
        ]
    elif method == "3dBlurToFWHM":
        cmd = [
            "3dBlurToFWHM",
            "-FWHM", str(fwhm),
            "-mask", mask_path,
            "-input", bold_path,
            "-prefix", out_path,
        ]
    else:
        raise OrchestratorError(
            f"Invalid smoothing method '{method}'. Must be '3dmerge' or '3dBlurToFWHM'."
        )

    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise OrchestratorError(
            f"Smoothing failed ({method}): {e.stderr.strip() if e.stderr else str(e)}"
        )

    if not os.path.isfile(out_path):
        raise OrchestratorError(f"Smoothing produced no output: {out_path}")

    logger.info("Smoothing (%s, FWHM=%.1f mm) complete: %s", method, fwhm, out_path)
    return out_path

# ============================================================================
# Section K: QC — Preprocessing
# ============================================================================

def compute_tsnr(bold_path, mask_path, out_dir, out_prefix, logger):
    """
    Compute temporal SNR (mean/stdev across time) within the brain mask.

    Returns
    -------
    float
        Median brain tSNR.
    """
    mean_path = os.path.join(out_dir, f"{out_prefix}_tsnr_mean.nii.gz")
    stdev_path = os.path.join(out_dir, f"{out_prefix}_tsnr_stdev.nii.gz")
    tsnr_path = os.path.join(out_dir, f"{out_prefix}_tsnr.nii.gz")

    try:
        # Compute mean across time
        subprocess.run(
            ["3dTstat", "-mean", "-prefix", mean_path, bold_path],
            capture_output=True, text=True, check=True,
        )
        # Compute stdev across time
        subprocess.run(
            ["3dTstat", "-stdev", "-prefix", stdev_path, bold_path],
            capture_output=True, text=True, check=True,
        )
        # tSNR = mean / stdev (within mask)
        subprocess.run(
            ["3dcalc",
             "-a", mean_path, "-b", stdev_path, "-c", mask_path,
             "-expr", "step(c)*a/max(b,0.001)",
             "-prefix", tsnr_path],
            capture_output=True, text=True, check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.warning("tSNR computation failed: %s", e.stderr.strip() if e.stderr else str(e))
        return None

    # Get median tSNR within mask
    try:
        result = subprocess.run(
            ["3dBrickStat", "-mask", mask_path, "-percentile", "50", "1", "50", tsnr_path],
            capture_output=True, text=True, check=True,
        )
        # 3dBrickStat -percentile outputs: value1 percentile value2
        parts = result.stdout.strip().split()
        median_tsnr = float(parts[1]) if len(parts) >= 2 else float(parts[0])
    except (subprocess.CalledProcessError, ValueError, IndexError):
        logger.warning("Could not compute median tSNR from %s", tsnr_path)
        return None

    # Clean up intermediate files
    for f in [mean_path, stdev_path]:
        if os.path.isfile(f):
            os.remove(f)

    logger.info("Median brain tSNR = %.2f", median_tsnr)
    return median_tsnr

def generate_carpet_plot(bold_path, mask_path, confounds_path, n_remove, fd_threshold, out_path, logger):

    """
    Generate a carpet plot: FD/DVARS traces on top, voxel x time heatmap below.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.gridspec import GridSpec
    except ImportError:
        logger.warning("matplotlib not available — skipping carpet plot generation.")
        return

    confounds_df = pd.read_csv(confounds_path, sep="\t")

    # Get FD and DVARS
    fd = confounds_df.get("framewise_displacement", pd.Series(dtype=float)).values
    dvars = confounds_df.get("dvars", pd.Series(dtype=float)).values

    if n_remove > 0:
        fd = fd[n_remove:]
        dvars = dvars[n_remove:]

    n_vols = len(fd)
    time_axis = np.arange(n_vols)

    # Extract voxel timeseries within mask using 3dmaskdump
    try:
        result = subprocess.run(
            ["3dmaskdump", "-mask", mask_path, "-noijk", "-quiet", bold_path],
            capture_output=True, text=True, check=True,
        )
        lines = result.stdout.strip().split("\n")
        # Subsample voxels for a manageable plot
        n_voxels = len(lines)
        max_voxels = 2000
        if n_voxels > max_voxels:
            indices = np.linspace(0, n_voxels - 1, max_voxels, dtype=int)
            lines = [lines[i] for i in indices]

        voxel_data = np.array([list(map(float, line.split())) for line in lines])
    except (subprocess.CalledProcessError, ValueError):
        logger.warning("Could not extract voxel data for carpet plot — skipping.")
        return

    # Z-score normalize each voxel
    vox_mean = np.mean(voxel_data, axis=1, keepdims=True)
    vox_std = np.std(voxel_data, axis=1, keepdims=True)
    vox_std[vox_std == 0] = 1
    voxel_z = (voxel_data - vox_mean) / vox_std

    # Create figure
    fig = plt.figure(figsize=(12, 8))
    gs = GridSpec(3, 1, height_ratios=[1, 1, 4], hspace=0.3)

    # FD trace
    ax_fd = fig.add_subplot(gs[0])
    ax_fd.plot(time_axis, fd, color="black", linewidth=0.5)
    ax_fd.axhline(y=fd_threshold, color="red", linestyle="--", linewidth=0.8, alpha=0.7)
    ax_fd.set_ylabel("FD (mm)")
    ax_fd.set_xlim(0, n_vols - 1)
    ax_fd.set_xticklabels([])

    # DVARS trace
    ax_dvars = fig.add_subplot(gs[1])
    ax_dvars.plot(time_axis, dvars, color="black", linewidth=0.5)
    ax_dvars.set_ylabel("DVARS")
    ax_dvars.set_xlim(0, n_vols - 1)
    ax_dvars.set_xticklabels([])

    # Carpet plot
    ax_carpet = fig.add_subplot(gs[2])
    ax_carpet.imshow(voxel_z, aspect="auto", cmap="gray", interpolation="none",
                     vmin=-2, vmax=2)
    ax_carpet.set_xlabel("Volume")
    ax_carpet.set_ylabel("Voxels")

    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Carpet plot saved: %s", out_path)

def compute_registration_quality(func_mask_path, anat_mask_path, logger):
    """
    Compute Dice coefficient between functional and anatomical brain masks.

    Returns
    -------
    float or None
        Dice coefficient (0-1), or None if computation fails.
    """
    try:
        # Count voxels in func mask
        r1 = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", func_mask_path],
            capture_output=True, text=True, check=True,
        )
        n_func = float(r1.stdout.strip())

        # Count voxels in anat mask
        r2 = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", anat_mask_path],
            capture_output=True, text=True, check=True,
        )
        n_anat = float(r2.stdout.strip())

        # Count intersection
        overlap_path = os.path.join(os.path.dirname(func_mask_path), "_temp_overlap.nii.gz")
        subprocess.run(
            ["3dcalc", "-a", func_mask_path, "-b", anat_mask_path,
             "-expr", "step(a)*step(b)", "-prefix", overlap_path],
            capture_output=True, text=True, check=True,
        )
        r3 = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", overlap_path],
            capture_output=True, text=True, check=True,
        )
        n_overlap = float(r3.stdout.strip())

        # Clean up
        if os.path.isfile(overlap_path):
            os.remove(overlap_path)

        dice = 2 * n_overlap / (n_func + n_anat) if (n_func + n_anat) > 0 else 0.0
        logger.info("Registration quality (Dice): %.4f", dice)
        return dice

    except (subprocess.CalledProcessError, ValueError) as e:
        logger.warning("Could not compute registration quality: %s", str(e))
        return None

def compute_preproc_qc(run_info, confounds_path, bold_path, mask_path, n_remove, TR, fd_threshold, qc_config, out_dir, sub_id, space, logger):

    """
    Compute preprocessing QC metrics for a single run.

    The censor section records n_total_trs, n_censored_trs, n_clean_trs,
    pct_censored, and clean_time_seconds. These fields are designed for
    post-hoc group-level exclusion decisions (e.g. max 30% censored,
    min 12 minutes of clean data). No motion-based gating is applied here.

    Parameters
    ----------
    TR : float
        Repetition time in seconds (used to compute clean_time_seconds).

    Returns
    -------
    dict
        QC metrics dictionary.
    """
    qc = {
        "sub_id": sub_id,
        "session": run_info.get("session"),
        "task": run_info["task_label"],
        "run": run_info["run"],
        "non_steady_state_trs": n_remove,
    }

    confounds_df = pd.read_csv(confounds_path, sep="\t")

    # -- Motion metrics --
    fd = confounds_df.get("framewise_displacement", pd.Series(dtype=float)).values
    if n_remove > 0:
        fd = fd[n_remove:]
    fd_valid = fd[~np.isnan(fd)]

    n_total_trs = len(fd)
    n_censored_trs = int(np.sum(fd_valid > fd_threshold)) if len(fd_valid) > 0 else 0
    n_clean_trs = n_total_trs - n_censored_trs
    pct_censored = (n_censored_trs / n_total_trs * 100) if n_total_trs > 0 else None
    clean_time_seconds = round(n_clean_trs * TR, 2)

    qc["motion"] = {
        "mean_fd": float(np.mean(fd_valid)) if len(fd_valid) > 0 else None,
        "max_fd": float(np.max(fd_valid)) if len(fd_valid) > 0 else None,
        "median_fd": float(np.median(fd_valid)) if len(fd_valid) > 0 else None,
    }

    # Censor stats: primary fields for post-hoc group-level exclusion
    qc["censor"] = {
        "fd_threshold_mm": fd_threshold,
        "n_total_trs": n_total_trs,
        "n_censored_trs": n_censored_trs,
        "n_clean_trs": n_clean_trs,
        "pct_censored": round(pct_censored, 2) if pct_censored is not None else None,
        "clean_time_seconds": clean_time_seconds,
    }

    # -- DVARS metrics --
    dvars = confounds_df.get("dvars", pd.Series(dtype=float)).values
    if n_remove > 0:
        dvars = dvars[n_remove:]
    dvars_valid = dvars[~np.isnan(dvars)]

    qc["dvars"] = {
        "mean": float(np.mean(dvars_valid)) if len(dvars_valid) > 0 else None,
        "max": float(np.max(dvars_valid)) if len(dvars_valid) > 0 else None,
    }

    # -- tSNR --
    if qc_config.get("tsnr", False):
        run_label = run_info["run_label"]
        median_tsnr = compute_tsnr(bold_path, mask_path, out_dir,
                                   f"{sub_id}_{run_label}", logger)
        qc["tsnr"] = {"median_brain": median_tsnr}
    else:
        qc["tsnr"] = {"median_brain": None}

    # -- Brain mask coverage --
    try:
        result = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", mask_path],
            capture_output=True, text=True, check=True,
        )
        n_voxels = int(float(result.stdout.strip()))

        # Get voxel volume
        result2 = subprocess.run(
            ["3dinfo", "-ad3", mask_path],
            capture_output=True, text=True, check=True,
        )
        spacings = result2.stdout.strip().split()
        voxel_vol = 1.0
        for s in spacings:
            voxel_vol *= float(s)
        volume_mm3 = n_voxels * voxel_vol

        qc["brain_mask"] = {"n_voxels": n_voxels, "volume_mm3": round(volume_mm3, 2)}
    except (subprocess.CalledProcessError, ValueError):
        qc["brain_mask"] = {"n_voxels": None, "volume_mm3": None}

    # -- Carpet plot --
    if qc_config.get("carpet_plots", False):
        carpet_path = os.path.join(out_dir, f"{sub_id}_{run_info['run_label']}_carpet.png")
        generate_carpet_plot(bold_path, mask_path, confounds_path, n_remove,
                             fd_threshold, carpet_path, logger)
        qc["carpet_plot_path"] = carpet_path
    else:
        qc["carpet_plot_path"] = None

    # -- Registration quality --
    if qc_config.get("registration_quality", False):
        # Look for anatomical brain mask in fMRIPrep
        fmriprep_sub_dir = os.path.dirname(os.path.dirname(mask_path))
        ses_part = run_info.get("session")
        if ses_part:
            anat_dir = os.path.join(fmriprep_sub_dir, ses_part, "anat")
        else:
            anat_dir = os.path.join(fmriprep_sub_dir, "anat")

        # Find anatomical brain mask matching the template space.
        # Filter on space-{space} entity to avoid matching masks in a
        # different space (e.g. T1w) that are also present in the anat dir.
        anat_mask = None
        if os.path.isdir(anat_dir):
            space_tag = f"space-{space}"
            candidates = [
                f for f in os.listdir(anat_dir)
                if "desc-brain_mask" in f
                and space_tag in f
                and f.endswith(".nii.gz")
            ]
            if candidates:
                # Prefer exact match; fall back to first candidate
                anat_mask = os.path.join(anat_dir, sorted(candidates)[0])
                if len(candidates) > 1:
                    logger.warning(
                        "Multiple anat brain masks found for space '%s' in %s; "
                        "using: %s", space, anat_dir, candidates[0]
                    )

        if anat_mask is not None:
            dice = compute_registration_quality(mask_path, anat_mask, logger)
            qc["registration"] = {"dice": dice, "anat_mask": anat_mask}
        else:
            logger.info(
                "No anatomical brain mask found for space '%s' in %s — "
                "skipping registration quality check.", space, anat_dir
            )
            qc["registration"] = {"dice": None, "anat_mask": None}
    else:
        qc["registration"] = {"dice": None}

    return qc

def save_qc_json(qc_metrics, out_path, logger):
    """Save QC metrics dict to a JSON file."""
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(qc_metrics, f, indent=2, default=str)
    logger.info("QC metrics saved: %s", out_path)

def compute_first_level_qc(analysis_name, analysis_type, out_dir, sub_id, censor_proportion, logger, error_msg=None):

    """
    Compute first-level analysis QC metrics.

    Checks for expected output files by analysis type to determine whether
    the analysis completed successfully. A QC JSON is always written even for
    failed analyses so failures are visible in group-level QC aggregation.

    Expected outputs by type:
      task_act  — at least one .nii.gz (stat bucket) in the analysis directory
      task_conn — at least one .nii.gz (beta series) in the analysis directory
      rest_conn — at least one .nii.gz (residual/connectivity) in the analysis directory

    Parameters
    ----------
    error_msg : str or None
        Error message if the analysis raised an exception; None on success.

    Returns
    -------
    dict
        QC metrics dictionary.
    """
    analysis_dir = os.path.join(out_dir, analysis_name)
    output_files = sorted(os.listdir(analysis_dir)) if os.path.isdir(analysis_dir) else []

    # Determine success: at least one non-empty .nii.gz must be present
    nifti_outputs = [
        f for f in output_files
        if f.endswith(".nii.gz") and
        os.path.getsize(os.path.join(analysis_dir, f)) > 0
    ]
    completed_successfully = len(nifti_outputs) > 0 and error_msg is None

    if not completed_successfully:
        if error_msg:
            logger.warning(
                "First-level QC: analysis '%s' marked as failed — %s",
                analysis_name, error_msg
            )
        else:
            logger.warning(
                "First-level QC: analysis '%s' produced no non-empty NIfTI outputs "
                "in %s — marking as failed.",
                analysis_name, analysis_dir
            )
    else:
        logger.info(
            "First-level QC: analysis '%s' completed successfully (%d NIfTI output(s)).",
            analysis_name, len(nifti_outputs)
        )

    qc = {
        "sub_id": sub_id,
        "analysis_name": analysis_name,
        "type": analysis_type,
        "pct_censored": round(censor_proportion * 100, 2) if censor_proportion is not None else None,
        "completed_successfully": completed_successfully,
        "error": error_msg,
        "n_nifti_outputs": len(nifti_outputs),
        "output_files": output_files,
    }

    return qc

# ============================================================================
# Section L: Config Building
# ============================================================================

def build_first_level_config(sub_id, session, study_config, task_defs, processed_files, analyses, proc_template, logger):
    """
    Build a first-level config by deep-copying the proc template and overriding
    only subject-specific fields (paths, output dirs, prefixes).

    All analysis-level settings (hrf_model, contrasts, bandpass, etc.) are
    preserved verbatim from the proc template.

    Parameters
    ----------
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    study_config : dict
        The 'study' section of the orchestrator config.
    task_defs : list of dict
        Task definitions from the orchestrator config.
    processed_files : dict
        Mapping of task_label -> processed file info.
        For task analyses (concatenated):
            {"bold": path, "motion": path, "censor": {fd_thresh: path}, "timing": path}
        For rest analyses (per-run):
            {"bolds": [paths], "motions": [paths],
             "censors": {fd_thresh: [paths]},
             "csf": [paths], "wm": [paths], "gs": [paths]}
    analyses : list of dict
        The 'analyses' section of the orchestrator config.
    proc_template : dict
        The fmri_first_level_proc config template (deep-copied internally).
    logger : logging.Logger

    Returns
    -------
    dict
        Config dict compatible with first_level_config.load_and_validate.
    """
    config = copy.deepcopy(proc_template)
    output_dir = study_config["output_dir"]
    ses_label = f"ses-{session}A"
    session_out = os.path.join(output_dir, f"sub-{sub_id}", ses_label)

    # Index template analyses by name for fast lookup
    template_by_name = {}
    for block in config.get("analyses", []):
        template_by_name[block["name"]] = block

    # Track which template analyses are referenced by the orchestrator
    referenced_names = set()

    for orch_analysis in analyses:
        analysis_name = orch_analysis["name"]
        analysis_type = orch_analysis["type"]
        task_label = orch_analysis["task_label"]
        fd_threshold = round(orch_analysis["fd_threshold"], 4)
        referenced_names.add(analysis_name)

        # Find matching template block
        if analysis_name not in template_by_name:
            raise OrchestratorError(
                f"Analysis '{analysis_name}' not found in proc template. "
                f"Available template analyses: {list(template_by_name.keys())}"
            )

        block = template_by_name[analysis_name]

        # Get processed files for this task
        pf = processed_files.get(task_label)
        if pf is None:
            raise OrchestratorError(
                f"No processed files found for task '{task_label}' "
                f"(analysis '{analysis_name}')."
            )

        # Session-aware output directory and file prefix
        block["out_dir"] = os.path.join(session_out, "first_level_out", analysis_name)
        prefix_base = f"sub-{sub_id}_{ses_label}"
        block["out_file_pre"] = f"{prefix_base}_{orch_analysis['post_id_out_pre']}"

        # Override paths based on analysis type
        if analysis_type in ("task_act", "task_conn"):
            # Get censor file for this analysis's FD threshold
            censor_path = pf["censor"].get(fd_threshold)
            if censor_path is None:
                raise OrchestratorError(
                    f"No censor file for fd_threshold={fd_threshold} "
                    f"(analysis '{analysis_name}', task '{task_label}')."
                )

            block["paths"] = {
                "scan_path": pf["bold"],
                "task_timing_path": pf["timing"],
                "motion_path": pf["motion"],
                "censor_path": censor_path,
            }

        elif analysis_type == "rest_conn":
            # Get censor files for this analysis's FD threshold
            censor_paths = pf["censors"].get(fd_threshold)
            if censor_paths is None:
                raise OrchestratorError(
                    f"No censor files for fd_threshold={fd_threshold} "
                    f"(analysis '{analysis_name}', task '{task_label}')."
                )

            block["paths"] = {
                "scan_paths": pf["bolds"],
                "motion_paths": pf["motions"],
                "censor_paths": censor_paths,
                "CSF_paths": pf["csf"],
                "WM_paths": pf["wm"],
                "GS_paths": pf.get("gs"),
            }

        # Override extraction prefix if present
        if "post_id_extract_pre" in orch_analysis and "extraction" in block:
            block["extraction"]["extract_out_file_pre"] = (
                f"{prefix_base}_{orch_analysis['post_id_extract_pre']}"
            )

        # Override connectivity prefix if present
        if "post_id_conn_pre" in orch_analysis and "connectivity" in block:
            block["connectivity"]["conn_out_file_pre"] = (
                f"{prefix_base}_{orch_analysis['post_id_conn_pre']}"
            )

    # Remove template analyses not referenced by orchestrator
    unreferenced = [name for name in template_by_name if name not in referenced_names]
    if unreferenced:
        logger.warning(
            "Removing %d proc template analysis block(s) not referenced by "
            "orchestrator config: %s",
            len(unreferenced), unreferenced
        )
        config["analyses"] = [
            b for b in config["analyses"] if b["name"] in referenced_names
        ]

    return config

def write_temp_config(config_dict, out_dir, sub_id, session, logger):
    """
    Write a generated first-level config dict to a YAML file.

    Parameters
    ----------
    session : str
        Session code (e.g. "00").

    Returns
    -------
    str
        Path to the written YAML file.
    """
    ses_label = f"ses-{session}A"
    out_path = os.path.join(out_dir, f"sub-{sub_id}_{ses_label}_first_level_config.yaml")
    os.makedirs(out_dir, exist_ok=True)

    with open(out_path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    logger.info("First-level config written: %s", out_path)
    return out_path

# ============================================================================
# Section M: Orchestrator Config Validation
# ============================================================================

def load_orchestrator_config(config_path, logger):
    """
    Load and validate the orchestrator YAML config.

    Returns
    -------
    dict
        Validated config dictionary.
    """
    if not os.path.isfile(config_path):
        raise OrchestratorError(f"Orchestrator config not found: {config_path}")

    with open(config_path, "r") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise OrchestratorError(f"YAML parse error in {config_path}: {e}")

    if config is None:
        raise OrchestratorError(f"Config file is empty: {config_path}")

    # Validate required top-level sections
    required_sections = ["study", "tasks", "analyses"]
    for section in required_sections:
        if section not in config:
            raise OrchestratorError(f"Config missing required section '{section}'.")

    study = config["study"]

    # Validate required study keys (bids_dir and sessions removed — ABCD uses
    # S3-based events and dynamic session discovery)
    required_study_keys = ["fmriprep_dir", "output_dir", "space", "TR"]
    for key in required_study_keys:
        if key not in study or study[key] is None:
            raise OrchestratorError(f"Config study section missing required key '{key}'.")

    # Validate TR is positive
    if not isinstance(study["TR"], (int, float)) or study["TR"] <= 0:
        raise OrchestratorError(f"study.TR must be a positive number, got: {study['TR']}")

    # Validate tasks
    tasks = config["tasks"]
    if not isinstance(tasks, list) or len(tasks) == 0:
        raise OrchestratorError("'tasks' must be a non-empty list.")

    task_labels = set()
    for i, task in enumerate(tasks):
        if "task_label" not in task:
            raise OrchestratorError(f"tasks[{i}] missing required key 'task_label'.")
        # Runs are now discovered dynamically from S3 — not required in config
        task_labels.add(task["task_label"])

    # Validate analyses
    analyses = config["analyses"]
    if not isinstance(analyses, list) or len(analyses) == 0:
        raise OrchestratorError("'analyses' must be a non-empty list.")

    valid_types = {"task_act", "task_conn", "rest_conn"}

    for i, analysis in enumerate(analyses):
        name = analysis.get("name", f"analyses[{i}]")

        if "type" not in analysis:
            raise OrchestratorError(f"[{name}] Missing required key 'type'.")
        if analysis["type"] not in valid_types:
            raise OrchestratorError(
                f"[{name}] Invalid type '{analysis['type']}'. Must be one of {valid_types}."
            )

        if "task_label" not in analysis:
            raise OrchestratorError(f"[{name}] Missing required key 'task_label'.")
        if analysis["task_label"] not in task_labels:
            raise OrchestratorError(
                f"[{name}] task_label '{analysis['task_label']}' not defined in tasks section. "
                f"Available: {task_labels}"
            )

        # Validate post_id_out_pre is present for all analyses
        if "post_id_out_pre" not in analysis:
            raise OrchestratorError(
                f"[{name}] Missing required key 'post_id_out_pre'."
            )

        # Validate per-analysis fd_threshold (required, positive float)
        if "fd_threshold" not in analysis:
            raise OrchestratorError(
                f"[{name}] Missing required key 'fd_threshold'."
            )
        fd_val = analysis["fd_threshold"]
        if not isinstance(fd_val, (int, float)) or fd_val <= 0:
            raise OrchestratorError(
                f"[{name}] fd_threshold must be a positive number, got: {fd_val}"
            )

        atype = analysis["type"]

        # Validate post_id_conn_pre for connectivity types
        if atype in ("task_conn", "rest_conn"):
            if "post_id_conn_pre" not in analysis:
                raise OrchestratorError(
                    f"[{name}] {atype} analysis requires 'post_id_conn_pre'."
                )

        # Deprecation warnings for old fields that now belong in proc template
        _deprecated_fields = [
            "hrf_model", "custom_hrf", "include_motion_derivs", "cond_labels",
            "cond_beta_labels", "contrasts", "bandpass", "motion_deriv_degree",
            "template_path", "average_type", "extraction", "connectivity",
            "extract_out_file_suffix", "conn_out_file_suffix",
        ]
        for field in _deprecated_fields:
            if field in analysis:
                logger.warning(
                    "[%s] Field '%s' in orchestrator analysis block is deprecated. "
                    "This field should now be set in the proc template config. "
                    "It will be ignored by the orchestrator.",
                    name, field
                )

    # Validate smoothing if present
    smoothing = config.get("smoothing", {})
    if smoothing and smoothing.get("enabled", False):
        method = smoothing.get("method")
        if method not in ("3dmerge", "3dBlurToFWHM"):
            raise OrchestratorError(
                f"smoothing.method must be '3dmerge' or '3dBlurToFWHM', got '{method}'."
            )
        fwhm = smoothing.get("fwhm")
        if not isinstance(fwhm, (int, float)) or fwhm <= 0:
            raise OrchestratorError(f"smoothing.fwhm must be a positive number, got: {fwhm}")

    # Validate S3 config if enabled
    s3_cfg = config.get("s3", {})
    if s3_cfg.get("enabled", False):
        required_s3_fields = [
            "bucket", "fmriprep_s3_prefix", "mmps_mproc_s3_prefix", "upload_prefix"
        ]
        for field in required_s3_fields:
            if not s3_cfg.get(field):
                raise OrchestratorError(
                    f"s3.enabled is true but required field 's3.{field}' is null or missing."
                )

        if s3_cfg["bucket"].startswith("s3://"):
            raise OrchestratorError(
                f"s3.bucket must be the bucket name only (no 's3://' prefix), "
                f"got: {s3_cfg['bucket']}"
            )

        for prefix_field in ["fmriprep_s3_prefix", "mmps_mproc_s3_prefix", "upload_prefix"]:
            val = s3_cfg.get(prefix_field, "")
            if val and (val.startswith("/") or val.endswith("/")):
                raise OrchestratorError(
                    f"s3.{prefix_field} must not have a leading or trailing '/', got: {val}"
                )

        # Validate available_sessions
        avail_sessions = s3_cfg.get("available_sessions")
        if not isinstance(avail_sessions, list) or len(avail_sessions) == 0:
            raise OrchestratorError(
                "s3.available_sessions must be a non-empty list of session codes "
                "(e.g. ['00', '02', '04', '06'])."
            )
        for s in avail_sessions:
            if not isinstance(s, str):
                raise OrchestratorError(
                    f"s3.available_sessions entries must be strings, got: {s!r}"
                )

    # Validate calc_n_motion_derivs
    study.setdefault("calc_n_motion_derivs", 1)
    if not isinstance(study["calc_n_motion_derivs"], int) or study["calc_n_motion_derivs"] < 0:
        raise OrchestratorError(
            f"study.calc_n_motion_derivs must be a non-negative integer, "
            f"got: {study['calc_n_motion_derivs']}"
        )

    # Set defaults for optional sections
    config.setdefault("smoothing", {"enabled": False})
    config.setdefault("qc", {"preproc": {"enabled": False}, "first_level": {"enabled": False}})
    config.setdefault("s3", {"enabled": False})
    config["s3"].setdefault("cleanup_after_upload", True)
    config["s3"].setdefault("available_sessions", [])

    logger.info("Orchestrator config validated successfully.")
    return config


def validate_proc_template(orchestrator_config, proc_template, logger):
    """
    Cross-validate the orchestrator config against the proc template.

    Checks:
    1. Proc template has 'analyses' as a non-empty list.
    2. Every orchestrator analysis 'name' has a matching entry in the proc template.
    3. 'type' fields match for each name-matched pair.
    4. 'post_id_out_pre' is present for every orchestrator analysis.
    5. 'post_id_extract_pre' is present when the matched proc template analysis
       has an 'extraction' block.
    6. 'post_id_conn_pre' is present for 'task_conn' and 'rest_conn' types.
    7. Warns about template analyses not referenced in orchestrator config.

    Parameters
    ----------
    orchestrator_config : dict
        Validated orchestrator config dict.
    proc_template : dict
        Loaded proc template dict.
    logger : logging.Logger

    Raises
    ------
    OrchestratorError
        On any validation failure.
    """
    # 1. Proc template must have analyses
    if proc_template is None:
        raise OrchestratorError("Proc template config is empty (null).")

    template_analyses = proc_template.get("analyses")
    if not isinstance(template_analyses, list) or len(template_analyses) == 0:
        raise OrchestratorError(
            "Proc template must have 'analyses' as a non-empty list."
        )

    # Index template analyses by name
    template_by_name = {}
    for block in template_analyses:
        bname = block.get("name")
        if bname is None:
            raise OrchestratorError(
                "Proc template has an analysis block without a 'name' field."
            )
        template_by_name[bname] = block

    orch_analyses = orchestrator_config["analyses"]
    referenced_names = set()

    for orch_analysis in orch_analyses:
        name = orch_analysis["name"]
        atype = orch_analysis["type"]
        referenced_names.add(name)

        # 2. Name match
        if name not in template_by_name:
            raise OrchestratorError(
                f"Orchestrator analysis '{name}' has no matching entry in proc template. "
                f"Available template analyses: {list(template_by_name.keys())}"
            )

        tmpl_block = template_by_name[name]

        # 3. Type match
        tmpl_type = tmpl_block.get("type")
        if tmpl_type != atype:
            raise OrchestratorError(
                f"Type mismatch for analysis '{name}': "
                f"orchestrator has '{atype}', proc template has '{tmpl_type}'."
            )

        # 4. post_id_out_pre required for all
        if "post_id_out_pre" not in orch_analysis:
            raise OrchestratorError(
                f"[{name}] Missing required key 'post_id_out_pre'."
            )

        # 5. post_id_extract_pre required when template has extraction block
        if "extraction" in tmpl_block:
            if "post_id_extract_pre" not in orch_analysis:
                raise OrchestratorError(
                    f"[{name}] Proc template has an 'extraction' block but orchestrator "
                    f"analysis is missing 'post_id_extract_pre'."
                )

        # 6. post_id_conn_pre required for task_conn and rest_conn
        if atype in ("task_conn", "rest_conn"):
            if "post_id_conn_pre" not in orch_analysis:
                raise OrchestratorError(
                    f"[{name}] {atype} analysis requires 'post_id_conn_pre'."
                )

    # 7. Warn about unreferenced template analyses
    unreferenced = [n for n in template_by_name if n not in referenced_names]
    if unreferenced:
        logger.warning(
            "Proc template contains %d analysis block(s) not referenced by "
            "orchestrator config (will be removed from generated config): %s",
            len(unreferenced), unreferenced
        )

    logger.info("Proc template cross-validation passed.")


# ============================================================================
# Section N: Output Compression and Local Cleanup
# ============================================================================

def compress_session_outputs(sub_id, session, session_out_dir, logger):
    """
    Compress a session's first-level output directory into a .tar.gz archive.

    Only the first_level_out/ subdirectory is included in the archive. The
    archive is written atomically via a temporary file to prevent corruption.

    Parameters
    ----------
    sub_id : str
        Participant ID (e.g. "NDARABC123").
    session : str
        Session code (e.g. "00").
    session_out_dir : str
        Path to the session output directory (contains first_level_out/).
    logger : logging.Logger

    Returns
    -------
    str
        Path to the created .tar.gz archive.
    """
    archive_path = os.path.join(session_out_dir, "first_level_out.tar.gz")

    if os.path.isfile(archive_path):
        size_mb = os.path.getsize(archive_path) / (1024 * 1024)
        logger.info(
            "Archive already exists — skipping compression: %s (%.1f MB)",
            archive_path, size_mb
        )
        return archive_path

    fl_out_dir = os.path.join(session_out_dir, "first_level_out")
    if not os.path.isdir(fl_out_dir):
        raise FileNotFoundError(
            f"first_level_out directory not found — cannot compress: {fl_out_dir}"
        )

    ses_label = f"ses-{session}A"
    tmp = archive_path + ".tmp"
    try:
        with tarfile.open(tmp, "w:gz") as tar:
            tar.add(fl_out_dir, arcname=f"sub-{sub_id}_{ses_label}_first_level_out")
        os.rename(tmp, archive_path)
    except Exception:
        if os.path.isfile(tmp):
            os.remove(tmp)
        raise

    size_mb = os.path.getsize(archive_path) / (1024 * 1024)
    logger.info(
        "Session %s compression complete: %s (%.1f MB)",
        ses_label, archive_path, size_mb
    )
    return archive_path


def cleanup_local_inputs(downloaded_paths, logger):
    """
    Delete locally downloaded input files after a successful S3 upload.

    Only the exact file paths in downloaded_paths are removed — no recursive
    directory deletion is performed. Per-file errors are logged as warnings
    and do not interrupt cleanup of remaining files.

    Parameters
    ----------
    downloaded_paths : list of str
        Paths returned by download_from_s3().
    logger : logging.Logger
    """
    n_removed = 0
    n_failed = 0

    for fpath in downloaded_paths:
        try:
            os.remove(fpath)
            logger.debug("Removed local input: %s", fpath)
            n_removed += 1
        except OSError as e:
            logger.warning("Could not remove %s: %s", fpath, str(e))
            n_failed += 1

    logger.info(
        "Input cleanup complete: %d file(s) removed, %d failed.",
        n_removed, n_failed
    )
