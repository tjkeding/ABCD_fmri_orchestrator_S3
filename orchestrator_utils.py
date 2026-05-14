#!/usr/bin/env python3

# ============================================================================
# ORCHESTRATOR UTILITIES FOR fMRI FIRST-LEVEL PROCESSING
# Helper functions for the per-participant orchestration pipeline.
# Called by orchestrate_first_level.py in pipeline order.
#
# Author: Taylor J. Keding, Ph.D.
# Last updated: 03/16/26
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
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
import numpy as np
import pandas as pd

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError, NoCredentialsError


class OrchestratorError(Exception):
    """Raised for unrecoverable orchestrator errors."""
    pass


class LegacyArchiveCorruptError(Exception):
    """
    Raised by migrate_session_from_archive when a legacy on-S3 tarball
    cannot be migrated cleanly (tarfile.TarError on extract, or extracted
    file set fails sanity checks). Caught by _process_session as a signal
    to fall through to FULL processing per the Q8 routing decision tree.
    """
    pass


# Valid task labels recognized by this orchestrator.
# Defined here (authoritative) and imported into orchestrate_first_level.py
# to avoid circular imports.
VALID_TASK_LABELS = {"rest", "nback"}


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


def enumerate_upload_targets(session_out_dir, sub_id, session):
    """
    Walk the canonical session output tree and return the upload target list.

    This is the single source of truth for the per-session expected key set
    used by both upload (gap computation, verification) and routing consumers.

    Parameters
    ----------
    session_out_dir : str
        Local path to the session output directory.
    sub_id : str
        Participant ID (e.g. "NDARABC123").
    session : str
        Session code (e.g. "00").

    Returns
    -------
    list of dict
        Each dict has keys {"local_path": str, "s3_key_suffix": str,
        "size_bytes": int}. s3_key_suffix is the path relative to
        session_out_dir with os.sep replaced by "/" for forward-slash S3 keys.
        The list is sorted by s3_key_suffix for deterministic ordering.
    """
    targets = []

    # first_level_out/: full recursive walk, include all files
    fl_out_dir = os.path.join(session_out_dir, "first_level_out")
    if os.path.isdir(fl_out_dir):
        for root, dirs, files in os.walk(fl_out_dir):
            for fname in files:
                local_path = os.path.join(root, fname)
                rel = os.path.relpath(local_path, session_out_dir)
                s3_key_suffix = rel.replace(os.sep, "/")
                targets.append({
                    "local_path": local_path,
                    "s3_key_suffix": s3_key_suffix,
                    "size_bytes": os.path.getsize(local_path),
                })

    # qc/: full recursive walk, include all files
    qc_dir = os.path.join(session_out_dir, "qc")
    if os.path.isdir(qc_dir):
        for root, dirs, files in os.walk(qc_dir):
            for fname in files:
                local_path = os.path.join(root, fname)
                rel = os.path.relpath(local_path, session_out_dir)
                s3_key_suffix = rel.replace(os.sep, "/")
                targets.append({
                    "local_path": local_path,
                    "s3_key_suffix": s3_key_suffix,
                    "size_bytes": os.path.getsize(local_path),
                })

    # preproc/: single-level, files only, exclude *.nii.gz
    preproc_dir = os.path.join(session_out_dir, "preproc")
    if os.path.isdir(preproc_dir):
        for fname in os.listdir(preproc_dir):
            if fname.endswith(".nii.gz"):
                continue
            local_path = os.path.join(preproc_dir, fname)
            if not os.path.isfile(local_path):
                continue
            rel = os.path.relpath(local_path, session_out_dir)
            s3_key_suffix = rel.replace(os.sep, "/")
            targets.append({
                "local_path": local_path,
                "s3_key_suffix": s3_key_suffix,
                "size_bytes": os.path.getsize(local_path),
            })

    # concat/: single-level, files only, exclude *.nii.gz
    concat_dir = os.path.join(session_out_dir, "concat")
    if os.path.isdir(concat_dir):
        for fname in os.listdir(concat_dir):
            if fname.endswith(".nii.gz"):
                continue
            local_path = os.path.join(concat_dir, fname)
            if not os.path.isfile(local_path):
                continue
            rel = os.path.relpath(local_path, session_out_dir)
            s3_key_suffix = rel.replace(os.sep, "/")
            targets.append({
                "local_path": local_path,
                "s3_key_suffix": s3_key_suffix,
                "size_bytes": os.path.getsize(local_path),
            })

    targets.sort(key=lambda t: t["s3_key_suffix"])
    return targets


def _session_upload_prefix(s3_config, sub_id, session):
    """
    Return the per-session S3 prefix root (no trailing slash).

    Returns f"{s3_config['upload_prefix']}/sub-{sub_id}/ses-{session}A".
    Used by all S3 helpers to construct consistent per-session S3 key prefixes.
    """
    return f"{s3_config['upload_prefix']}/sub-{sub_id}/ses-{session}A"


def check_session_complete(s3_config, sub_id, session, logger):
    """
    Check whether the per-session _COMPLETE sentinel exists on S3.

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    logger : logging.Logger

    Returns
    -------
    bool
        True if the sentinel object is present; False if absent (404/NoSuchKey).

    Raises
    ------
    ClientError
        For any S3 error other than 404/NoSuchKey.
    """
    s3_client = _get_s3_client()
    bucket = s3_config["bucket"]
    sentinel_key = f"{_session_upload_prefix(s3_config, sub_id, session)}/_COMPLETE"
    try:
        s3_client.head_object(Bucket=bucket, Key=sentinel_key)
        logger.debug("Sentinel found: s3://%s/%s", bucket, sentinel_key)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            logger.debug("Sentinel absent: s3://%s/%s", bucket, sentinel_key)
            return False
        raise


def delete_session_sentinel(s3_config, sub_id, session, logger):
    """
    Delete the per-session _COMPLETE sentinel from S3.

    Idempotent: boto3 delete_object returns success for non-existent keys.

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    logger : logging.Logger

    Raises
    ------
    ClientError
        Re-raised to the caller on any S3 error.
    """
    s3_client = _get_s3_client()
    bucket = s3_config["bucket"]
    sentinel_key = f"{_session_upload_prefix(s3_config, sub_id, session)}/_COMPLETE"
    s3_client.delete_object(Bucket=bucket, Key=sentinel_key)
    logger.info("Deleted sentinel: s3://%s/%s", bucket, sentinel_key)


def determine_session_routing(s3_config, sub_id, session, force_recompute, logger):
    """
    Determine the routing path for a session: "skip", "migrate", or "full".

    Performs up to three head_object calls (sentinel, force_recompute delete,
    legacy tarball) and returns a routing decision with associated remote metadata.
    Does NOT download anything; integrity is verified downstream by
    migrate_session_from_archive.

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    force_recompute : bool
        If True, existing sentinel is deleted and routing returns "full".
    logger : logging.Logger

    Returns
    -------
    dict
        Keys:
          "routing": one of "skip", "migrate", "full".
          "remote_metadata": dict with routing-specific artifact metadata.
            - "skip": {"sentinel_last_modified": "<ISO8601>"}
            - "migrate": {"source_tarball_etag": str, "source_tarball_size": int,
                          "source_tarball_last_modified": str}
            - "full": {}

    Raises
    ------
    OrchestratorError
        On unexpected S3 errors during routing probes.
    """
    s3_client = _get_s3_client()
    bucket = s3_config["bucket"]

    # Step 1: Check for existing sentinel
    sentinel_present = check_session_complete(s3_config, sub_id, session, logger)
    if sentinel_present:
        if not force_recompute:
            # Read sentinel head_object for LastModified
            sentinel_key = f"{_session_upload_prefix(s3_config, sub_id, session)}/_COMPLETE"
            resp = s3_client.head_object(Bucket=bucket, Key=sentinel_key)
            last_modified_iso = resp["LastModified"].isoformat()
            logger.info(
                "Routing: SKIP for sub-%s ses-%s (sentinel last_modified=%s)",
                sub_id, session, last_modified_iso
            )
            return {
                "routing": "skip",
                "remote_metadata": {"sentinel_last_modified": last_modified_iso},
            }
        else:
            # force_recompute=True: delete sentinel and route FULL immediately
            delete_session_sentinel(s3_config, sub_id, session, logger)
            logger.info(
                "force_recompute=True: deleted existing sentinel for sub-%s ses-%s; routing FULL",
                sub_id, session
            )
            logger.info(
                "Routing: FULL for sub-%s ses-%s (no prior outputs detected)",
                sub_id, session
            )
            return {"routing": "full", "remote_metadata": {}}

    # Step 2: Check for legacy tarball
    tarball_key = f"{_session_upload_prefix(s3_config, sub_id, session)}/first_level_out.tar.gz"
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=tarball_key)
        etag = resp["ETag"]
        size = resp["ContentLength"]
        last_modified_iso = resp["LastModified"].isoformat()
        logger.info(
            "Routing: MIGRATE for sub-%s ses-%s (source tarball ETag=%s size=%d)",
            sub_id, session, etag, size
        )
        return {
            "routing": "migrate",
            "remote_metadata": {
                "source_tarball_etag": etag,
                "source_tarball_size": size,
                "source_tarball_last_modified": last_modified_iso,
            },
        }
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            pass
        else:
            raise OrchestratorError(
                f"Routing probe failed for {tarball_key}: {e}"
            )

    # Step 3: Neither sentinel nor tarball — route FULL
    logger.info(
        "Routing: FULL for sub-%s ses-%s (no prior outputs detected)",
        sub_id, session
    )
    return {"routing": "full", "remote_metadata": {}}


def upload_session_to_s3(s3_config, sub_id, session, session_out_dir, logger):
    """
    Upload a session's outputs to S3 using per-file parallel transfer.

    Enumerates expected targets, diffs against the remote inventory, uploads
    only the gap, verifies all expected keys post-upload, and writes a zero-byte
    _COMPLETE sentinel only on a full verification pass.

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    session_out_dir : str
        Local path to the session output directory.
    logger : logging.Logger

    Returns
    -------
    dict
        Keys: {"n_files_uploaded": int, "n_files_total": int,
               "verified_keys": int, "sentinel_key": str}.

    Raises
    ------
    OrchestratorError
        On empty enumeration, stale sentinel at upload start, upload failure,
        or post-upload verification failure.
    """
    prefix = _session_upload_prefix(s3_config, sub_id, session)
    bucket = s3_config["bucket"]
    max_workers = s3_config.get("upload_max_workers", 8)

    # 1. Enumerate expected targets
    targets = enumerate_upload_targets(session_out_dir, sub_id, session)
    if len(targets) == 0:
        raise OrchestratorError(
            f"No upload targets enumerated for sub-{sub_id} ses-{session}A; "
            f"cannot proceed."
        )

    # 2. Build expected-keys map
    expected = {f"{prefix}/{t['s3_key_suffix']}": t for t in targets}

    logger.info(
        "Upload phase: sub-%s ses-%s, %d expected targets, max_workers=%d",
        sub_id, session, len(expected), max_workers
    )

    # 3. Probe remote inventory
    s3_client = _get_s3_client()
    remote = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            remote[obj["Key"]] = obj["Size"]

    sentinel_key = f"{prefix}/_COMPLETE"
    if sentinel_key in remote:
        raise OrchestratorError(
            f"Stale sentinel present at upload start: {sentinel_key}. "
            f"Caller did not clear it; aborting."
        )

    # 4. Compute upload gap
    upload_list = []
    already_present = 0
    for expected_key, target in expected.items():
        if expected_key in remote and remote[expected_key] == target["size_bytes"]:
            already_present += 1
        else:
            upload_list.append((expected_key, target))

    logger.info(
        "Upload gap for sub-%s ses-%s: %d/%d files to upload (%d already present)",
        sub_id, session, len(upload_list), len(expected), already_present
    )

    # 5. Parallel upload via ThreadPoolExecutor
    n_uploaded = 0
    failures = []
    if upload_list:
        future_to_target = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for expected_key, target in upload_list:
                fut = ex.submit(
                    s3_client.upload_file,
                    target["local_path"],
                    bucket,
                    expected_key,
                )
                future_to_target[fut] = (expected_key, target)
            for i, fut in enumerate(as_completed(future_to_target), start=1):
                exp_key, target = future_to_target[fut]
                try:
                    fut.result()
                    n_uploaded += 1
                    if i % 10 == 0:
                        logger.info(
                            "%d/%d uploaded (sub-%s ses-%s)",
                            i, len(upload_list), sub_id, session
                        )
                    else:
                        logger.debug("Uploaded: %s", exp_key)
                except Exception as e:
                    failures.append((target, e))

        if failures:
            raise OrchestratorError(
                f"S3 upload failed for {len(failures)} file(s) in "
                f"sub-{sub_id} ses-{session}A; first failure: "
                f"{failures[0][0]['s3_key_suffix']}: {failures[0][1]}"
            )

    # 6. Batched verification
    final_remote = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            if obj["Key"] != sentinel_key:
                final_remote[obj["Key"]] = obj["Size"]

    missing = []
    mismatched = []
    for expected_key, target in expected.items():
        if expected_key not in final_remote:
            missing.append(expected_key)
        elif final_remote[expected_key] != target["size_bytes"]:
            mismatched.append(
                (expected_key, target["size_bytes"], final_remote[expected_key])
            )

    if missing or mismatched:
        raise OrchestratorError(
            f"Post-upload verification failed for sub-{sub_id} ses-{session}A: "
            f"{len(missing)} missing, {len(mismatched)} size mismatch; "
            f"first missing: {missing[0] if missing else 'n/a'}; "
            f"first mismatch: {mismatched[0] if mismatched else 'n/a'}"
        )

    logger.info(
        "Verification pass: %d keys present with matching sizes",
        len(expected)
    )

    # 7. Write sentinel (only on full verification pass)
    s3_client.put_object(Bucket=bucket, Key=sentinel_key, Body=b"")
    logger.info("Sentinel written: s3://%s/%s", bucket, sentinel_key)

    # 8. Return
    return {
        "n_files_uploaded": len(upload_list),
        "n_files_total": len(expected),
        "verified_keys": len(expected),
        "sentinel_key": sentinel_key,
    }


def migrate_session_from_archive(
    s3_config, sub_id, session, session_out_dir, logger, source_tarball_etag
):
    """
    Migrate a session from a legacy on-S3 tarball to the per-file layout.

    Downloads the legacy first_level_out.tar.gz, extracts to a staging dir with
    path-traversal guard, validates required arcname structure, stages files under
    canonical subdirs, inserts a migration provenance key into the session QC JSON,
    delegates to upload_session_to_s3, and deletes the legacy tarball from S3 on
    success.

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    session_out_dir : str
        Local path to the session output directory.
    logger : logging.Logger
    source_tarball_etag : str
        ETag of the legacy tarball from the routing result's remote_metadata;
        avoids a duplicate head_object call inside this function.

    Returns
    -------
    dict
        Keys: {"n_files_migrated": int, "sentinel_key": str}.

    Raises
    ------
    OrchestratorError
        On download failure or upload failure.
    LegacyArchiveCorruptError
        On extraction failure or arcname sanity-check failure; caught by
        _process_session to fall through to FULL processing.
    """
    ses_label = f"ses-{session}A"
    archive_prefix = f"sub-{sub_id}_{ses_label}"
    bucket = s3_config["bucket"]
    tarball_key = f"{_session_upload_prefix(s3_config, sub_id, session)}/first_level_out.tar.gz"

    logger.info(
        "Migration start: sub-%s ses-%s, source ETag=%s",
        sub_id, session, source_tarball_etag
    )

    # 1. Prepare directories
    os.makedirs(session_out_dir, exist_ok=True)
    for subdir in ("first_level_out", "qc", "preproc", "concat"):
        os.makedirs(os.path.join(session_out_dir, subdir), exist_ok=True)
    staging_dir = os.path.join(session_out_dir, "_migration_staging")
    os.makedirs(staging_dir, exist_ok=True)
    local_archive_path = os.path.join(session_out_dir, "legacy_archive.tar.gz")

    # 2. Download legacy tarball
    s3_client = _get_s3_client()
    try:
        s3_client.download_file(bucket, tarball_key, local_archive_path)
    except ClientError as e:
        raise OrchestratorError(
            f"Failed to download legacy tarball {tarball_key}: {e}"
        )
    archive_size_mb = os.path.getsize(local_archive_path) / (1024 * 1024)
    logger.info(
        "Migration download complete: %s (%.1f MB)", tarball_key, archive_size_mb
    )

    # 3. Extract to staging with path-traversal guard
    try:
        with tarfile.open(local_archive_path, "r:gz") as tar:
            target_real = os.path.realpath(staging_dir) + os.sep
            all_members = tar.getmembers()
            safe_members = []
            for member in all_members:
                member_path = os.path.realpath(
                    os.path.join(staging_dir, member.name)
                )
                if (
                    member_path.startswith(target_real)
                    or member_path == target_real.rstrip(os.sep)
                ):
                    safe_members.append(member)
            if len(safe_members) < len(all_members):
                n_skipped = len(all_members) - len(safe_members)
                logger.warning(
                    "Skipped %d unsafe tar member(s) with path traversal in %s",
                    n_skipped, local_archive_path
                )
            if not safe_members:
                raise LegacyArchiveCorruptError(
                    f"Legacy tarball {tarball_key} contained no safe members."
                )
            tar.extractall(path=staging_dir, members=safe_members)
    except LegacyArchiveCorruptError:
        raise
    except tarfile.TarError as e:
        raise LegacyArchiveCorruptError(
            f"Failed to extract legacy tarball {tarball_key}: {e}"
        )

    # 4. Validate arcname structure
    required_arcnames = [
        f"{archive_prefix}_first_level_out",
        f"{archive_prefix}_qc",
    ]
    for required in required_arcnames:
        if not os.path.isdir(os.path.join(staging_dir, required)):
            raise LegacyArchiveCorruptError(
                f"Legacy tarball {tarball_key} missing required arcname(s); "
                f"found at staging root: {os.listdir(staging_dir)}"
            )

    # 5. Stage files under canonical subdirs
    arcname_to_subdir = [
        (f"{archive_prefix}_first_level_out", "first_level_out"),
        (f"{archive_prefix}_qc", "qc"),
        (f"{archive_prefix}_preproc", "preproc"),
        (f"{archive_prefix}_concat", "concat"),
    ]
    for src_arcname, dst_subdir in arcname_to_subdir:
        src_path = os.path.join(staging_dir, src_arcname)
        dst_path = os.path.join(session_out_dir, dst_subdir)
        if os.path.isdir(src_path):
            shutil.copytree(src_path, dst_path, dirs_exist_ok=True)

    # 6. Insert migration provenance key into staged session QC JSON (AI7)
    qc_json_path = os.path.join(
        session_out_dir, "qc",
        f"sub-{sub_id}_{ses_label}_orchestrator_qc.json"
    )
    if os.path.isfile(qc_json_path):
        try:
            from orchestrate_first_level import __version__ as _orch_ver
        except ImportError:
            _orch_ver = "unknown"
        with open(qc_json_path, "r") as fh:
            qc_obj = json.load(fh)
        qc_obj["migration"] = {
            "status": "migrated_from_archive",
            "source_tarball_etag": source_tarball_etag,
            "migration_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "orchestrator_version_at_migration": _orch_ver,
        }
        tmp_path = qc_json_path + ".tmp"
        with open(tmp_path, "w") as fh:
            json.dump(qc_obj, fh, indent=2)
        os.rename(tmp_path, qc_json_path)
    else:
        logger.warning(
            "Legacy tarball lacked session QC JSON at %s; "
            "migration provenance key not inserted.",
            qc_json_path
        )

    # 7. Upload via per-file routine
    try:
        upload_result = upload_session_to_s3(
            s3_config, sub_id, session, session_out_dir, logger
        )
    except Exception:
        # Do NOT delete the legacy tarball on S3; leave it for re-attempt.
        raise

    # 8. Delete legacy tarball from S3 (only on successful sentinel write)
    try:
        s3_client.delete_object(Bucket=bucket, Key=tarball_key)
        logger.info(
            "Legacy tarball deleted from S3: %s/%s", bucket, tarball_key
        )
    except ClientError as e:
        logger.error(
            "Failed to delete legacy tarball from S3 (non-fatal): %s/%s: %s",
            bucket, tarball_key, str(e)
        )

    # 9. Cleanup local intermediates
    shutil.rmtree(staging_dir, ignore_errors=True)
    try:
        os.remove(local_archive_path)
    except OSError as e:
        logger.warning(
            "Could not remove local legacy archive %s: %s",
            local_archive_path, str(e)
        )

    # 10. Return
    n_migrated = upload_result["n_files_uploaded"]
    sentinel_key = upload_result["sentinel_key"]
    logger.info(
        "Migration complete: sub-%s ses-%s, %d files migrated, sentinel written",
        sub_id, session, n_migrated
    )
    return {"n_files_migrated": n_migrated, "sentinel_key": sentinel_key}


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
    3. Motion files (ALL tasks, including rest): probes for runs 1-9 per task

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
        - motion_files: {task_label: [path1, path2, ...]} ordered by run
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
        is_rest = (task_label == "rest")
        if is_rest:
            continue

        task_events = []
        # Probe all run indices 1-9 unconditionally (runs may be non-contiguous)
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
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("404", "NoSuchKey"):
                    # Continue probing — run indices may be non-contiguous
                    continue
                else:
                    raise

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
                continue

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

    # --- 3. Download motion files (ALL tasks, including rest) ---
    motion_files = {}
    for task_def in task_defs:
        task_label = task_def["task_label"]

        task_motions = []
        for run_num in range(1, 10):
            motion_s3_key = (
                f"{mmps_prefix}/sub-{sub_id}/{ses_label}/func/"
                f"sub-{sub_id}_{ses_label}_task-{task_label}_run-0{run_num}_motion.tsv"
            )
            motion_local_dir = os.path.join(local_ses_dir, "motion")
            os.makedirs(motion_local_dir, exist_ok=True)
            motion_local = os.path.join(
                motion_local_dir,
                f"sub-{sub_id}_{ses_label}_task-{task_label}_run-0{run_num}_motion.tsv"
            )

            if os.path.isfile(motion_local):
                task_motions.append(motion_local)
                all_downloaded.append(motion_local)
                continue

            try:
                s3_client.head_object(Bucket=bucket, Key=motion_s3_key)
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("404", "NoSuchKey"):
                    # Continue probing — run indices may be non-contiguous
                    continue
                else:
                    raise

            try:
                s3_client.download_file(bucket, motion_s3_key, motion_local)
                task_motions.append(motion_local)
                all_downloaded.append(motion_local)
                logger.debug(
                    "Downloaded motion: s3://%s/%s", bucket, motion_s3_key
                )
            except ClientError as e:
                logger.warning(
                    "Failed to download motion file for sub-%s %s task-%s run-%d: %s",
                    sub_id, ses_label, task_label, run_num, str(e)
                )
                continue

        if task_motions:
            motion_files[task_label] = task_motions
            logger.info(
                "Downloaded %d motion file(s) for task '%s' %s",
                len(task_motions), task_label, ses_label
            )
        else:
            logger.warning(
                "No motion files found on S3 for task '%s' sub-%s %s",
                task_label, sub_id, ses_label
            )

    logger.info(
        "S3 download summary for sub-%s %s: archive + %d events file(s) + %d motion file(s)",
        sub_id, ses_label,
        sum(len(v) for v in events_files.values()),
        sum(len(v) for v in motion_files.values())
    )

    return {
        "archive_path": archive_local,
        "events_files": events_files,
        "motion_files": motion_files,
        "all_downloaded_paths": all_downloaded,
    }


def discover_local_mmps_files(local_base_dir, sub_id, session, task_defs, logger):
    """
    Discover previously-downloaded motion and events files on the local filesystem.

    Used in local mode (S3 disabled) to find files that were downloaded during
    a prior S3-enabled run. Mirrors the directory structure created by
    download_session_data(): motion files in {base}/motion/, events in {base}/events/.

    Parameters
    ----------
    local_base_dir : str
        Root fMRIPrep directory (study.fmriprep_dir). Session data is expected
        at {local_base_dir}/sub-{sub_id}/ses-{session}A/.
    sub_id : str
        Participant ID (e.g. "NDARABC123").
    session : str
        Session code (e.g. "00").
    task_defs : list of dict
        Task definitions from the orchestrator config.
    logger : logging.Logger

    Returns
    -------
    dict
        {"events_files": {task_label: [paths]}, "motion_files": {task_label: [paths]}}

    Raises
    ------
    FileNotFoundError
        If no motion files are found for any task. This indicates a
        misconfiguration (the user specified a local path but no downloaded
        data exists there).
    """
    ses_label = f"ses-{session}A"
    ses_dir = os.path.join(local_base_dir, f"sub-{sub_id}", ses_label)

    motion_dir = os.path.join(ses_dir, "motion")
    events_dir = os.path.join(ses_dir, "events")

    motion_files = {}
    events_files = {}

    for task_def in task_defs:
        task_label = task_def["task_label"]
        is_rest = (task_label == "rest")

        # --- Motion files (all tasks including rest) ---
        if os.path.isdir(motion_dir):
            pattern = os.path.join(
                motion_dir,
                f"sub-{sub_id}_{ses_label}_task-{task_label}_run-*_motion.tsv"
            )
            matched = sorted(globmod.glob(pattern))
            if matched:
                motion_files[task_label] = matched
                logger.debug(
                    "Local motion: task '%s' — %d file(s)", task_label, len(matched)
                )

        # --- Events files (non-rest tasks only) ---
        if not is_rest and os.path.isdir(events_dir):
            pattern = os.path.join(
                events_dir,
                f"sub-{sub_id}_{ses_label}_task-{task_label}_run-*_events.tsv"
            )
            matched = sorted(globmod.glob(pattern))
            if matched:
                events_files[task_label] = matched
                logger.debug(
                    "Local events: task '%s' — %d file(s)", task_label, len(matched)
                )

    total_motion = sum(len(v) for v in motion_files.values())
    total_events = sum(len(v) for v in events_files.values())
    logger.info(
        "Local file discovery for sub-%s %s: %d motion file(s), %d events file(s)",
        sub_id, ses_label, total_motion, total_events
    )

    if total_motion == 0:
        raise FileNotFoundError(
            f"No motion files found in local mode for sub-{sub_id} {ses_label}. "
            f"Expected files at: {motion_dir}/ "
            f"(pattern: sub-{sub_id}_{ses_label}_task-*_run-*_motion.tsv). "
            f"Verify that a prior S3 download populated this directory."
        )

    return {"events_files": events_files, "motion_files": motion_files}


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


# ============================================================================
# Section B: AFNI Check
# ============================================================================

def verify_afni_installation(logger):
    """
    Verify that AFNI is installed and reachable on PATH.

    Runs ``afni -ver`` as a subprocess. Logs the version string on success.
    Called at startup when not in dry-run mode.

    Parameters
    ----------
    logger : logging.Logger

    Raises
    ------
    OrchestratorError
        If AFNI is not found on PATH or if ``afni -ver`` exits non-zero.
    """
    try:
        result = subprocess.run(
            ["afni", "-ver"],
            capture_output=True, text=True, check=True,
        )
        logger.info("AFNI version: %s", result.stdout.strip())
    except FileNotFoundError:
        raise OrchestratorError(
            "AFNI ('afni') not found on PATH. Please install AFNI and "
            "ensure it is available in your environment."
        )
    except subprocess.CalledProcessError as e:
        raise OrchestratorError(
            f"AFNI check failed: {e.stderr.strip() if e.stderr else str(e)}"
        )

# ============================================================================
# Section C: File Discovery
# ============================================================================

def discover_session_files(extracted_dir, sub_id, session, task_defs, events_files, motion_files, space, logger):
    """
    Discover fMRIPrep outputs from an extracted session archive.

    Globs the extracted func/ directory for all runs per task, matching
    each discovered run with its corresponding events file (from mmps_mproc)
    and raw motion file (from mmps_mproc).

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
    motion_files : dict
        {task_label: [motion_path_run1, motion_path_run2, ...]} from
        download_session_data(). Missing tasks have no entry.
    space : str
        Template space string (e.g. "MNI152NLin2009cAsym").
    logger : logging.Logger

    Returns
    -------
    dict
        {task_label: [run_dict, ...], "_anat_mask": anat_mask_path or None}
        Each run_dict has keys: bold_path, confounds_path, mask_path,
        motion_tsv_path, events_path (None for rest), session, task_label,
        run, run_label.
    """
    ses_label = f"ses-{session}A"
    func_dir = os.path.join(extracted_dir, "func")
    result = {}

    for task_def in task_defs:
        task_label = task_def["task_label"]
        is_rest = (task_label == "rest")

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

        # Build motion file lookup dict keyed by run number
        task_motion_by_run = {}
        task_motions = motion_files.get(task_label, [])
        for mot_path in task_motions:
            mot_match = re.search(r"_run-0?(\d+)_motion\.tsv$", os.path.basename(mot_path))
            if mot_match:
                task_motion_by_run[int(mot_match.group(1))] = mot_path

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

            # Match motion file by run number
            motion_tsv_path = task_motion_by_run.get(run_num)
            if motion_tsv_path is None:
                logger.warning(
                    "No motion file for sub-%s %s task-%s run-%d — skipping run",
                    sub_id, ses_label, task_label, run_num
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
                "motion_tsv_path": motion_tsv_path,
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

def apply_brain_mask(bold_path, mask_path, out_dir, out_prefix, logger, force_recompute=False):
    """
    Apply a brain mask to a BOLD file using AFNI's 3dcalc.

    Uses the expression ``a*step(b)`` to zero out non-brain voxels.
    Output filename: ``{out_prefix}_masked.nii.gz``. Idempotent — skips
    computation if the output file already exists and ``force_recompute``
    is False.

    Parameters
    ----------
    bold_path : str
        Path to the input BOLD NIfTI file.
    mask_path : str
        Path to the binary brain mask NIfTI file.
    out_dir : str
        Directory to write the masked BOLD file into.
    out_prefix : str
        Filename prefix for the output file.
    logger : logging.Logger
    force_recompute : bool, optional
        If True, delete and recompute output even if it already exists.
        Default False.

    Returns
    -------
    str
        Path to the masked BOLD file.

    Raises
    ------
    OrchestratorError
        If 3dcalc fails or produces no output.
    """
    out_path = os.path.join(out_dir, f"{out_prefix}_masked.nii.gz")

    if os.path.isfile(out_path) and not force_recompute:
        logger.info("Masked BOLD already exists: %s", out_path)
        return out_path

    if force_recompute and os.path.isfile(out_path):
        os.remove(out_path)
        logger.debug("force_recompute: removed existing %s", out_path)

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

    Counts columns matching the prefix ``non_steady_state_outlier_``. Each
    such column in the confounds TSV corresponds to one NSS TR that should
    be removed from the beginning of the scan before first-level analysis.

    Parameters
    ----------
    confounds_path : str
        Path to the fMRIPrep confounds TSV file.
    logger : logging.Logger

    Returns
    -------
    int
        Number of non-steady-state TRs to remove (0 if none detected).
    """
    confounds_df = pd.read_csv(confounds_path, sep="\t")
    nss_cols = [c for c in confounds_df.columns if c.startswith("non_steady_state_outlier")]
    n_remove = len(nss_cols)

    if n_remove > 0:
        logger.info("Detected %d non-steady-state TR(s) in %s", n_remove, confounds_path)
    else:
        logger.info("No non-steady-state TRs detected in %s", confounds_path)

    return n_remove

def remove_initial_trs_bold(bold_path, n_remove, out_dir, out_prefix, logger, force_recompute=False):
    """
    Remove initial TRs from a BOLD file using AFNI's 3dTcat.

    When ``n_remove == 0``, returns the input path unchanged (no 3dTcat call)
    but still queries TR count via ``3dinfo -nv``. Output filename:
    ``{out_prefix}_trimmed.nii.gz``. Idempotent — skips if output exists and
    ``force_recompute`` is False.

    Parameters
    ----------
    bold_path : str
        Path to the masked BOLD NIfTI file.
    n_remove : int
        Number of initial TRs to remove (from detect_non_steady_state_trs).
    out_dir : str
        Directory to write the trimmed BOLD file into.
    out_prefix : str
        Filename prefix for the output file.
    logger : logging.Logger
    force_recompute : bool, optional
        If True, delete and recompute output even if it already exists.
        Default False.

    Returns
    -------
    tuple of (str, int)
        (path to trimmed BOLD, number of TRs remaining after trimming).
        The TR count is -1 if ``3dinfo`` fails.

    Raises
    ------
    OrchestratorError
        If 3dTcat fails or produces no output.
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

    if os.path.isfile(out_path) and not force_recompute:
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

    if force_recompute and os.path.isfile(out_path):
        os.remove(out_path)
        logger.debug("force_recompute: removed existing %s", out_path)

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
    Remove initial rows from a tabular (whitespace-delimited text) file.

    Used to trim confounds files or other tabular files to match a BOLD
    timeseries after non-steady-state TR removal. When ``n_remove == 0``,
    the file is copied to ``out_path`` unchanged.

    Parameters
    ----------
    file_path : str
        Path to the input tabular file (loaded via ``np.loadtxt``).
    n_remove : int
        Number of initial rows to remove.
    out_path : str
        Destination path for the trimmed output file.
    logger : logging.Logger

    Returns
    -------
    tuple of (str, int)
        (output path, number of rows remaining after trimming).
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
# Section G: Motion and Confounds Extraction
# ============================================================================

def extract_motion_regressors(motion_tsv_path, n_remove, calc_n_motion_derivs, out_path, logger, force_recompute=False):
    """
    Extract motion regressors from a raw motion TSV file.

    Reads the 6 base motion parameters (trans_x/y/z, rot_x/y/z) from the
    raw motion.tsv file produced by mmps_mproc. Output units: translations
    in mm, rotations in degrees (per fmri-first-level-proc >= 2.4.0 input
    contract). No unit conversion is applied.

    Temporal derivatives are always computed numerically via finite differences
    (padded with 0.0 at the first row to preserve length). Total output
    columns = 6 * (1 + calc_n_motion_derivs).

    Parameters
    ----------
    motion_tsv_path : str
        Path to raw motion TSV file (columns: t_indx, rot_z/x/y, trans_z/x/y).
    n_remove : int
        Number of non-steady-state TRs to trim from the start.
    calc_n_motion_derivs : int
        Number of temporal derivative sets to include (>= 0).
    out_path : str
        Destination path for the output motion file.
    logger : logging.Logger

    Returns
    -------
    tuple[str, bool]
        (out_path, rotation_unit_ambiguous):
        - out_path: path to the motion regressors file.
        - rotation_unit_ambiguous: True when max(abs(rotation)) <= 1.0 and units
          cannot be definitively determined (genuinely low-motion subject or data
          already in radians). False when max(abs(rotation)) > 1.0, confirming
          degrees. Always False when returning from cache (file already exists).
    """
    if os.path.isfile(out_path) and not force_recompute:
        logger.info("Motion regressors already exist: %s", out_path)
        return out_path, False

    motion_df = pd.read_csv(motion_tsv_path, sep="\t")

    if len(motion_df) == 0:
        raise OrchestratorError(f"Motion file is empty (0 rows): {motion_tsv_path}")

    base_cols = ["trans_x", "trans_y", "trans_z", "rot_x", "rot_y", "rot_z"]

    # Verify base columns exist (select by name; column order is NOT guaranteed)
    missing_base = [c for c in base_cols if c not in motion_df.columns]
    if missing_base:
        raise OrchestratorError(
            f"Missing base motion columns in {motion_tsv_path}: {missing_base}. "
            f"Available columns: {list(motion_df.columns)}"
        )

    # Verify base columns contain valid data (not entirely NaN)
    base_data = motion_df[base_cols]
    if base_data.isna().all().any():
        all_nan_cols = [c for c in base_cols if base_data[c].isna().all()]
        raise OrchestratorError(
            f"Motion columns are entirely NaN in {motion_tsv_path}: {all_nan_cols}. "
            f"Motion data may be corrupted."
        )

    # Rotation unit validation: exploit physical constraints of MRI head coils.
    # 1.0 radian = 57.3 degrees — physically impossible inside a head coil.
    # 1.0 degree is trivially common in any real fMRI scan.
    rot_cols = motion_df[["rot_x", "rot_y", "rot_z"]].values
    max_rot = float(np.nanmax(np.abs(rot_cols)))
    rotation_unit_ambiguous = False
    if max_rot > 1.0:
        logger.info(
            "Rotation unit check: PASSED (max abs rotation = %.4f > 1.0, definitively in degrees).",
            max_rot
        )
    else:
        # Cannot distinguish genuinely low-motion subjects from data already in
        # radians. Real-world testing (ABCD sub-7L18GGXH, 17 runs rejected;
        # 14 additional runs across 3 participants) confirmed that some ABCD
        # subjects exhibit max rotations of 0.08–0.66 degrees throughout an
        # entire session. Raising a fatal error was overly conservative.
        # Pass through without conversion and flag for QC review.
        logger.warning(
            "Rotation unit check AMBIGUOUS for %s: "
            "max(abs(rotation)) = %.6f <= 1.0 across all TRs and axes. "
            "Units cannot be definitively determined (genuinely low-motion "
            "subject or data already in radians). Proceeding WITHOUT "
            "conversion (fmri-first-level-proc expects degrees). Run "
            "flagged as rotation_unit_ambiguous=True for QC review.",
            motion_tsv_path, max_rot
        )
        rotation_unit_ambiguous = True

    # Extract base parameters (rotations remain in degrees per fmri-first-level-proc >= 2.4.0 contract)
    motion_array = motion_df[base_cols].values.copy()

    # Build the motion array column by column
    arrays = [motion_array]  # shape (n_trs, 6)
    prev_degree_data = arrays[0]

    for degree in range(1, calc_n_motion_derivs + 1):
        # Always compute numerically: forward difference of the previous degree
        diff = np.diff(prev_degree_data, axis=0)
        deriv_data = np.vstack([np.zeros((1, prev_degree_data.shape[1])), diff])
        logger.info(
            "Computing motion derivative (degree %d) numerically via finite differences.",
            degree
        )
        arrays.append(deriv_data)
        prev_degree_data = deriv_data

    motion_data = np.hstack(arrays)  # shape (n_trs, 6 * (1 + calc_n_motion_derivs))

    # Remove initial non-steady-state TRs
    if n_remove > 0:
        motion_data = motion_data[n_remove:, :]

    # NaN handling: "unknown = censor" policy — impute 999.0 to guarantee censoring.
    # NaN in motion parameters indicates a tracking failure — the true motion
    # is unknown. Imputing 999.0 ensures these TRs exceed any reasonable FD
    # threshold and are censored by upstream 1d_tool.py.
    nan_mask = np.isnan(motion_data)
    if nan_mask.any():
        nan_rows, nan_cols = np.where(nan_mask)
        col_labels = [f"col_{i}" for i in range(motion_data.shape[1])]
        affected = [
            f"TR {r} {col_labels[c]}"
            for r, c in zip(nan_rows.tolist(), nan_cols.tolist())
        ]
        logger.warning(
            "NaN motion values detected in %s: %d occurrence(s) across "
            "%d unique TR(s). Imputing 999.0 (guarantees censoring). "
            "Affected: %s",
            motion_tsv_path,
            int(nan_mask.sum()),
            int(np.unique(nan_rows).size),
            "; ".join(affected[:20]) + (" ..." if len(affected) > 20 else "")
        )
    motion_data = np.where(nan_mask, 999.0, motion_data)

    np.savetxt(out_path, motion_data, fmt="%.10g", delimiter="\t")
    logger.info(
        "Motion regressors saved: %d columns, %d rows → %s",
        motion_data.shape[1], motion_data.shape[0], out_path
    )
    return out_path, rotation_unit_ambiguous

def extract_tissue_signals(confounds_path, n_remove, tissue_type, out_path, logger, force_recompute=False):
    """
    Extract a tissue/nuisance signal time series from fMRIPrep confounds.

    Trims the initial ``n_remove`` rows (non-steady-state TRs) before writing
    the output. Used for rest_conn analyses only; common tissue types are
    ``"csf"``, ``"white_matter"``, and ``"global_signal"``. Idempotent —
    skips if output exists and ``force_recompute`` is False.

    Parameters
    ----------
    confounds_path : str
        Path to the fMRIPrep confounds TSV file.
    n_remove : int
        Number of non-steady-state TRs to trim from the start.
    tissue_type : str
        Column name in the confounds TSV. Accepted values:
        ``"csf"``, ``"white_matter"``, ``"global_signal"``.
    out_path : str
        Destination path for the output signal file (plain text, one value per line).
    logger : logging.Logger
    force_recompute : bool, optional
        If True, delete and recompute output even if it already exists.
        Default False.

    Returns
    -------
    str
        Path to the tissue signal file.

    Raises
    ------
    OrchestratorError
        If the requested column is not found in the confounds TSV.
    """
    if os.path.isfile(out_path) and not force_recompute:
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

def fix_nback_cue_labels(events_path, condition_column, out_path, logger, force_recompute=False):
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
    if os.path.isfile(out_path) and not force_recompute:
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


def format_task_timing(events_path, condition_column, conditions_exclude, n_remove, TR, out_path, logger, force_recompute=False):
    """
    Convert BIDS events.tsv to first-level timing CSV (CONDITION, ONSET, DURATION).

    Adjusts event onsets for removed non-steady-state TRs
    (``adjusted_onset = original_onset - n_remove * TR``) and drops events
    with ``adjusted_onset < 0``. Optionally filters conditions via
    ``conditions_exclude``. Idempotent — skips if output exists and
    ``force_recompute`` is False.

    Parameters
    ----------
    events_path : str
        Path to the BIDS events TSV file (possibly relabeled by fix_nback_cue_labels).
    condition_column : str
        Column containing condition labels (typically ``"trial_type"``).
    conditions_exclude : list of str or None
        Conditions to drop before writing output. ``None`` = keep all.
    n_remove : int
        Number of non-steady-state TRs removed from the BOLD timeseries.
    TR : float
        Repetition time in seconds, used to compute the time offset.
    out_path : str
        Destination path for the output timing CSV.
    logger : logging.Logger
    force_recompute : bool, optional
        If True, delete and recompute output even if it already exists.
        Default False.

    Returns
    -------
    tuple of (str, int)
        (path to timing CSV, number of events dropped due to negative onset
        after adjustment). Note: the drop count is 0 when returning from
        cache (cached result does not re-compute the count).

    Raises
    ------
    OrchestratorError
        If required columns are missing or no events remain after filtering.
    """
    if os.path.isfile(out_path) and not force_recompute:
        logger.info("Task timing already exists: %s", out_path)
        # Note: cached return does not preserve the original drop count;
        # n_dropped is currently only logged, not used for downstream logic.
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

def concatenate_bolds(bold_paths, out_path, logger, force_recompute=False):
    """
    Concatenate multiple BOLD runs using AFNI's 3dTcat.
    Single-run: copies file instead of running 3dTcat.

    Returns
    -------
    str
        Path to the concatenated BOLD file.
    """
    if os.path.isfile(out_path) and not force_recompute:
        logger.info("Concatenated BOLD already exists: %s", out_path)
        return out_path

    if force_recompute and os.path.isfile(out_path):
        os.remove(out_path)
        logger.debug("force_recompute: removed existing %s", out_path)

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

def concatenate_tabular_files(file_paths, out_path, logger, force_recompute=False):
    """
    Concatenate tabular files (motion regressors, tissue signals) by row stacking.

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
    if os.path.isfile(out_path) and not force_recompute:
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

def concatenate_task_timing(timing_paths, run_tr_counts, TR, out_path, logger, force_recompute=False):
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
    if len(timing_paths) != len(run_tr_counts):
        raise OrchestratorError(
            f"Mismatch between timing files ({len(timing_paths)}) and "
            f"TR counts ({len(run_tr_counts)}). These must correspond 1:1."
        )

    if any(c <= 0 for c in run_tr_counts):
        bad = [(i, c) for i, c in enumerate(run_tr_counts) if c <= 0]
        raise OrchestratorError(
            f"Invalid TR count(s) in run_tr_counts: {bad}. "
            f"All values must be positive integers."
        )

    if os.path.isfile(out_path) and not force_recompute:
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

def apply_smoothing(bold_path, mask_path, method, fwhm, out_dir, out_prefix, logger, force_recompute=False):
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

    if os.path.isfile(out_path) and not force_recompute:
        logger.info("Smoothed BOLD already exists: %s", out_path)
        return out_path

    if force_recompute and os.path.isfile(out_path):
        os.remove(out_path)
        logger.debug("force_recompute: removed existing %s", out_path)

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
# Section J2: Mask Intersection
# ============================================================================

def compute_mask_intersection(mask_paths, out_path, logger,
                              force_recompute=False):
    """
    Compute the intersection of multiple brain masks using 3dmask_tool -inter.

    When only one mask is provided, returns that mask path directly (no-op).
    For multiple masks, produces a conservative intersection where a voxel is
    included only if it is non-zero in ALL input masks.

    Parameters
    ----------
    mask_paths : list of str
        Paths to binary NIfTI mask files.
    out_path : str
        Output path for the intersection mask.
    logger : logging.Logger
        Logger instance.
    force_recompute : bool, optional
        If True, recompute even if out_path exists. Default False.

    Returns
    -------
    str
        Path to the intersection mask (out_path, or the single input mask).
    """
    if len(mask_paths) == 1:
        return mask_paths[0]

    if os.path.isfile(out_path) and not force_recompute:
        logger.info("Mask intersection already exists: %s", out_path)
        return out_path

    if force_recompute and os.path.isfile(out_path):
        os.remove(out_path)
        logger.debug("force_recompute: removed existing %s", out_path)

    # Log reference mask grid dimensions to confirm inputs are grid-matched.
    try:
        dim_result = subprocess.run(
            ["3dinfo", "-n4", mask_paths[0]],
            capture_output=True, text=True, check=True
        )
        logger.debug(
            "Mask intersection reference grid (3dinfo -n4): %s — %s",
            os.path.basename(mask_paths[0]), dim_result.stdout.strip()
        )
    except Exception:
        logger.debug("Could not retrieve grid dimensions for mask: %s", mask_paths[0])

    cmd = ["3dmask_tool", "-inter", "-prefix", out_path,
           "-input"] + list(mask_paths)
    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise OrchestratorError(
            f"3dmask_tool mask intersection failed: "
            f"{e.stderr.strip() if e.stderr else str(e)}"
        )
    if not os.path.isfile(out_path):
        raise OrchestratorError(
            f"3dmask_tool produced no output: {out_path}"
        )
    logger.info(
        "Mask intersection (%d masks) → %s", len(mask_paths), out_path
    )
    return out_path


# ============================================================================
# Section K: QC — Preprocessing
# ============================================================================

def compute_tsnr(bold_path, mask_path, out_dir, out_prefix, logger):
    """
    Compute temporal SNR (mean/stdev across time) within the brain mask.

    Uses AFNI ``3dTstat`` to compute the mean and stdev volumes, then
    computes tSNR voxelwise (``mean / max(stdev, 0.001)`` within mask)
    via ``3dcalc``. Returns the median tSNR across brain voxels queried
    with ``3dBrickStat -percentile 50``. Intermediate files (mean, stdev)
    are deleted after use.

    Parameters
    ----------
    bold_path : str
        Path to the preprocessed BOLD NIfTI (after masking and NSS trimming).
    mask_path : str
        Path to the brain mask NIfTI.
    out_dir : str
        Directory to write intermediate tSNR volumes.
    out_prefix : str
        Filename prefix for intermediate output files.
    logger : logging.Logger

    Returns
    -------
    float or None
        Median brain tSNR, or None if computation fails.
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

def generate_carpet_plot(bold_path, mask_path, confounds_path, n_remove, out_path, logger):

    """
    Generate a carpet plot: DVARS trace on top, voxel x time heatmap below.

    FD is no longer computed or displayed by the orchestrator (pre-analysis
    preprocessing QC). Motion metrics are deferred to per-analysis QC
    (consolidated from upstream enorm.1D/censor.1D produced by fmri_first_level_proc).

    Parameters
    ----------
    bold_path : str
        Path to preprocessed BOLD NIfTI.
    mask_path : str
        Path to brain mask NIfTI.
    confounds_path : str
        Path to fMRIPrep confounds TSV (used for DVARS).
    n_remove : int
        Number of non-steady-state TRs removed from start.
    out_path : str
        Output path for carpet plot image.
    logger : logging.Logger
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

    # DVARS from confounds (derived from BOLD signal, unaffected by motion source)
    dvars = confounds_df.get("dvars", pd.Series(dtype=float)).values

    if n_remove > 0:
        dvars = dvars[n_remove:]

    n_vols = len(dvars)
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
    gs = GridSpec(2, 1, height_ratios=[1, 4], hspace=0.3)

    # DVARS trace
    ax_dvars = fig.add_subplot(gs[0])
    ax_dvars.plot(time_axis, dvars, color="black", linewidth=0.5)
    ax_dvars.set_ylabel("DVARS")
    ax_dvars.set_xlim(0, n_vols - 1)
    ax_dvars.set_xticklabels([])

    # Carpet plot
    ax_carpet = fig.add_subplot(gs[1])
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

    Resamples the anatomical mask to the functional mask grid via
    ``3dresample -rmode NN`` before computing voxel overlap. The Dice
    coefficient is defined as ``2 * |A ∩ B| / (|A| + |B|)``. Temporary
    resampled and overlap files are cleaned up after use.

    Parameters
    ----------
    func_mask_path : str
        Path to the functional brain mask NIfTI.
    anat_mask_path : str
        Path to the anatomical brain mask NIfTI (may differ in resolution).
    logger : logging.Logger

    Returns
    -------
    float or None
        Dice coefficient in [0, 1], or None if computation fails.
    """
    try:
        # Count voxels in func mask
        r1 = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", func_mask_path],
            capture_output=True, text=True, check=True,
        )
        n_func = float(r1.stdout.strip())

        # Resample anat mask to func mask grid (they may differ in resolution)
        resampled_anat_path = os.path.join(
            os.path.dirname(func_mask_path), "_temp_anat_resampled.nii.gz"
        )
        subprocess.run(
            ["3dresample", "-master", func_mask_path, "-rmode", "NN",
             "-input", anat_mask_path, "-prefix", resampled_anat_path],
            capture_output=True, text=True, check=True,
        )

        # Count voxels in resampled anat mask (recount after resampling)
        r2 = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", resampled_anat_path],
            capture_output=True, text=True, check=True,
        )
        n_anat = float(r2.stdout.strip())

        # Count intersection
        overlap_path = os.path.join(os.path.dirname(func_mask_path), "_temp_overlap.nii.gz")
        subprocess.run(
            ["3dcalc", "-a", func_mask_path, "-b", resampled_anat_path,
             "-expr", "step(a)*step(b)", "-prefix", overlap_path],
            capture_output=True, text=True, check=True,
        )
        r3 = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", overlap_path],
            capture_output=True, text=True, check=True,
        )
        n_overlap = float(r3.stdout.strip())

        # Clean up temp files
        for tmp in (overlap_path, resampled_anat_path):
            if os.path.isfile(tmp):
                os.remove(tmp)

        dice = 2 * n_overlap / (n_func + n_anat) if (n_func + n_anat) > 0 else 0.0
        logger.info("Registration quality (Dice): %.4f", dice)
        return dice

    except (subprocess.CalledProcessError, ValueError) as e:
        logger.warning("Could not compute registration quality: %s", str(e))
        return None

def compute_preproc_qc(run_info, confounds_path, bold_path, mask_path, n_remove, qc_config, out_dir, sub_id, space, logger, anat_mask_path=None):
    """
    Compute pre-analysis preprocessing QC metrics for a single run.

    Pre-analysis QC includes non-motion metrics only: tSNR, brain mask coverage,
    registration Dice, DVARS (from fMRIPrep confounds), and carpet plots.
    Motion metrics (FD, censor stats) are deferred to per-analysis QC
    (consolidated session QC) where they are sourced exclusively from upstream
    enorm.1D and censor.1D files produced by fmri_first_level_proc.

    Parameters
    ----------
    run_info : dict
        Run metadata (session, task_label, run, run_label).
    confounds_path : str
        Path to fMRIPrep confounds TSV (used for DVARS).
    bold_path : str
        Path to preprocessed BOLD NIfTI (after masking).
    mask_path : str
        Path to brain mask NIfTI.
    n_remove : int
        Number of non-steady-state TRs detected.
    qc_config : dict
        QC configuration (tsnr, carpet_plots, registration_quality flags).
    out_dir : str
        Output directory for QC files.
    sub_id : str
        Subject ID prefix (e.g., "sub-NDARABC123").
    space : str
        Template space string (e.g., "MNI152NLin2009cAsym").
    logger : logging.Logger
    anat_mask_path : str or None
        Path to the anatomical brain mask discovered by discover_session_files().
        When provided, used directly for registration quality computation
        instead of re-deriving the path internally.

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

    # -- DVARS metrics (from fMRIPrep confounds) --
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
        generate_carpet_plot(bold_path, mask_path, confounds_path,
                             n_remove, carpet_path, logger)
        qc["carpet_plot_path"] = carpet_path
    else:
        qc["carpet_plot_path"] = None

    # -- Registration quality --
    if qc_config.get("registration_quality", False):
        anat_mask = anat_mask_path  # Use pre-discovered path when available

        if anat_mask is None:
            # Fallback: derive path from mask_path directory structure
            fmriprep_sub_dir = os.path.dirname(os.path.dirname(mask_path))
            ses_part = run_info.get("session")
            if ses_part:
                ses_dir_name = f"ses-{ses_part}A"
                anat_dir = os.path.join(fmriprep_sub_dir, ses_dir_name, "anat")
            else:
                anat_dir = os.path.join(fmriprep_sub_dir, "anat")

            if os.path.isdir(anat_dir):
                space_tag = f"space-{space}"
                candidates = [
                    f for f in os.listdir(anat_dir)
                    if "desc-brain_mask" in f
                    and space_tag in f
                    and f.endswith(".nii.gz")
                ]
                if candidates:
                    anat_mask = os.path.join(anat_dir, sorted(candidates)[0])
                    if len(candidates) > 1:
                        logger.warning(
                            "Multiple anat brain masks found for space '%s' in %s; "
                            "using: %s", space, anat_dir, candidates[0]
                        )

            if anat_mask is None:
                anat_dir_str = anat_dir
                logger.info(
                    "No anatomical brain mask found for space '%s' in %s — "
                    "skipping registration quality check.", space, anat_dir_str
                )

        if anat_mask is not None:
            dice = compute_registration_quality(mask_path, anat_mask, logger)
            qc["registration"] = {"dice": dice, "anat_mask": anat_mask}
        else:
            qc["registration"] = {"dice": None, "anat_mask": None}
    else:
        qc["registration"] = {"dice": None}

    return qc

def save_qc_json(qc_metrics, out_path, logger):
    """
    Save a QC metrics dict to a JSON file.

    Creates parent directories as needed. Values not JSON-serializable by
    default (e.g., numpy types) are converted to strings via ``default=str``.

    Parameters
    ----------
    qc_metrics : dict
        QC metrics dictionary to serialize.
    out_path : str
        Destination path for the JSON file.
    logger : logging.Logger
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(qc_metrics, f, indent=2, default=str)
    logger.info("QC metrics saved: %s", out_path)

def compute_first_level_qc(analysis_name, analysis_type, out_dir, sub_id, out_file_pre, logger, error_msg=None):
    """
    Compute first-level analysis QC metrics.

    Checks for expected output files by analysis type to determine whether
    the analysis completed successfully. Reads the upstream QC summary JSON
    produced by fmri_first_level_proc for censor statistics and other metrics.

    Expected outputs by type:
      task_act  — at least one .nii.gz (stat bucket) in the analysis directory
      task_conn — at least one .nii.gz (beta series) in the analysis directory
      rest_conn — at least one .nii.gz (residual/connectivity) in the analysis directory

    Parameters
    ----------
    analysis_name : str
        Name of the analysis block.
    analysis_type : str
        Type of the analysis (task_act, task_conn, rest_conn).
    out_dir : str
        Base output directory containing analysis subdirectories.
    sub_id : str
        Subject ID string (e.g. "sub-TEST001").
    out_file_pre : str or None
        Output file prefix used by fmri_first_level_proc. When not None,
        the upstream QC summary is expected at
        {out_dir}/{analysis_name}/{out_file_pre}_qc_summary.json.
    logger : logging.Logger
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

    # Read upstream QC summary JSON produced by fmri_first_level_proc
    pct_censored = None
    upstream_qc = None
    if out_file_pre is not None:
        qc_json_path = os.path.join(analysis_dir, f"{out_file_pre}_qc_summary.json")
        if os.path.isfile(qc_json_path):
            try:
                with open(qc_json_path) as f:
                    upstream_qc = json.load(f)
                pct_censored = upstream_qc.get("pct_censored")
                if pct_censored is not None:
                    pct_censored = round(float(pct_censored), 2)
                logger.info(
                    "First-level QC: read upstream QC summary for '%s' from %s",
                    analysis_name, qc_json_path
                )
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                logger.warning(
                    "First-level QC: could not parse upstream QC JSON for '%s' at %s: %s",
                    analysis_name, qc_json_path, str(e)
                )
        else:
            logger.warning(
                "First-level QC: upstream QC summary not found for '%s' at %s",
                analysis_name, qc_json_path
            )

    qc = {
        "sub_id": sub_id,
        "analysis_name": analysis_name,
        "type": analysis_type,
        "pct_censored": pct_censored,
        "completed_successfully": completed_successfully,
        "error": error_msg,
        "n_nifti_outputs": len(nifti_outputs),
        "output_files": output_files,
        "upstream_qc": upstream_qc,
    }

    return qc

# ============================================================================
# Section K3: Consolidated Session QC
# ============================================================================

def consolidate_session_qc(sub_id, ses_label, session_status, session_wall_time,
                           preproc_qc_by_run, analysis_outcomes, out_path, logger):
    """
    Build and write a consolidated session-level QC JSON.

    Combines pre-analysis preprocessing QC and per-analysis QC into a
    single file per session, replacing the previous pattern of separate
    per-run and per-analysis JSON files.

    Parameters
    ----------
    sub_id : str
        Participant ID (e.g., "NDARABC123").
    ses_label : str
        Session label (e.g., "ses-00A").
    session_status : str
        Qualified session status: "success", "partial", or "failed".
    session_wall_time : float
        Total session wall time in seconds.
    preproc_qc_by_run : dict
        Mapping of run_label -> QC dict from compute_preproc_qc().
    analysis_outcomes : list of dict
        Per-analysis outcome dicts with keys: name, type, status, error,
        wall_time_seconds, and optionally fl_qc.
    out_path : str
        Output path for the consolidated QC JSON.
    logger : logging.Logger

    Returns
    -------
    str
        Path to the written QC JSON file.
    """
    # Resolve orchestrator version from the entry-point module constant.
    # Lazy import avoids circular dependency; fallback to "unknown" for
    # environments that import orchestrator_utils in isolation (e.g. tests).
    try:
        from orchestrate_first_level import __version__ as _orch_ver
    except ImportError:
        _orch_ver = "unknown"

    # Provenance block
    afni_ver = None
    try:
        result = subprocess.run(
            ["afni", "--version"], capture_output=True, text=True, check=False
        )
        if result.returncode == 0:
            afni_ver = result.stdout.strip()
    except FileNotFoundError:
        pass

    proc_ver = None
    try:
        from fmri_first_level_proc import __version__ as _proc_ver
        proc_ver = _proc_ver
    except ImportError:
        pass

    qc = {
        "provenance": {
            "orchestrator_version": _orch_ver,
            "fmri_first_level_proc_version": proc_ver,
            "afni_version": afni_ver,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "sub_id": sub_id,
            "session": ses_label,
        },
        "preprocessing": preproc_qc_by_run,
        "analyses": {},
        "session": {
            "status": session_status,
            "wall_time_seconds": round(session_wall_time, 2),
            "n_analyses_attempted": len(analysis_outcomes),
            "n_analyses_succeeded": sum(
                1 for o in analysis_outcomes if o["status"] == "success"
            ),
        },
    }

    # Populate per-analysis entries
    for outcome in analysis_outcomes:
        qc["analyses"][outcome["name"]] = {
            "type": outcome["type"],
            "status": outcome["status"],
            "error": outcome["error"],
            "wall_time_seconds": outcome["wall_time_seconds"],
            "upstream_qc": outcome.get("fl_qc"),
        }

    save_qc_json(qc, out_path, logger)
    return out_path


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
            {"bold": path, "motion": path, "timing": path}
        For rest analyses (per-run):
            {"bolds": [paths], "motions": [paths],
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

    # Inject or validate global.tr against study.TR
    config["global"] = config.get("global", {})
    if "tr" in config["global"]:
        if abs(config["global"]["tr"] - study_config["TR"]) > 1e-6:
            raise OrchestratorError(
                f"global.tr in proc template ({config['global']['tr']}) does not match "
                f"study.TR ({study_config['TR']}). These must be consistent."
            )
    else:
        config["global"]["tr"] = study_config["TR"]

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

        # Inject per-analysis censoring parameters (handled by upstream)
        block["fd_threshold"] = fd_threshold
        block["censor_prev_tr"] = orch_analysis.get("censor_prev_tr", False)

        # Override paths based on analysis type
        if analysis_type in ("task_act", "task_conn"):
            block["paths"] = {
                "scan_path": pf["bold"],
                "task_timing_path": pf["timing"],
                "motion_path": pf["motion"],
            }

        elif analysis_type == "rest_conn":
            block["paths"] = {
                "scan_paths": pf["bolds"],
                "motion_paths": pf["motions"],
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

    Output filename: ``sub-{sub_id}_ses-{session}A_first_level_config.yaml``.
    Creates the output directory if it does not exist.

    Parameters
    ----------
    config_dict : dict
        Config dictionary produced by build_first_level_config().
    out_dir : str
        Directory to write the YAML file into.
    sub_id : str
        Participant ID (e.g. "NDARABC123").
    session : str
        Session code (e.g. "00").
    logger : logging.Logger

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

    Performs comprehensive validation of all required fields, types, and
    cross-references (e.g., analysis task_labels must reference defined tasks).
    Sets default values for optional fields (``force_recompute``,
    ``calc_n_motion_derivs``, ``smoothing``, ``qc``, ``s3``). See
    Section 9 of INPUT_SPECIFICATION.md for the complete validation rule table.

    Parameters
    ----------
    config_path : str
        Path to the orchestrator YAML configuration file.
    logger : logging.Logger

    Returns
    -------
    dict
        Validated and normalized config dictionary.

    Raises
    ------
    OrchestratorError
        On any validation failure (file not found, parse error, missing
        required fields, type errors, constraint violations).
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
        if task["task_label"] not in VALID_TASK_LABELS:
            raise OrchestratorError(
                f"tasks[{i}] task_label '{task['task_label']}' is not a recognized "
                f"task label for this orchestrator. Valid labels: "
                f"{sorted(VALID_TASK_LABELS)}. Check the 'tasks' section "
                f"of the orchestrator config."
            )

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

        # Validate per-analysis censor_prev_tr (optional, must be bool)
        if "censor_prev_tr" in analysis:
            if not isinstance(analysis["censor_prev_tr"], bool):
                raise OrchestratorError(
                    f"[{name}] censor_prev_tr must be a boolean, got: {analysis['censor_prev_tr']!r}"
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

    # Validate force_recompute
    study.setdefault("force_recompute", False)
    if not isinstance(study.get("force_recompute"), bool):
        raise OrchestratorError(
            "study.force_recompute must be a boolean, got: "
            f"{study['force_recompute']!r}"
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
    config["s3"].setdefault("upload_max_workers", 8)
    umw = config["s3"]["upload_max_workers"]
    if not isinstance(umw, int) or isinstance(umw, bool) or not (1 <= umw <= 64):
        raise OrchestratorError(
            f"s3.upload_max_workers must be an integer in [1, 64], got: {umw!r}"
        )

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

    # Validate global block exists as a dict
    global_block = proc_template.get("global")
    if not isinstance(global_block, dict):
        raise OrchestratorError(
            "Proc template must have a 'global' block (dict). "
            "Required fields: num_cores, tr (injected if omitted), template_path, force_diff_atlas."
        )

    # Index template analyses by name
    template_by_name = {}
    for block in template_analyses:
        bname = block.get("name")
        if bname is None:
            raise OrchestratorError(
                "Proc template has an analysis block without a 'name' field."
            )

        # Reject templates that still contain censor_path or censor_paths
        paths_block = block.get("paths", {})
        if paths_block and isinstance(paths_block, dict):
            if "censor_path" in paths_block:
                raise OrchestratorError(
                    f"Proc template analysis '{bname}' contains 'censor_path' in its paths block. "
                    f"Censor files are now auto-generated by fmri_first_level_proc from "
                    f"fd_threshold; remove censor_path from the template."
                )
            if "censor_paths" in paths_block:
                raise OrchestratorError(
                    f"Proc template analysis '{bname}' contains 'censor_paths' in its paths block. "
                    f"Censor files are now auto-generated by fmri_first_level_proc from "
                    f"fd_threshold; remove censor_paths from the template."
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
# Section N: Local Cleanup
# ============================================================================

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
