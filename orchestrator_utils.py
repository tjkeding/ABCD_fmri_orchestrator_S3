#!/usr/bin/env python3

# ============================================================================
# Orchestrator utilities for fMRI first-level processing (orchestrate_first_level.py helpers)
# Author: Taylor J. Keding, Ph.D.
# ============================================================================

import os
import re
import sys
import math
import copy
import json
import logging
import resource
import subprocess
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
import numpy as np
import pandas as pd
from packaging.version import Version, InvalidVersion

import boto3
from botocore.config import Config as BotocoreConfig
from botocore.exceptions import ClientError, NoCredentialsError

from fmri_first_level_proc import names as upstream_names

_log = logging.getLogger(__name__)


class OrchestratorError(Exception):
    """Unrecoverable orchestrator error; optional ``context`` dict carries a run-exclusion "reason"
    and metadata for QC recording."""

    def __init__(self, message="", context=None):
        super().__init__(message)
        self.context = context


# Task-label whitelist; imported by orchestrate_first_level.py.
VALID_TASK_LABELS = {"rest", "nback"}


# ============================================================================
# Exit code constants
# ============================================================================

EXIT_COMPLETE = 0
"""Every analysis reached a terminal state; the _COMPLETE sentinel is written."""

EXIT_FAILED_UNEXPLAINED = 1
"""Non-terminal outcome, unexplained run exclusion, or session-level exception; no sentinel, so the
session is reprocessed on the next run."""

EXIT_FAILED_KNOWN_CAUSE = 2
"""Terminal ModelError (e.g. insufficient DOF) and nothing unexplained; sentinel written because
rerunning cannot change the outcome."""

_UNEXPLAINED_EXCLUSION_REASONS = frozenset({"unexpected_error", "producer_fd_inconsistent"})
"""Run-exclusion reasons that make a session EXIT_FAILED_UNEXPLAINED."""


# ============================================================================
# S3 routing, sentinel and upload
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


def enumerate_upload_targets(session_out_dir, sub_id, session, first_level_file_paths):
    """
    Build the session's upload targets (the expected S3 key set): the explicit first-level files,
    all of qc/, non-NIfTI files directly in preproc/, and the rendered config YAML.

    Parameters
    ----------
    session_out_dir : str
        Local path to the session output directory.
    sub_id : str
        Participant ID (e.g. "SUBJ01").
    session : str
        Session code (e.g. "00").
    first_level_file_paths : list of str
        First-level output files; first_level_out/ is never walked, so stale files cannot be uploaded.
        Missing files or files outside ``session_out_dir`` are skipped with a warning.

    Returns
    -------
    list of dict
        {"local_path", "s3_key_suffix" (forward-slash path relative to session_out_dir),
        "size_bytes"}, sorted by s3_key_suffix.
    """
    targets = {}
    session_real = os.path.realpath(session_out_dir)

    def _add(local_path):
        if not os.path.isfile(local_path):
            _log.warning("Upload target does not exist, skipping: %s", local_path)
            return
        lp_real = os.path.realpath(local_path)
        if not lp_real.startswith(session_real + os.sep) and lp_real != session_real:
            _log.warning(
                "Upload target not under session output directory, skipping: %s",
                local_path,
            )
            return
        rel = os.path.relpath(local_path, session_out_dir)
        suffix = rel.replace(os.sep, "/")
        if suffix not in targets:
            targets[suffix] = {
                "local_path": local_path,
                "s3_key_suffix": suffix,
                "size_bytes": os.path.getsize(local_path),
            }

    # Explicit first-level file paths
    for p in first_level_file_paths:
        _add(p)

    # qc/: full recursive walk, include all files
    qc_dir = os.path.join(session_out_dir, "qc")
    if os.path.isdir(qc_dir):
        for root, _dirs, files in os.walk(qc_dir):
            for fname in files:
                _add(os.path.join(root, fname))

    # preproc/: single-level, files only, exclude *.nii.gz
    preproc_dir = os.path.join(session_out_dir, "preproc")
    if os.path.isdir(preproc_dir):
        for fname in os.listdir(preproc_dir):
            if fname.endswith(".nii.gz"):
                continue
            local_path = os.path.join(preproc_dir, fname)
            if os.path.isfile(local_path):
                _add(local_path)

    # Rendered session config YAML at session root
    ses_label = f"ses-{session}A"
    config_name = f"sub-{sub_id}_{ses_label}_first_level_config.yaml"
    config_path = os.path.join(session_out_dir, config_name)
    if os.path.isfile(config_path):
        _add(config_path)

    result = sorted(targets.values(), key=lambda t: t["s3_key_suffix"])
    return result


def _session_upload_prefix(s3_config, sub_id, session):
    """Per-session S3 prefix, ``{upload_prefix}/sub-{sub_id}/ses-{session}A`` (no trailing slash)."""
    return f"{s3_config['upload_prefix']}/sub-{sub_id}/ses-{session}A"


def get_session_sentinel(s3_config, sub_id, session, logger):
    """
    Fetch the per-session _COMPLETE sentinel metadata from S3.

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
    dict or None
        The head_object response when the sentinel exists; None when it is
        absent (404/NoSuchKey).

    Raises
    ------
    ClientError
        For any S3 error other than 404/NoSuchKey.
    """
    s3_client = _get_s3_client()
    bucket = s3_config["bucket"]
    sentinel_key = f"{_session_upload_prefix(s3_config, sub_id, session)}/_COMPLETE"
    try:
        resp = s3_client.head_object(Bucket=bucket, Key=sentinel_key)
        logger.debug("Sentinel found: s3://%s/%s", bucket, sentinel_key)
        return resp
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchKey"):
            logger.debug("Sentinel absent: s3://%s/%s", bucket, sentinel_key)
            return None
        raise


def delete_session_sentinel(s3_config, sub_id, session, logger):
    """
    Delete the per-session _COMPLETE sentinel from S3 (idempotent for absent keys).

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
    Route a session to "skip" (sentinel present) or "full" from a single sentinel probe.

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID.
    session : str
        Session code (e.g. "00").
    force_recompute : bool
        If True, an existing sentinel is deleted and routing is "full".
    logger : logging.Logger

    Returns
    -------
    dict
        {"routing": "skip" | "full", "remote_metadata": {"sentinel_last_modified": ISO8601} for
        skip, {} for full}.

    Raises
    ------
    ClientError
        botocore ClientError for S3 errors other than 404/NoSuchKey.
    """
    sentinel_resp = get_session_sentinel(s3_config, sub_id, session, logger)
    if sentinel_resp is not None:
        if not force_recompute:
            last_modified_iso = sentinel_resp["LastModified"].isoformat()
            logger.info(
                "Routing: SKIP for sub-%s ses-%s (sentinel last_modified=%s)",
                sub_id, session, last_modified_iso
            )
            return {
                "routing": "skip",
                "remote_metadata": {"sentinel_last_modified": last_modified_iso},
            }
        else:
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


def upload_session_to_s3(s3_config, sub_id, session, session_out_dir, logger,
                         first_level_file_paths=None, write_sentinel=True):
    """
    Upload a session's outputs in parallel: send only files missing or size-mismatched remotely,
    verify every expected key, sweep stale keys, then optionally write the _COMPLETE sentinel.

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
    first_level_file_paths : list of str or None
        First-level output files, passed to enumerate_upload_targets (None means none).
    write_sentinel : bool
        Write the zero-byte _COMPLETE sentinel after verification; the sweep runs either way.

    Returns
    -------
    dict
        {"n_files_uploaded", "n_files_total", "verified_keys", "sentinel_key" (str or None),
        "sentinel_written", "swept_keys"}.

    Raises
    ------
    OrchestratorError
        On empty enumeration, stale sentinel at upload start, upload failure,
        or post-upload verification failure.
    """
    prefix = _session_upload_prefix(s3_config, sub_id, session)
    bucket = s3_config["bucket"]
    max_workers = s3_config.get("upload_max_workers", 8)

    # Enumerate expected targets
    targets = enumerate_upload_targets(
        session_out_dir, sub_id, session, first_level_file_paths or []
    )
    if len(targets) == 0:
        raise OrchestratorError(
            f"No upload targets enumerated for sub-{sub_id} ses-{session}A; "
            f"cannot proceed."
        )

    # Build expected-keys map
    expected = {f"{prefix}/{t['s3_key_suffix']}": t for t in targets}

    logger.info(
        "Starting upload: sub-%s ses-%s, %d expected targets, max_workers=%d",
        sub_id, session, len(expected), max_workers
    )

    # Probe remote inventory
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

    # Compute upload gap
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

    # Parallel upload via ThreadPoolExecutor
    n_uploaded = 0
    failures = []
    if upload_list:
        step = max(1, math.ceil(len(upload_list) / 10))
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
            for fut in as_completed(future_to_target):
                exp_key, target = future_to_target[fut]
                try:
                    fut.result()
                    n_uploaded += 1
                    if n_uploaded % step == 0 or n_uploaded == len(upload_list):
                        logger.info(
                            "%d/%d uploaded (sub-%s ses-%s)",
                            n_uploaded, len(upload_list), sub_id, session
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

    # Batched verification
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

    # Sweep stale remote keys (runs regardless of write_sentinel)
    stale = sorted(
        k for k in final_remote
        if k != sentinel_key and k not in expected
    )
    for stale_key in stale:
        try:
            s3_client.delete_object(Bucket=bucket, Key=stale_key)
            logger.debug("Swept stale key: %s", stale_key)
        except ClientError as e:
            raise OrchestratorError(
                f"Failed to sweep stale remote key {stale_key}: {e}"
            )
    if stale:
        logger.info("Swept %d stale remote key(s) from %s/", len(stale), prefix)

    # Conditionally write sentinel
    sentinel_written = False
    if write_sentinel:
        s3_client.put_object(Bucket=bucket, Key=sentinel_key, Body=b"")
        sentinel_written = True
        logger.info("Sentinel written: s3://%s/%s", bucket, sentinel_key)
    else:
        logger.info(
            "Sentinel not written (session exit category 1, failed unexplained)"
        )

    return {
        "n_files_uploaded": len(upload_list),
        "n_files_total": len(expected),
        "verified_keys": len(expected),
        "sentinel_key": sentinel_key if sentinel_written else None,
        "sentinel_written": sentinel_written,
        "swept_keys": stale,
    }


# ============================================================================
# Upstream version check
# ============================================================================

MIN_UPSTREAM_VERSION = "4.0.0"
MAX_UPSTREAM_VERSION = "5.0.0"


def verify_upstream_version(logger, min_version=MIN_UPSTREAM_VERSION,
                            max_version=MAX_UPSTREAM_VERSION):
    """Require min_version <= installed fmri_first_level_proc < max_version (PEP 440, so
    pre-releases such as 4.0.0rc1 do not satisfy >= 4.0.0).

    Raises
    ------
    OrchestratorError
        If the package is missing, unversioned, unparseable, or outside the window.
    """
    try:
        import fmri_first_level_proc
    except ImportError:
        raise OrchestratorError(
            f"fmri_first_level_proc is not installed. "
            f"Requires >= {min_version}, < {max_version}."
        )
    found = getattr(fmri_first_level_proc, "__version__", None)
    if found is None:
        raise OrchestratorError(
            f"fmri_first_level_proc version could not be determined. "
            f"Requires >= {min_version}, < {max_version}."
        )
    try:
        installed = Version(str(found))
        required_min = Version(str(min_version))
        required_max = Version(str(max_version))
    except InvalidVersion as e:
        raise OrchestratorError(
            f"Could not parse fmri_first_level_proc version "
            f"'{found}' (required >= {min_version}, < {max_version}): {e}"
        ) from e
    if installed < required_min:
        raise OrchestratorError(
            f"fmri_first_level_proc {found} is installed, but this orchestrator "
            f"requires >= {min_version}. Update the environment (see environment.yaml)."
        )
    if installed >= required_max:
        raise OrchestratorError(
            f"fmri_first_level_proc {found} is installed, but this orchestrator "
            f"requires < {max_version}. A newer orchestrator version may be needed."
        )
    logger.info(
        "fmri_first_level_proc version: %s (required >= %s, < %s)",
        found, min_version, max_version
    )
    return found


# ============================================================================
# Input discovery, completeness check and download
# ============================================================================

# Producer filename patterns; session directories end in "A" (ses-00A) and the captured code is numeric.
_SES = r"ses-(?P<ses>[0-9]+)A"
_RUN_ENTITIES = (
    r"^sub-(?P<sub>[A-Za-z0-9]+)_"
    + _SES
    + r"_task-(?P<task>[A-Za-z0-9]+)_run-(?P<run>[0-9]+)"
)

INPUT_FILE_PATTERNS = {
    "bold": re.compile(
        _RUN_ENTITIES
        + r"_space-(?P<space>[A-Za-z0-9]+)_bold\.nii\.gz$"
    ),
    "brainmask": re.compile(
        _RUN_ENTITIES
        + r"_space-(?P<space>[A-Za-z0-9]+)_brainmask\.nii\.gz$"
    ),
    "confounds": re.compile(
        _RUN_ENTITIES + r"_desc-confounds_timeseries\.tsv$"
    ),
    "events": re.compile(
        _RUN_ENTITIES + r"_events\.tsv$"
    ),
    "csf_mask": re.compile(
        r"^sub-(?P<sub>[A-Za-z0-9]+)_"
        + _SES
        + r"_space-(?P<space>[A-Za-z0-9]+)(?:_res-(?P<res>[A-Za-z0-9]+))?_label-CSF_mask\.nii\.gz$"
    ),
    "wm_mask": re.compile(
        r"^sub-(?P<sub>[A-Za-z0-9]+)_"
        + _SES
        + r"_space-(?P<space>[A-Za-z0-9]+)(?:_res-(?P<res>[A-Za-z0-9]+))?_label-WM_mask\.nii\.gz$"
    ),
}

# Producer files not consumed as inputs; logged at DEBUG instead of WARNING.
_KNOWN_NON_INPUT_PATTERNS = [
    re.compile(r"_res-[A-Za-z0-9]+_dseg\.nii\.gz$"),
    re.compile(r"_res-[A-Za-z0-9]+_dseg\.tsv$"),
    re.compile(r"_label-GM_mask\.nii\.gz$"),
    re.compile(r"_label-[A-Za-z]+-?[A-Za-z]*_probseg\.nii\.gz$"),
    re.compile(r"_desc-boldref\.nii\.gz$"),
    re.compile(r"^_complete\.json$"),
    re.compile(r"_bold\.tar\.gz$"),
]


def _parse_input_basename(basename):
    """Return (role, groupdict) for the first INPUT_FILE_PATTERNS match, else (None, None)."""
    for role, pattern in INPUT_FILE_PATTERNS.items():
        m = pattern.match(basename)
        if m:
            return role, m.groupdict()
    return None, None


def _read_completion_manifest(s3_config, ref):
    """Load a producer _complete.json from its S3 key."""
    try:
        resp = _get_s3_client().get_object(Bucket=s3_config["bucket"], Key=ref)
        manifest = json.loads(resp["Body"].read())
    except (ClientError, ValueError) as e:
        raise OrchestratorError(f"Could not read completion manifest {ref}: {e}")
    if not isinstance(manifest, dict):
        raise OrchestratorError(f"Completion manifest {ref} is not a JSON object")
    return manifest


def discover_subject_inputs(study_config, s3_config, sub_id, task_defs, logger,
                            session_filter=None):
    """
    Discover a subject's inputs from one S3 listing, keeping only sessions whose _complete.json
    (schema_version 1, kind func-session) exists and lists no absent run files or CSF/WM masks.

    Parameters
    ----------
    study_config : dict
        The 'study' section of the orchestrator config.
    s3_config : dict
        The 's3' section of the orchestrator config.
    sub_id : str
        Participant ID (e.g. "SUBJ01").
    task_defs : list of dict
        Task definitions from the orchestrator config.
    logger : logging.Logger
    session_filter : str or None
        Keep only this session; other sessions' manifests are not read.

    Returns
    -------
    dict
        {session: {task: {run: {"bold", "brainmask", "confounds", "events"}},
        "anat": {"csf_mask", "wm_mask"}, "_manifest": {"written_at", "schema_version"}}};
        values are S3 keys or None, and "_"-prefixed keys are metadata.

    Raises
    ------
    OrchestratorError
        On an invalid or inconsistent manifest, or duplicate files for one slot.
    """
    configured_tasks = {td["task_label"] for td in task_defs}
    study_space = study_config.get("space")

    candidates = []
    bucket = s3_config["bucket"]
    func_template = s3_config["func_prefix_template"]
    anat_template = s3_config["anat_prefix_template"]
    prefix_a = func_template.split("{session}")[0].replace("{subject}", sub_id)
    prefix_b = anat_template.split("{session}")[0].replace("{subject}", sub_id)
    listing_prefix = os.path.commonprefix([prefix_a, prefix_b])
    s3_client = _get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=listing_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            basename = key.rsplit("/", 1)[-1] if "/" in key else key
            candidates.append((basename, key))

    parsed = []  # [(role, groups, s3_key)]
    for basename, value in candidates:
        role, groups = _parse_input_basename(basename)
        if role is None:
            if any(p.search(basename) for p in _KNOWN_NON_INPUT_PATTERNS):
                logger.debug("Known non-input file, ignoring: %s", value)
            else:
                logger.warning("Non-matching input file, ignoring: %s", value)
            continue
        if groups.get("sub") and groups["sub"] != sub_id:
            logger.warning(
                "Subject token '%s' differs from --subj_id '%s', ignoring: %s",
                groups["sub"], sub_id, value
            )
            continue
        if study_space and groups.get("space") and groups["space"] != study_space:
            logger.warning(
                "Space '%s' differs from study.space '%s', ignoring: %s",
                groups["space"], study_space, value
            )
            continue
        if role in ("bold", "brainmask", "confounds", "events"):
            if groups["task"] not in configured_tasks:
                logger.warning(
                    "Task '%s' not in configured tasks %s, ignoring: %s",
                    groups["task"], sorted(configured_tasks), value
                )
                continue
        parsed.append((role, groups, value))

    # BOLDs define the valid (session, task, run) slots
    result = {}
    bold_slots = set()

    for role, groups, value in parsed:
        ses = groups["ses"]
        if ses not in result:
            result[ses] = {t: {} for t in configured_tasks}
            result[ses]["anat"] = {"csf_mask": None, "wm_mask": None}

        if role in ("bold", "brainmask", "confounds", "events"):
            task = groups["task"]
            run = int(groups["run"])
            if role == "bold":
                bold_slots.add((ses, task, run))
                if run in result[ses][task]:
                    existing = result[ses][task][run].get("bold")
                    if existing is not None:
                        raise OrchestratorError(
                            f"Duplicate BOLD for ses-{ses} task-{task} run-{run}: "
                            f"{existing} and {value}"
                        )
                else:
                    result[ses][task][run] = {
                        "bold": None, "brainmask": None,
                        "confounds": None, "events": None,
                    }
                result[ses][task][run]["bold"] = value

        elif role in ("csf_mask", "wm_mask"):
            anat_key = role
            if result[ses]["anat"][anat_key] is not None:
                raise OrchestratorError(
                    f"Duplicate {anat_key} for ses-{ses}: "
                    f"{result[ses]['anat'][anat_key]} and {value}"
                )
            result[ses]["anat"][anat_key] = value

    # Assign non-BOLD run-level files to their BOLD-defined slots
    for role, groups, value in parsed:
        if role in ("brainmask", "confounds", "events"):
            ses = groups["ses"]
            task = groups["task"]
            run = int(groups["run"])
            if (ses, task, run) not in bold_slots:
                logger.warning(
                    "No BOLD for ses-%s task-%s run-%d, ignoring %s: %s",
                    ses, task, run, role, value
                )
                continue
            existing = result[ses][task][run].get(role)
            if existing is not None:
                raise OrchestratorError(
                    f"Duplicate {role} for ses-{ses} task-{task} run-{run}: "
                    f"{existing} and {value}"
                )
            result[ses][task][run][role] = value

    logger.info("Discovery: sessions with input files: %s", sorted(result))
    if session_filter is not None:
        result = {s: v for s, v in result.items() if s == session_filter}

    # _complete.json is written last, so a session directory without it is still being written.
    ses_dir_re = re.compile(
        r"(?:^|/)sub-" + re.escape(sub_id) + r"/ses-(?P<ses>[0-9]+)A$"
    )
    present_by_ses = {}
    manifest_ref_by_ses = {}
    for basename, value in candidates:
        m = ses_dir_re.search(os.path.dirname(value))
        if m is None:
            continue
        ses = m.group("ses")
        present_by_ses.setdefault(ses, set()).add(basename)
        if basename == "_complete.json":
            manifest_ref_by_ses[ses] = value

    run_key_re = re.compile(r"task-(?P<task>[A-Za-z0-9]+)_run-[0-9]+")
    for ses in sorted(result):
        manifest_ref = manifest_ref_by_ses.get(ses)
        if manifest_ref is None:
            logger.info(
                "Discovery: ses-%sA has no producer completion manifest "
                "(_complete.json); skipping session.", ses
            )
            del result[ses]
            continue

        manifest = _read_completion_manifest(s3_config, manifest_ref)
        if manifest.get("schema_version") != 1:
            raise OrchestratorError(
                f"ses-{ses}A: _complete.json schema_version is "
                f"{manifest.get('schema_version')!r}, expected 1 ({manifest_ref})"
            )
        if manifest.get("kind") != "func-session":
            raise OrchestratorError(
                f"ses-{ses}A: _complete.json kind is {manifest.get('kind')!r}, "
                f"expected 'func-session' ({manifest_ref})"
            )

        # Manifest-listed configured-task run files and CSF/WM masks must all be in the listing.
        present = present_by_ses.get(ses, set())
        missing = []
        for run_key, run_files in (manifest.get("runs") or {}).items():
            m = run_key_re.fullmatch(run_key)
            if m and m.group("task") in configured_tasks:
                missing.extend(f for f in run_files if f not in present)
        for fname in manifest.get("session_files") or []:
            if ("_label-CSF_mask" in fname or "_label-WM_mask" in fname) \
                    and fname not in present:
                missing.append(fname)
        if missing:
            raise OrchestratorError(
                f"ses-{ses}A: _complete.json lists file(s) absent from the "
                f"listing: {sorted(missing)}"
            )

        result[ses]["_manifest"] = {
            "written_at": manifest.get("written_at"),
            "schema_version": manifest.get("schema_version"),
        }

    for ses in sorted(result):
        for task in sorted(configured_tasks):
            n_runs = len(result[ses].get(task, {}))
            if n_runs > 0:
                logger.info(
                    "Discovery: ses-%s task-%s: %d run(s)", ses, task, n_runs
                )
        anat = result[ses]["anat"]
        logger.info(
            "Discovery: ses-%s anat: csf_mask=%s, wm_mask=%s",
            ses,
            "present" if anat["csf_mask"] else "absent",
            "present" if anat["wm_mask"] else "absent",
        )

    return result


def download_session_inputs(s3_config, input_dir, session_inputs, logger):
    """
    Download one session's inputs to os.path.join(input_dir, key), reusing files already on disk.

    Parameters
    ----------
    s3_config : dict
        The 's3' section of the orchestrator config.
    input_dir : str
        Local root directory mirroring the S3 layout.
    session_inputs : dict
        One session's dict from discover_subject_inputs (_manifest passes through unchanged).
    logger : logging.Logger

    Returns
    -------
    tuple of (dict, list of str)
        (session_inputs with S3 keys replaced by local paths, all local paths for cleanup).
    """
    s3_client = _get_s3_client()
    bucket = s3_config["bucket"]
    local = copy.deepcopy(session_inputs)
    downloaded = []

    def _download(s3_key):
        if s3_key is None:
            return None
        local_path = os.path.join(input_dir, s3_key)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        if not os.path.isfile(local_path):
            try:
                s3_client.download_file(bucket, s3_key, local_path)
            except ClientError as e:
                raise OrchestratorError(
                    f"Failed to download s3://{bucket}/{s3_key}: {e}"
                )
        downloaded.append(local_path)
        return local_path

    for task_label, runs in local.items():
        if task_label.startswith("_"):
            continue  # metadata such as _manifest
        if task_label == "anat":
            for anat_key in ("csf_mask", "wm_mask"):
                local["anat"][anat_key] = _download(runs[anat_key])
            continue
        for files in runs.values():
            for file_key in ("bold", "brainmask", "confounds", "events"):
                files[file_key] = _download(files[file_key])

    n_files = len(downloaded)
    logger.info("Downloaded %d input file(s) to %s", n_files, input_dir)
    return local, downloaded


def check_subject_input_completeness(discovered, analyses, proc_template, logger):
    """
    Pre-download presence check: every run needs a brainmask and confounds TSV, and sessions feeding
    analyses with non-null CSF_paths/WM_paths need CSF/WM masks (missing events exclude runs later).

    Parameters
    ----------
    discovered : dict
        discover_subject_inputs output for the sessions selected for processing.
    analyses : list of dict
        The 'analyses' section of the orchestrator config.
    proc_template : dict
        The proc template dict.
    logger : logging.Logger

    Raises
    ------
    OrchestratorError
        When any required file is absent, listing every missing item.
    """
    tmpl = {b["name"]: b for b in proc_template.get("analyses", [])}

    # Determine which tasks need CSF/WM masks
    needs_csf = set()
    needs_wm = set()
    for a in analyses:
        tname = a["name"]
        if tname in tmpl:
            paths = tmpl[tname].get("paths") or {}
            if paths.get("CSF_paths") is not None:
                needs_csf.add(a["task_label"])
            if paths.get("WM_paths") is not None:
                needs_wm.add(a["task_label"])

    problems = []

    if not discovered:
        problems.append("no sessions discovered")
    else:
        for ses in sorted(discovered):
            ses_data = discovered[ses]
            configured_tasks = [k for k in ses_data if k != "anat" and not k.startswith("_")]
            total_runs = sum(len(ses_data.get(t, {})) for t in configured_tasks)
            if total_runs == 0:
                problems.append(
                    f"ses-{ses}: no runs discovered for any configured task"
                )
                continue

            for task in sorted(configured_tasks):
                for run in sorted(ses_data.get(task, {})):
                    files = ses_data[task][run]
                    if files.get("brainmask") is None:
                        problems.append(
                            f"ses-{ses} task-{task} run-{run}: missing brainmask"
                        )
                    if files.get("confounds") is None:
                        problems.append(
                            f"ses-{ses} task-{task} run-{run}: missing confounds"
                        )

            # Tissue mask checks (session-level)
            anat = ses_data.get("anat", {})
            session_has_csf_task = any(
                len(ses_data.get(t, {})) > 0 for t in needs_csf
            )
            session_has_wm_task = any(
                len(ses_data.get(t, {})) > 0 for t in needs_wm
            )
            if session_has_csf_task and anat.get("csf_mask") is None:
                problems.append(
                    f"ses-{ses}: missing CSF mask (csf_mask) required by CSF_paths"
                )
            if session_has_wm_task and anat.get("wm_mask") is None:
                problems.append(
                    f"ses-{ses}: missing WM mask (wm_mask) required by WM_paths"
                )

    if problems:
        raise OrchestratorError(
            f"Subject input-completeness gate failed: {'; '.join(problems)}"
        )

    logger.info("Subject input-completeness gate passed.")


# ============================================================================
# Non-steady-state TR detection and removal
# ============================================================================

def detect_non_steady_state_trs(confounds_path, logger):
    """
    Count ``non_steady_state_outlier*`` confounds columns, one per leading NSS TR to remove.

    Parameters
    ----------
    confounds_path : str
        Path to the producer confounds TSV file.
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

def remove_initial_trs_bold(bold_path, n_remove, out_dir, out_prefix, logger):
    """
    Trim the first n_remove TRs with 3dTcat to ``{out_prefix}_trimmed.nii.gz``; with n_remove == 0
    the input path is returned unchanged.

    Parameters
    ----------
    bold_path : str
        Path to the input BOLD NIfTI.
    n_remove : int
        Number of initial TRs to remove.
    out_dir : str
        Directory to write the trimmed BOLD file into.
    out_prefix : str
        Filename prefix for the output file.
    logger : logging.Logger

    Returns
    -------
    tuple of (str, int)
        (trimmed BOLD path, remaining TR count, or -1 if 3dinfo fails).

    Raises
    ------
    OrchestratorError
        If 3dTcat fails or produces no output.
    """
    if n_remove == 0:
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

# ============================================================================
# Motion regressors and producer FD check
# ============================================================================

_FD_HEAD_RADIUS_MM = 50.0
_FD_CONSISTENCY_ATOL_MM = 1e-6


def _check_producer_fd_consistency(motion_df, confounds_path, logger):
    """
    Confirm rotations are in degrees: Power FD recomputed from the six motion parameters (50 mm
    radius) must match the producer FD within 1e-6 mm; a degree/radian mix-up differs by ~0.01-1 mm.

    Returns
    -------
    str
        "producer_fd_consistent", or "not_checkable" if FD/motion columns are absent or no frame
        has finite values in both.

    Raises
    ------
    OrchestratorError
        context reason 'producer_fd_inconsistent' when the values disagree.
    """
    base_cols = ["trans_x", "trans_y", "trans_z", "rot_x", "rot_y", "rot_z"]
    if "framewise_displacement" not in motion_df.columns or \
            any(c not in motion_df.columns for c in base_cols):
        logger.info(
            "Producer-FD check not performed for %s (FD or base motion column "
            "absent); rotations taken as degrees.", confounds_path
        )
        return "not_checkable"

    producer_fd = pd.to_numeric(
        motion_df["framewise_displacement"], errors="coerce"
    ).to_numpy(dtype=float)
    base = motion_df[base_cols].to_numpy(dtype=float)
    deltas = np.abs(np.diff(base, axis=0))
    deltas[:, 3:] *= math.pi / 180.0 * _FD_HEAD_RADIUS_MM
    recomputed_fd = np.concatenate([[np.nan], deltas.sum(axis=1)])

    comparable = np.isfinite(producer_fd) & np.isfinite(recomputed_fd)
    if not comparable.any():
        logger.info(
            "Producer-FD check not performed for %s (no finite FD frames); "
            "rotations taken as degrees.", confounds_path
        )
        return "not_checkable"

    if not np.allclose(recomputed_fd[comparable], producer_fd[comparable],
                       atol=_FD_CONSISTENCY_ATOL_MM, rtol=0):
        max_diff = float(np.max(np.abs(
            recomputed_fd[comparable] - producer_fd[comparable]
        )))
        raise OrchestratorError(
            f"Producer-FD consistency check failed for {confounds_path}: "
            f"Power FD recomputed with rotations in degrees differs from the "
            f"producer framewise_displacement by up to {max_diff:.6g} mm "
            f"(tolerance {_FD_CONSISTENCY_ATOL_MM:g} mm).",
            context={"reason": "producer_fd_inconsistent", "max_abs_diff_mm": max_diff},
        )

    logger.info("Producer-FD check passed for %s.", confounds_path)
    return "producer_fd_consistent"


def extract_motion_regressors(confounds_path, n_remove, calc_n_motion_derivs,
                              out_path, logger, expected_n_frames=None):
    """
    Write the six base motion parameters (mm, degrees) plus calc_n_motion_derivs finite-difference
    derivative sets, computed before the NSS trim; non-finite values after the trim raise (no imputation).

    Parameters
    ----------
    confounds_path : str
        Path to the producer confounds TSV file.
    n_remove : int
        Number of non-steady-state TRs to trim from the start.
    calc_n_motion_derivs : int
        Number of temporal derivative sets to include (>= 0).
    out_path : str
        Destination path for the output motion file.
    logger : logging.Logger
    expected_n_frames : int or None
        Required confounds row count (BOLD frames), if given.

    Returns
    -------
    tuple[str, str]
        (out_path, motion_units_check from _check_producer_fd_consistency).

    Raises
    ------
    OrchestratorError
        Context reason 'motion_non_finite' or 'producer_fd_inconsistent' (the caller excludes the
        run), or on missing columns or a row-count mismatch.
    """
    motion_df = pd.read_csv(confounds_path, sep="\t")

    if len(motion_df) == 0:
        raise OrchestratorError(f"Confounds file is empty (0 rows): {confounds_path}")

    if expected_n_frames is not None and len(motion_df) != expected_n_frames:
        raise OrchestratorError(
            f"Confounds rows ({len(motion_df)}) differ from BOLD frames "
            f"({expected_n_frames}) in {confounds_path}."
        )

    base_cols = ["trans_x", "trans_y", "trans_z", "rot_x", "rot_y", "rot_z"]

    # Select by name; column order is not guaranteed
    missing_base = [c for c in base_cols if c not in motion_df.columns]
    if missing_base:
        raise OrchestratorError(
            f"Missing base motion columns in {confounds_path}: {missing_base}. "
            f"Available columns: {list(motion_df.columns)}"
        )

    base_data = motion_df[base_cols]
    if base_data.isna().all().any():
        all_nan_cols = [c for c in base_cols if base_data[c].isna().all()]
        n_retained = len(motion_df) - n_remove
        raise OrchestratorError(
            f"Motion columns are entirely NaN in {confounds_path}: {all_nan_cols}. "
            f"Motion data may be corrupted.",
            context={
                "reason": "motion_non_finite",
                "n_affected_frames": max(n_retained, 0),
                "affected_frame_indices": list(range(max(n_retained, 0))),
            }
        )

    motion_units_check = _check_producer_fd_consistency(motion_df, confounds_path, logger)

    motion_array = motion_df[base_cols].values.copy()

    # Derivatives before the trim, so the first retained frame differences against a real frame
    arrays = [motion_array]
    prev_degree_data = arrays[0]

    for degree in range(1, calc_n_motion_derivs + 1):
        diff = np.diff(prev_degree_data, axis=0)
        deriv_data = np.vstack([np.zeros((1, prev_degree_data.shape[1])), diff])
        logger.info(
            "Computing motion derivative (degree %d) numerically via finite differences.",
            degree
        )
        arrays.append(deriv_data)
        prev_degree_data = deriv_data

    motion_data = np.hstack(arrays)

    if n_remove > 0:
        motion_data = motion_data[n_remove:, :]

    bad_rows = np.where(~np.all(np.isfinite(motion_data), axis=1))[0]
    if len(bad_rows) > 0:
        indices = [int(i) for i in bad_rows]
        raise OrchestratorError(
            f"Non-finite motion values detected in {confounds_path} after "
            f"derivative computation and NSS trim: {len(bad_rows)} affected "
            f"frame(s) at post-trim indices {indices[:20]}"
            + (" ..." if len(indices) > 20 else ""),
            context={
                "reason": "motion_non_finite",
                "n_affected_frames": int(len(bad_rows)),
                "affected_frame_indices": indices,
            }
        )

    np.savetxt(out_path, motion_data, fmt="%.10g", delimiter="\t")
    logger.info(
        "Motion regressors saved: %d columns, %d rows -> %s",
        motion_data.shape[1], motion_data.shape[0], out_path
    )
    return out_path, motion_units_check


def derive_calc_n_motion_derivs(analyses, proc_template):
    """Motion derivative sets to compute: the max over analyses of 1 for task_* blocks with
    include_motion_derivs, and max(motion_deriv_degree - 1, 0) for rest_conn.

    Parameters
    ----------
    analyses : list of dict
        The 'analyses' section of the orchestrator config.
    proc_template : dict
        The proc template dict.

    Returns
    -------
    int
        Number of motion derivative sets to compute (>= 0).
    """
    tmpl = {b["name"]: b for b in proc_template.get("analyses", [])}
    max_derivs = 0
    for a in analyses:
        block = tmpl.get(a["name"], {})
        atype = a["type"]
        if atype in ("task_act", "task_conn"):
            if block.get("include_motion_derivs", False):
                max_derivs = max(max_derivs, 1)
        elif atype == "rest_conn":
            mdd = block.get("motion_deriv_degree")
            if mdd is not None and mdd > 0:
                max_derivs = max(max_derivs, mdd - 1)
    return max_derivs


# ============================================================================
# Tissue and global-signal regressors from masks
# ============================================================================

def compute_tissue_mask_regressor(trimmed_bold_path, brainmask_path,
                                  tissue_mask_path, out_path, logger):
    """
    Mean time course in a tissue mask (brainmask for GS): NN-resample the mask to the BOLD grid,
    intersect with the run brainmask, require non-zero voxels, then 3dmaskave.

    Parameters
    ----------
    trimmed_bold_path : str
        Path to the NSS-trimmed BOLD NIfTI (same as scan_paths input).
    brainmask_path : str
        Path to the run's brain mask NIfTI.
    tissue_mask_path : str
        Path to the tissue mask NIfTI (CSF, WM, or brainmask for GS).
    out_path : str
        Destination path for the output regressor file (one value per volume).
    logger : logging.Logger

    Returns
    -------
    tuple of (str, int)
        (out_path, post-intersection non-zero voxel count).

    Raises
    ------
    OrchestratorError
        If the intersection is empty (reason 'tissue_mask_empty') or an AFNI command fails.
    """
    stem = os.path.splitext(out_path)[0]
    if out_path.endswith(".1D"):
        stem = out_path[:-3]
    resampled_path = stem + "_mask_resampled.nii.gz"
    intersect_path = stem + "_mask_intersect.nii.gz"

    try:
        # Resample tissue mask to BOLD grid (skip for global signal)
        is_global_signal = (
            os.path.realpath(tissue_mask_path) == os.path.realpath(brainmask_path)
        )
        if is_global_signal:
            mask_a_input = brainmask_path
        else:
            subprocess.run(
                ["3dresample", "-master", trimmed_bold_path, "-rmode", "NN",
                 "-input", tissue_mask_path, "-prefix", resampled_path],
                capture_output=True, text=True, check=True,
            )
            mask_a_input = resampled_path

        # Intersect with brainmask
        subprocess.run(
            ["3dcalc", "-a", mask_a_input, "-b", brainmask_path,
             "-expr", "step(a)*step(b)", "-prefix", intersect_path],
            capture_output=True, text=True, check=True,
        )

        # Count non-zero voxels via AFNI 3dBrickStat
        count_result = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", intersect_path],
            capture_output=True, text=True, check=True,
        )
        n_voxels = int(float(count_result.stdout.strip()))

        if n_voxels == 0:
            # Clean up intermediates before raising
            for tmp in (resampled_path, intersect_path):
                if os.path.isfile(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
            raise OrchestratorError(
                f"Tissue mask intersection has zero non-zero voxels for "
                f"{tissue_mask_path}",
                context={
                    "reason": "tissue_mask_empty",
                    "tissue_mask": tissue_mask_path,
                    "n_voxels": 0,
                }
            )

        # Extract mean time course
        result = subprocess.run(
            ["3dmaskave", "-quiet", "-mask", intersect_path, trimmed_bold_path],
            capture_output=True, text=True, check=True,
        )
        values = [float(v) for v in result.stdout.strip().split()]
        if not values:
            raise OrchestratorError(
                f"3dmaskave returned no values for {trimmed_bold_path} "
                f"with mask {intersect_path}"
            )

        np.savetxt(out_path, values, fmt="%.10g")
        logger.info(
            "Tissue regressor computed: %s (%d voxels, %d volumes)",
            out_path, n_voxels, len(values)
        )

    except subprocess.CalledProcessError as e:
        raise OrchestratorError(
            f"AFNI command failed during tissue regressor computation: "
            f"{e.stderr.strip() if e.stderr else str(e)}"
        )
    finally:
        # Clean up intermediates (best effort)
        for tmp in (resampled_path, intersect_path):
            if os.path.isfile(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    return out_path, n_voxels


# ============================================================================
# Task timing relabeling, conversion and validation
# ============================================================================

def fix_nback_cue_labels(events_path, condition_column, out_path, logger):
    """
    Relabel n-back "cue" trials by the next block's level: 0-back cues show the target to be matched
    ("target_0back", pooled across stimuli); 2-back cues are instruction screens ("instruction").

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
        If a cue trial has no later trial matching ``{level}_back_{stimulus}``.
    """
    events_df = pd.read_csv(events_path, sep="\t")

    if condition_column not in events_df.columns:
        raise OrchestratorError(
            f"Condition column '{condition_column}' not found in {events_path}. "
            f"Available columns: {list(events_df.columns)}"
        )

    nback_pattern = re.compile(r"^(\d+)_back_(.+)$")

    n_relabeled = 0
    n_target = 0
    n_instruction = 0
    for i in range(len(events_df)):
        trial_type = str(events_df.at[i, condition_column]).strip().strip('"')
        if trial_type.lower() != "cue":
            continue

        # Look ahead for the next {level}_back_{stimulus} trial to infer the cue's role
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

        level = match.group(1)

        if level == "0":
            events_df.at[i, condition_column] = "target_0back"
            n_target += 1
        else:
            events_df.at[i, condition_column] = "instruction"
            n_instruction += 1

        n_relabeled += 1

    events_df.to_csv(out_path, sep="\t", index=False)
    logger.info(
        "Relabeled %d cue trial(s) (%d as 'target_0back', %d as 'instruction') "
        "in n-back events file: %s -> %s",
        n_relabeled, n_target, n_instruction,
        os.path.basename(events_path), os.path.basename(out_path)
    )
    return out_path


def format_task_timing(events_path, condition_column, conditions_exclude,
                       n_remove, TR, out_path, logger):
    """
    Write a run-local timing TSV with onsets shifted by -n_remove * TR, dropping every event whose
    shifted onset is < 0; straddling events are dropped, not clipped, as clipping would misplace them.

    Parameters
    ----------
    events_path : str
        Path to the BIDS events TSV file.
    condition_column : str
    conditions_exclude : list of str or None
    n_remove : int
    TR : float
    out_path : str
    logger : logging.Logger

    Returns
    -------
    tuple of (str, int, int)
        (path, n_dropped, n_dropped_straddling: dropped events that ended after the cutoff).

    Raises
    ------
    OrchestratorError
        If required columns are missing or no events remain after filtering.
    """
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

    if conditions_exclude is not None:
        events_df = events_df[~events_df[condition_column].isin(conditions_exclude)]

    if len(events_df) == 0:
        raise OrchestratorError(f"No events remain after filtering in {events_path}")

    timing_df = pd.DataFrame({
        "trial_type": events_df[condition_column].values,
        "onset": events_df["onset"].astype(float).values,
        "duration": events_df["duration"].astype(float).values,
    })

    time_offset = n_remove * TR
    timing_df["onset"] = timing_df["onset"] - time_offset
    timing_df["end"] = timing_df["onset"] + timing_df["duration"]

    drop_mask = timing_df["onset"] < 0
    straddle_mask = drop_mask & (timing_df["end"] > 0)
    n_dropped = int(drop_mask.sum())
    n_dropped_straddling = int(straddle_mask.sum())
    straddling_types = timing_df.loc[straddle_mask, "trial_type"].tolist()

    timing_df = timing_df[~drop_mask].copy()
    timing_df.drop(columns=["end"], inplace=True)

    if n_dropped > 0:
        logger.warning(
            "%d event(s) dropped from %s (shifted onset < 0 after removing "
            "%d non-steady-state TR(s), %.2fs offset); %d straddled the "
            "cutoff: %s",
            n_dropped, events_path, n_remove, time_offset,
            n_dropped_straddling, straddling_types
        )

    timing_df = timing_df.sort_values("onset").reset_index(drop=True)
    timing_df.to_csv(out_path, sep="\t", index=False)
    logger.info("Formatted task timing -> %s (%d events)", out_path, len(timing_df))
    return out_path, n_dropped, n_dropped_straddling


def validate_task_timing(timing_path, n_frames, TR):
    """
    Require finite onsets/durations and onsets in [0, n_frames * TR) in a final per-run timing TSV.

    Parameters
    ----------
    timing_path : str
        Path to the timing TSV (trial_type, onset, duration).
    n_frames : int
        Number of retained BOLD frames after NSS trimming.
    TR : float
        Repetition time in seconds.

    Returns
    -------
    dict
        {"conditions_present": set of str, "n_events_written": int}

    Raises
    ------
    OrchestratorError
        With context reason 'events_invalid_values' or 'events_onset_out_of_range'.
    """
    df = pd.read_csv(timing_path, sep="\t")
    onset = pd.to_numeric(df["onset"], errors="coerce")
    duration = pd.to_numeric(df["duration"], errors="coerce")

    non_finite = (~np.isfinite(onset)) | (~np.isfinite(duration))
    n_bad = int(non_finite.sum())
    if n_bad > 0:
        raise OrchestratorError(
            f"{n_bad} event(s) with non-finite onset/duration in {timing_path}",
            context={"reason": "events_invalid_values", "n_rows": n_bad}
        )

    retained_seconds = n_frames * TR
    out_of_range = (onset < 0) | (onset >= retained_seconds)
    n_oor = int(out_of_range.sum())
    if n_oor > 0:
        raise OrchestratorError(
            f"{n_oor} event(s) with onset outside [0, {retained_seconds:.4f}) "
            f"in {timing_path}",
            context={
                "reason": "events_onset_out_of_range",
                "n_rows": n_oor,
                "retained_seconds": retained_seconds,
            }
        )

    conditions_present = set(df["trial_type"].unique())
    n_events_written = len(df)
    return {"conditions_present": conditions_present, "n_events_written": n_events_written}


def check_required_conditions_present(required_labels, conditions_present_by_run):
    """
    Return the required labels absent from the union of conditions across surviving runs.

    Parameters
    ----------
    required_labels : list of str
        Labels that must be present (e.g. cond_labels or cond_beta_labels).
    conditions_present_by_run : list of set of str
        Per-run condition sets from validate_task_timing.

    Returns
    -------
    list of str
        Sorted missing labels.
    """
    if not conditions_present_by_run:
        return sorted(required_labels)
    union = set()
    for s in conditions_present_by_run:
        union |= s
    return sorted(set(required_labels) - union)


# ============================================================================
# Preprocessing and first-level QC
# ============================================================================

def compute_tsnr(bold_path, mask_path, out_dir, out_prefix, logger):
    """
    Median in-mask temporal SNR, computed voxelwise as mean / max(stdev, 0.001) over time.

    Parameters
    ----------
    bold_path : str
        Path to the NSS-trimmed BOLD NIfTI.
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
        subprocess.run(
            ["3dTstat", "-mean", "-prefix", mean_path, bold_path],
            capture_output=True, text=True, check=True,
        )
        subprocess.run(
            ["3dTstat", "-stdev", "-prefix", stdev_path, bold_path],
            capture_output=True, text=True, check=True,
        )
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

    try:
        result = subprocess.run(
            ["3dBrickStat", "-mask", mask_path, "-percentile", "50", "1", "50", tsnr_path],
            capture_output=True, text=True, check=True,
        )
        parts = result.stdout.strip().split()
        median_tsnr = float(parts[1]) if len(parts) >= 2 else float(parts[0])
    except (subprocess.CalledProcessError, ValueError, IndexError):
        logger.warning("Could not compute median tSNR from %s", tsnr_path)
        return None

    for f in [mean_path, stdev_path]:
        if os.path.isfile(f):
            os.remove(f)

    logger.info("Median brain tSNR = %.2f", median_tsnr)
    return median_tsnr

def generate_carpet_plot(bold_path, mask_path, confounds_path, n_remove, out_path, logger):
    """
    Carpet plot: confounds DVARS trace (trimmed by n_remove) above a voxel-by-time heatmap.

    Parameters
    ----------
    bold_path : str
        Path to the NSS-trimmed BOLD NIfTI.
    mask_path : str
        Path to brain mask NIfTI.
    confounds_path : str
        Path to the producer confounds TSV (used for DVARS).
    n_remove : int
        Number of non-steady-state TRs already removed from the BOLD.
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
        logger.warning("matplotlib not available; skipping carpet plot generation.")
        return

    confounds_df = pd.read_csv(confounds_path, sep="\t")
    dvars = confounds_df.get("dvars", pd.Series(dtype=float)).values

    if n_remove > 0:
        dvars = dvars[n_remove:]

    n_vols = len(dvars)
    time_axis = np.arange(n_vols)

    max_voxels = 2000
    try:
        count_result = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", mask_path],
            capture_output=True, text=True, check=True,
        )
        n_voxels = int(float(count_result.stdout.split()[0]))
    except (subprocess.CalledProcessError, ValueError, IndexError):
        logger.warning("Could not determine mask voxel count for carpet plot; skipping.")
        return

    # Stream 3dmaskdump keeping evenly spaced voxel rows, so the full dump is never held in memory
    if n_voxels <= max_voxels:
        keep = None
    else:
        keep = set(np.linspace(0, n_voxels - 1, max_voxels, dtype=int).tolist())

    rows = []
    try:
        with subprocess.Popen(
            ["3dmaskdump", "-mask", mask_path, "-noijk", "-quiet", bold_path],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        ) as proc:
            for i, line in enumerate(proc.stdout):
                if keep is None or i in keep:
                    rows.append(list(map(float, line.split())))
    except (OSError, ValueError):
        logger.warning("Could not extract voxel data for carpet plot; skipping.")
        return
    if proc.returncode != 0:
        logger.warning("Could not extract voxel data for carpet plot; skipping.")
        return

    expected_rows = min(n_voxels, max_voxels)
    if len(rows) != expected_rows:
        logger.warning(
            "Carpet plot row count mismatch (expected %d, got %d); skipping.",
            expected_rows, len(rows)
        )
        return

    voxel_data = np.array(rows)

    vox_mean = np.mean(voxel_data, axis=1, keepdims=True)
    vox_std = np.std(voxel_data, axis=1, keepdims=True)
    vox_std[vox_std == 0] = 1
    voxel_z = (voxel_data - vox_mean) / vox_std

    fig = plt.figure(figsize=(12, 8))
    gs = GridSpec(2, 1, height_ratios=[1, 4], hspace=0.3)

    ax_dvars = fig.add_subplot(gs[0])
    ax_dvars.plot(time_axis, dvars, color="black", linewidth=0.5)
    ax_dvars.set_ylabel("DVARS")
    ax_dvars.set_xlim(0, n_vols - 1)
    ax_dvars.set_xticklabels([])

    ax_carpet = fig.add_subplot(gs[1])
    ax_carpet.imshow(voxel_z, aspect="auto", cmap="gray", interpolation="none",
                     vmin=-2, vmax=2)
    ax_carpet.set_xlabel("Volume")
    ax_carpet.set_ylabel("Voxels")

    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Carpet plot saved: %s", out_path)


def compute_preproc_qc(run_info, confounds_path, bold_path, mask_path,
                       n_remove, qc_config, out_dir, sub_id, logger):
    """
    Per-run pre-analysis QC (DVARS, tSNR, brain-mask coverage, carpet plot, producer FD), with
    confounds-derived series trimmed by n_remove to the retained frames.

    Parameters
    ----------
    run_info : dict
        Run metadata (session, task_label, run, run_label).
    confounds_path : str
        Path to the producer confounds TSV.
    bold_path : str
        Path to the NSS-trimmed BOLD NIfTI.
    mask_path : str
        Path to brain mask NIfTI.
    n_remove : int
        Number of non-steady-state TRs already removed from bold_path.
    qc_config : dict
        QC configuration (tsnr, carpet_plots flags).
    out_dir : str
        Output directory for QC files.
    sub_id : str
        Subject ID prefix (e.g., "sub-SUBJ01").
    logger : logging.Logger

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

    # DVARS metrics (from producer confounds)
    dvars = confounds_df.get("dvars", pd.Series(dtype=float)).values
    if n_remove > 0:
        dvars = dvars[n_remove:]
    dvars_valid = dvars[~np.isnan(dvars)]

    qc["dvars"] = {
        "mean": float(np.mean(dvars_valid)) if len(dvars_valid) > 0 else None,
        "max": float(np.max(dvars_valid)) if len(dvars_valid) > 0 else None,
    }

    # tSNR
    if qc_config.get("tsnr", False):
        run_label = run_info["run_label"]
        median_tsnr = compute_tsnr(bold_path, mask_path, out_dir,
                                   f"{sub_id}_{run_label}", logger)
        qc["tsnr"] = {"median_brain": median_tsnr}
    else:
        qc["tsnr"] = {"median_brain": None}

    # Brain mask coverage
    try:
        result = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", mask_path],
            capture_output=True, text=True, check=True,
        )
        n_voxels = int(float(result.stdout.strip()))

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

    # Carpet plot
    if qc_config.get("carpet_plots", False):
        carpet_path = os.path.join(out_dir, f"{sub_id}_{run_info['run_label']}_carpet.png")
        generate_carpet_plot(bold_path, mask_path, confounds_path,
                             n_remove, carpet_path, logger)
        qc["carpet_plot_path"] = carpet_path
    else:
        qc["carpet_plot_path"] = None

    # Producer FD is informational only; the censoring FD is computed by fmri-first-level-proc
    fd_col = confounds_df.get("framewise_displacement")
    if fd_col is not None:
        fd_values = pd.to_numeric(fd_col, errors="coerce").values
        if n_remove > 0:
            fd_values = fd_values[n_remove:]
        fd_valid = fd_values[np.isfinite(fd_values)]
        qc["producer_fd_mean"] = float(np.mean(fd_valid)) if len(fd_valid) > 0 else None
        qc["producer_fd_max"] = float(np.max(fd_valid)) if len(fd_valid) > 0 else None
    else:
        qc["producer_fd_mean"] = None
        qc["producer_fd_max"] = None

    return qc

def save_qc_json(qc_metrics, out_path, logger):
    """
    Write QC metrics to JSON, creating parent directories; non-serializable values become strings.

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

def json_safe_context(context):
    """JSON-serializable deep copy of an exception context dict (str() for other leaves; {} if not a dict)."""
    if not isinstance(context, dict):
        return {}
    return json.loads(json.dumps(context, default=str))


def _primary_output_ok(analysis_type, out_dir, out_file_pre):
    """Return True iff the primary upstream output exists and is non-empty."""
    try:
        fname = upstream_names.frozen_primary(analysis_type, out_file_pre)
    except KeyError:
        return False
    path = os.path.join(str(out_dir), fname)
    return os.path.isfile(path) and os.path.getsize(path) > 0


def compute_first_level_qc(analysis_name, analysis_type, out_dir, sub_id, out_file_pre, logger, error_msg=None):
    """
    Per-analysis QC from disk: success requires a non-empty frozen primary output and no error, and
    censoring stats come from the upstream QC summary (PSC intermediates not counted as outputs).

    Parameters
    ----------
    analysis_name : str
    analysis_type : str
    out_dir : str
        Base output directory containing analysis subdirectories.
    sub_id : str
    out_file_pre : str or None
    logger : logging.Logger
    error_msg : str or None

    Returns
    -------
    dict
        QC metrics dictionary.
    """
    analysis_dir = os.path.join(out_dir, analysis_name)
    output_files = sorted(os.listdir(analysis_dir)) if os.path.isdir(analysis_dir) else []

    nifti_outputs = [
        f for f in output_files
        if f.endswith(".nii.gz") and not f.endswith("_psc.nii.gz") and
        os.path.getsize(os.path.join(analysis_dir, f)) > 0
    ]
    completed_successfully = (
        _primary_output_ok(analysis_type, analysis_dir, out_file_pre)
        and error_msg is None
    )

    if not completed_successfully:
        if error_msg:
            logger.warning(
                "First-level QC: analysis '%s' marked as failed: %s",
                analysis_name, error_msg
            )
        else:
            logger.warning(
                "First-level QC: analysis '%s' produced no non-empty NIfTI outputs "
                "in %s; marking as failed.",
                analysis_name, analysis_dir
            )
    else:
        logger.info(
            "First-level QC: analysis '%s' completed successfully (%d NIfTI output(s)).",
            analysis_name, len(nifti_outputs)
        )

    pct_censored = None
    upstream_qc = None
    if out_file_pre is not None:
        qc_json_path = os.path.join(
            analysis_dir, upstream_names.qc_summary(out_file_pre)
        )
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
        "upstream_qc_summary": upstream_qc,
    }

    return qc


def build_first_level_qc_from_result(analysis_name, analysis_type, sub_id, result, out_dir, out_file_pre):
    """Per-analysis QC from a FirstLevelResult (same keys as compute_first_level_qc); success still
    requires a non-empty frozen primary output on disk.

    Parameters
    ----------
    analysis_name : str
    analysis_type : str
    sub_id : str
    result : fmri_first_level_proc.FirstLevelResult
    out_dir : str
    out_file_pre : str
    """
    v = (result.qc_summary or {}).get("pct_censored")
    pct_censored = round(float(v), 2) if v is not None else None
    completed_successfully = _primary_output_ok(analysis_type, out_dir, out_file_pre)
    n_nifti_outputs = sum(
        1 for p in result.output_files
        if os.path.basename(p).endswith(".nii.gz")
        and not os.path.basename(p).endswith("_psc.nii.gz")
    )
    return {
        "sub_id": sub_id,
        "analysis_name": analysis_name,
        "type": analysis_type,
        "pct_censored": pct_censored,
        "completed_successfully": completed_successfully,
        "error": None,
        "n_nifti_outputs": n_nifti_outputs,
        "output_files": sorted(os.path.basename(p) for p in result.output_files),
        "upstream_qc_summary": result.qc_summary,
    }


# ============================================================================
# Peak memory measurement
# ============================================================================

def get_peak_memory_usage():
    """Return peak memory usage from cgroup v2 or rusage fallback.

    Returns
    -------
    tuple of (int or None, str)
        (peak_bytes, source) where source is "cgroup_v2" or
        "rusage_largest_child".
    """
    cgroup_path = "/sys/fs/cgroup/memory.peak"
    if os.path.isfile(cgroup_path):
        try:
            with open(cgroup_path) as f:
                peak = int(f.read().strip())
            return (peak, "cgroup_v2")
        except (ValueError, OSError):
            pass

    try:
        r = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        if sys.platform == "darwin":
            peak_bytes = r  # macOS reports bytes
        else:
            peak_bytes = r * 1024  # Linux reports KB
        return (int(peak_bytes) if peak_bytes > 0 else None, "rusage_largest_child")
    except Exception:
        return (None, "rusage_largest_child")


# ============================================================================
# Outcome classification and exit codes
# ============================================================================

def is_outcome_terminal(status, reason=None, error_type=None):
    """Determine whether an analysis outcome is terminal (rerunning cannot change it).

    Parameters
    ----------
    status : str
        "success", "failed", or "skipped".
    reason : str or None
        Skip reason (e.g. "no_usable_runs", "events_missing_conditions").
    error_type : str or None
        Exception class name (e.g. "ModelError", "RuntimeError").

    Returns
    -------
    bool
    """
    if status == "success":
        return True
    if status == "skipped" and reason in ("no_usable_runs", "events_missing_conditions"):
        return True
    if status == "failed" and error_type == "ModelError":
        return True
    return False


def classify_session_exit(session_error, analysis_outcomes, preproc_excluded_runs,
                          skip_first_level):
    """Exit category: 1 for a session exception, unexplained run exclusion, or non-terminal outcome;
    else 2 for a terminal ModelError; else 0.

    Parameters
    ----------
    session_error : str or None
        The session-level error message, if any.
    analysis_outcomes : list of dict
        Per-analysis outcomes with 'terminal', 'status', and 'error_type' keys.
    preproc_excluded_runs : list of dict
        Run-level exclusion records with a 'reason' key.
    skip_first_level : bool
        When True, analysis outcomes are ignored.

    Returns
    -------
    int
        EXIT_COMPLETE, EXIT_FAILED_UNEXPLAINED, or EXIT_FAILED_KNOWN_CAUSE.
    """
    if session_error:
        return EXIT_FAILED_UNEXPLAINED

    if any(e.get("reason") in _UNEXPLAINED_EXCLUSION_REASONS for e in preproc_excluded_runs):
        return EXIT_FAILED_UNEXPLAINED

    if not skip_first_level:
        if any(not o.get("terminal", False) for o in analysis_outcomes):
            return EXIT_FAILED_UNEXPLAINED

        if any(
            o.get("status") == "failed" and o.get("error_type") == "ModelError"
            for o in analysis_outcomes
        ):
            return EXIT_FAILED_KNOWN_CAUSE

    return EXIT_COMPLETE


def most_severe_exit(codes):
    """Most severe exit code, with precedence 1 (unexplained) over 2 (known cause) over 0.

    Parameters
    ----------
    codes : iterable of int
        Exit category codes.

    Returns
    -------
    int
    """
    code_set = set(codes)
    if EXIT_FAILED_UNEXPLAINED in code_set:
        return EXIT_FAILED_UNEXPLAINED
    if EXIT_FAILED_KNOWN_CAUSE in code_set:
        return EXIT_FAILED_KNOWN_CAUSE
    return EXIT_COMPLETE


# ============================================================================
# Session QC consolidation
# ============================================================================

def consolidate_session_qc(sub_id, ses_label, session_status, session_wall_time,
                           preproc_qc_by_run, analysis_outcomes,
                           preproc_excluded_runs, out_path, logger,
                           producer_manifest=None):
    """
    Write the session QC JSON combining per-run preprocessing QC and per-analysis outcomes.

    Parameters
    ----------
    sub_id : str
    ses_label : str
    session_status : str
    session_wall_time : float
    preproc_qc_by_run : dict
    analysis_outcomes : list of dict
    preproc_excluded_runs : list of dict
        Run-level exclusion records (written through verbatim).
    out_path : str
    logger : logging.Logger
    producer_manifest : dict or None
        Producer manifest metadata from discovery, written to provenance.producer_manifest.

    Returns
    -------
    str
        Path to the written QC JSON file.
    """
    try:
        from orchestrate_first_level import __version__ as _orch_ver
    except ImportError:
        _orch_ver = "unknown"

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

    peak_bytes, peak_source = get_peak_memory_usage()

    # Build session flags
    flags = []
    if preproc_excluded_runs:
        detail_parts = [
            f"{e['run_label']} ({e.get('reason', 'unknown')})"
            for e in preproc_excluded_runs
        ]
        flags.append({
            "code": "preproc_excluded_runs",
            "detail": f"{len(preproc_excluded_runs)} run(s) excluded: "
                      + ", ".join(detail_parts),
        })

    skipped = [o for o in analysis_outcomes if o.get("status") == "skipped"]
    if skipped:
        detail_parts = [
            f"{o['name']} ({o.get('reason', 'unknown')})" for o in skipped
        ]
        flags.append({
            "code": "analysis_skipped",
            "detail": ", ".join(detail_parts),
        })

    failed = [o for o in analysis_outcomes if o.get("status") == "failed"]
    if failed:
        detail_parts = [
            f"{o['name']} ({o.get('error_type', 'unknown')})" for o in failed
        ]
        flags.append({
            "code": "analysis_failed",
            "detail": ", ".join(detail_parts),
        })

    upstream_warned = [o for o in analysis_outcomes if o.get("qc_flagged")]
    if upstream_warned:
        flags.append({
            "code": "upstream_warning",
            "detail": ", ".join(o["name"] for o in upstream_warned),
        })

    all_terminal = (
        len(analysis_outcomes) > 0
        and all(o.get("terminal", False) for o in analysis_outcomes)
    )

    qc = {
        "provenance": {
            "orchestrator_version": _orch_ver,
            "fmri_first_level_proc_version": proc_ver,
            "afni_version": afni_ver,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "sub_id": sub_id,
            "session": ses_label,
            "producer_manifest": producer_manifest,
        },
        "preprocessing": preproc_qc_by_run,
        "preproc_excluded_runs": preproc_excluded_runs,
        "analyses": {},
        "session": {
            "status": session_status,
            "wall_time_seconds": round(session_wall_time, 2),
            "n_analyses_attempted": len(analysis_outcomes),
            "n_analyses_succeeded": sum(
                1 for o in analysis_outcomes if o["status"] == "success"
            ),
            "n_analyses_skipped": sum(
                1 for o in analysis_outcomes if o.get("status") == "skipped"
            ),
            "n_analyses_flagged": sum(
                1 for o in analysis_outcomes if o.get("qc_flagged")
            ),
            "any_analysis_flagged": any(
                o.get("qc_flagged") for o in analysis_outcomes
            ),
            "all_terminal": all_terminal,
            "peak_memory_bytes": peak_bytes,
            "peak_memory_source": peak_source,
            "flags": flags,
            "any_flag": len(flags) > 0,
        },
    }

    for outcome in analysis_outcomes:
        qc["analyses"][outcome["name"]] = {
            "type": outcome["type"],
            "status": outcome["status"],
            "reason": outcome.get("reason"),
            "upstream_status": outcome.get("upstream_status"),
            "qc_flagged": bool(outcome.get("qc_flagged", False)),
            "error": outcome.get("error"),
            "error_type": outcome.get("error_type"),
            "error_context": outcome.get("error_context", {}),
            "warnings": outcome.get("warnings", []),
            "wall_time_seconds": outcome.get("wall_time_seconds", 0.0),
            "qc": outcome.get("qc"),
            "run_labels": outcome.get("run_labels", []),
            "terminal": bool(outcome.get("terminal", False)),
        }

    save_qc_json(qc, out_path, logger)
    return out_path


# ============================================================================
# AFNI unvalidated-version warning filter
# ============================================================================

class AfniUnvalidatedVersionFilter(logging.Filter):
    """Suppress upstream '[UNVALIDATED_VERSION] AFNI ...' records; AFNI at or above the upstream
    minimum is accepted, while Python/numpy/pandas drift warnings are kept."""

    def filter(self, record):
        """Return False for AFNI unvalidated-version records, True otherwise."""
        msg = record.getMessage()
        if msg.startswith("[UNVALIDATED_VERSION] AFNI "):
            return False
        return True


def attach_afni_version_filter(logger):
    """Attach an AfniUnvalidatedVersionFilter to ``logger`` if not already present."""
    for f in logger.filters:
        if isinstance(f, AfniUnvalidatedVersionFilter):
            return
    logger.addFilter(AfniUnvalidatedVersionFilter())


def filter_afni_unvalidated_warnings(warnings):
    """Drop AFNI UNVALIDATED_VERSION records from FirstLevelResult warnings (other drift warnings kept)."""
    return [
        w for w in warnings
        if not (
            w.get("code") == "UNVALIDATED_VERSION"
            and str(w.get("message", "")).startswith("AFNI ")
        )
    ]


# ============================================================================
# First-level config building
# ============================================================================

def build_first_level_config(sub_id, session, study_config,
                             processed_files, analyses, proc_template, logger):
    """
    Copy the proc template and fill only subject-specific fields (out_dir, prefixes, per-run paths,
    run_labels); CSF/WM/GS paths are filled only where the template key is non-null.

    Parameters
    ----------
    sub_id : str
    session : str
    study_config : dict
    processed_files : dict
        {task_label: {"run_labels": [int], "bolds": [str], "motions": [str],
         "timings": [str] (task only), "csf": [str] or None,
         "wm": [str] or None, "gs": [str] or None}}
    analyses : list of dict
    proc_template : dict
    logger : logging.Logger

    Returns
    -------
    dict

    Raises
    ------
    OrchestratorError
        If global.tr is missing, mismatches study.TR, or a non-null template
        tissue key has no corresponding processed files.
    """
    config = copy.deepcopy(proc_template)
    output_dir = study_config["output_dir"]
    ses_label = f"ses-{session}A"
    session_out = os.path.join(output_dir, f"sub-{sub_id}", ses_label)

    # global.tr is never injected, only checked against study.TR
    config["global"] = config.get("global", {})
    if "tr" not in config["global"]:
        raise OrchestratorError(
            "global.tr is required in the proc template (it is never injected "
            "by the orchestrator)."
        )
    if abs(config["global"]["tr"] - study_config["TR"]) > 1e-6:
        raise OrchestratorError(
            f"global.tr in proc template ({config['global']['tr']}) does not match "
            f"study.TR ({study_config['TR']}). These must be consistent."
        )

    template_by_name = {}
    for block in config.get("analyses", []):
        template_by_name[block["name"]] = block

    referenced_names = set()

    for orch_analysis in analyses:
        analysis_name = orch_analysis["name"]
        analysis_type = orch_analysis["type"]
        task_label = orch_analysis["task_label"]
        referenced_names.add(analysis_name)

        if analysis_name not in template_by_name:
            raise OrchestratorError(
                f"Analysis '{analysis_name}' not found in proc template. "
                f"Available template analyses: {list(template_by_name.keys())}"
            )

        block = template_by_name[analysis_name]

        pf = processed_files.get(task_label)
        if pf is None:
            raise OrchestratorError(
                f"No processed files found for task '{task_label}' "
                f"(analysis '{analysis_name}')."
            )

        block["out_dir"] = os.path.join(session_out, "first_level_out", analysis_name)
        prefix_base = f"sub-{sub_id}_{ses_label}"
        block["out_file_pre"] = f"{prefix_base}_{orch_analysis['post_id_out_pre']}"

        block["run_labels"] = [int(r) for r in pf["run_labels"]]

        tmpl_paths = block.get("paths") or {}

        if analysis_type in ("task_act", "task_conn"):
            paths = {
                "scan_paths": list(pf["bolds"]),
                "motion_paths": list(pf["motions"]),
                "task_timing_paths": list(pf["timings"]),
            }
        elif analysis_type == "rest_conn":
            paths = {
                "scan_paths": list(pf["bolds"]),
                "motion_paths": list(pf["motions"]),
            }
        else:
            paths = {}

        # Tissue/GS paths only where the template requests them (proc config is authoritative)
        for key, pf_key in (("CSF_paths", "csf"), ("WM_paths", "wm"), ("GS_paths", "gs")):
            if tmpl_paths.get(key) is not None:
                pf_value = pf.get(pf_key)
                if pf_value is None:
                    raise OrchestratorError(
                        f"[{analysis_name}] template requests '{key}' (non-null) "
                        f"but no per-run regressors were produced for '{pf_key}'."
                    )
                paths[key] = list(pf_value)

        block["paths"] = paths

        if "post_id_extract_pre" in orch_analysis and "extraction" in block:
            block["extraction"]["extract_out_file_pre"] = (
                f"{prefix_base}_{orch_analysis['post_id_extract_pre']}"
            )

        if "post_id_conn_pre" in orch_analysis and "connectivity" in block:
            block["connectivity"]["conn_out_file_pre"] = (
                f"{prefix_base}_{orch_analysis['post_id_conn_pre']}"
            )

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
    Write the config to ``{out_dir}/sub-{sub_id}_ses-{session}A_first_level_config.yaml``.

    Parameters
    ----------
    config_dict : dict
    out_dir : str
    sub_id : str
    session : str
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
# Orchestrator config loading and proc-template validation
# ============================================================================

def load_orchestrator_config(config_path, logger):
    """
    Load and validate the orchestrator YAML config; unsupported keys raise with their replacement
    named, and the s3 section is required with s3.enabled true.

    Parameters
    ----------
    config_path : str
    logger : logging.Logger

    Returns
    -------
    dict

    Raises
    ------
    OrchestratorError
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

    required_sections = ["study", "tasks", "analyses", "s3"]
    for section in required_sections:
        if section not in config:
            raise OrchestratorError(f"Config missing required section '{section}'.")

    study = config["study"]

    if "fmriprep_dir" in study:
        raise OrchestratorError(
            "study.fmriprep_dir is retired; use study.input_dir "
            "(absolute path to the local input directory mirroring the S3 layout)."
        )
    if "calc_n_motion_derivs" in study:
        raise OrchestratorError(
            "study.calc_n_motion_derivs is retired (now derived from the proc "
            "template analysis blocks). Remove this key."
        )

    required_study_keys = ["input_dir", "output_dir", "space", "TR"]
    for key in required_study_keys:
        if key not in study or study[key] is None:
            raise OrchestratorError(f"Config study section missing required key '{key}'.")

    for path_key in ("input_dir", "output_dir"):
        val = study[path_key]
        if not isinstance(val, str) or not os.path.isabs(val):
            raise OrchestratorError(
                f"study.{path_key} must be an absolute path, got: {val!r}"
            )

    if not isinstance(study["TR"], (int, float)) or study["TR"] <= 0:
        raise OrchestratorError(f"study.TR must be a positive number, got: {study['TR']}")

    if "smoothing" in config:
        raise OrchestratorError(
            "Top-level 'smoothing' section is retired. Use per-block "
            "smoothing {enabled, fwhm} in the proc template config."
        )

    qc_cfg = config.get("qc", {})
    preproc_qc = qc_cfg.get("preproc", {})
    if "registration_quality" in preproc_qc:
        raise OrchestratorError(
            "qc.preproc.registration_quality is retired (registration Dice "
            "QC has been removed). Remove this key."
        )

    tasks = config["tasks"]
    if not isinstance(tasks, list) or len(tasks) == 0:
        raise OrchestratorError("'tasks' must be a non-empty list.")

    task_labels = set()
    for i, task in enumerate(tasks):
        if "task_label" not in task:
            raise OrchestratorError(f"tasks[{i}] missing required key 'task_label'.")
        task_labels.add(task["task_label"])
        if task["task_label"] not in VALID_TASK_LABELS:
            raise OrchestratorError(
                f"tasks[{i}] task_label '{task['task_label']}' is not a recognized "
                f"task label for this orchestrator. Valid labels: "
                f"{sorted(VALID_TASK_LABELS)}. Check the 'tasks' section "
                f"of the orchestrator config."
            )
        if "concatenate_runs" in task:
            raise OrchestratorError(
                f"Task '{task['task_label']}': 'concatenate_runs' is no longer "
                f"supported (removed in orchestrator 4.0). Runs are passed to "
                f"fmri_first_level_proc as per-run lists; multi-run task "
                f"analyses are concatenated upstream. Remove this key from "
                f"the orchestrator config."
            )

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

        if "post_id_out_pre" not in analysis:
            raise OrchestratorError(
                f"[{name}] Missing required key 'post_id_out_pre'."
            )

        # Censoring settings belong to the proc config
        if "fd_threshold" in analysis:
            raise OrchestratorError(
                f"[{name}] analyses[*].fd_threshold is retired (set in the proc "
                f"template config, not the orchestrator config). Remove this key."
            )
        if "censor_prev_tr" in analysis:
            raise OrchestratorError(
                f"[{name}] analyses[*].censor_prev_tr is retired (set in the proc "
                f"template config, not the orchestrator config). Remove this key."
            )

        atype = analysis["type"]

        if atype in ("task_conn", "rest_conn"):
            if "post_id_conn_pre" not in analysis:
                raise OrchestratorError(
                    f"[{name}] {atype} analysis requires 'post_id_conn_pre'."
                )

        # Proc-template fields are ignored here, with a warning
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

    s3_cfg = config["s3"] or {}
    for retired_key in ("fmriprep_s3_prefix", "mmps_mproc_s3_prefix", "available_sessions"):
        if retired_key in s3_cfg:
            raise OrchestratorError(
                f"s3.{retired_key} is retired. Use s3.func_prefix_template and "
                f"s3.anat_prefix_template instead."
            )

    if not isinstance(s3_cfg, dict) or s3_cfg.get("enabled") is not True:
        raise OrchestratorError(
            "s3.enabled must be true: this orchestrator reads its inputs from S3 "
            "and writes its outputs to S3."
        )

    required_s3_fields = [
        "bucket", "func_prefix_template", "anat_prefix_template", "upload_prefix"
    ]
    for field in required_s3_fields:
        if not s3_cfg.get(field):
            raise OrchestratorError(
                f"Required field 's3.{field}' is null or missing."
            )

    if s3_cfg["bucket"].startswith("s3://"):
        raise OrchestratorError(
            f"s3.bucket must be the bucket name only (no 's3://' prefix), "
            f"got: {s3_cfg['bucket']}"
        )

    for prefix_field in ["func_prefix_template", "anat_prefix_template", "upload_prefix"]:
        val = s3_cfg.get(prefix_field, "")
        if val and (val.startswith("/") or val.endswith("/")):
            raise OrchestratorError(
                f"s3.{prefix_field} must not have a leading or trailing '/', got: {val}"
            )

    for tmpl_field in ("func_prefix_template", "anat_prefix_template"):
        tmpl_val = s3_cfg.get(tmpl_field, "")
        if "{subject}" not in tmpl_val or "{session}" not in tmpl_val:
            raise OrchestratorError(
                f"s3.{tmpl_field} must contain both '{{subject}}' and "
                f"'{{session}}' placeholders, got: {tmpl_val}"
            )

    study.setdefault("force_recompute", False)
    if not isinstance(study.get("force_recompute"), bool):
        raise OrchestratorError(
            "study.force_recompute must be a boolean, got: "
            f"{study['force_recompute']!r}"
        )

    config.setdefault("qc", {"preproc": {"enabled": False}, "first_level": {"enabled": False}})
    config["s3"].setdefault("cleanup_after_upload", True)
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
    Cross-validate the orchestrator config against the proc template (global.tr, analysis names,
    types, required prefixes, unsupported path/extraction keys); unreferenced blocks only warn.

    Parameters
    ----------
    orchestrator_config : dict
    proc_template : dict
    logger : logging.Logger

    Raises
    ------
    OrchestratorError
    """
    if proc_template is None:
        raise OrchestratorError("Proc template config is empty (null).")

    template_analyses = proc_template.get("analyses")
    if not isinstance(template_analyses, list) or len(template_analyses) == 0:
        raise OrchestratorError(
            "Proc template must have 'analyses' as a non-empty list."
        )

    global_block = proc_template.get("global")
    if not isinstance(global_block, dict):
        raise OrchestratorError(
            "Proc template must have a 'global' block (dict). "
            "Required fields: num_cores, tr, template_path, force_diff_atlas."
        )

    if "tr" not in global_block:
        raise OrchestratorError(
            "global.tr is required in the proc template (it is never injected "
            "by the orchestrator)."
        )

    tpl = global_block.get("template_path")
    if tpl is not None and not os.path.isabs(tpl):
        raise OrchestratorError(
            f"global.template_path must be an absolute path when set, "
            f"got: {tpl!r}"
        )

    study_tr = orchestrator_config.get("study", {}).get("TR")
    if study_tr is not None and abs(global_block["tr"] - study_tr) > 1e-6:
        raise OrchestratorError(
            f"global.tr in proc template ({global_block['tr']}) does not match "
            f"study.TR ({study_tr}). These must be consistent."
        )

    template_by_name = {}
    for block in template_analyses:
        bname = block.get("name")
        if bname is None:
            raise OrchestratorError(
                "Proc template has an analysis block without a 'name' field."
            )

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

            block_type = block.get("type", "")
            if block_type in ("task_act", "task_conn"):
                for singular_key in ("CSF_path", "WM_path"):
                    if singular_key in paths_block and paths_block[singular_key] is not None:
                        raise OrchestratorError(
                            f"Proc template analysis '{bname}' uses singular "
                            f"'{singular_key}' in its paths block. Task blocks "
                            f"accept only the Mode A plural form (CSF_paths/WM_paths)."
                        )

        extraction = block.get("extraction")
        if isinstance(extraction, dict):
            legacy = sorted(k for k in ("extract", "extract_labels", "extract_stat") if k in extraction)
            if legacy:
                raise OrchestratorError(
                    f"Proc template analysis '{bname}' uses legacy extraction keys "
                    f"{legacy}. These are not read by fmri_first_level_proc >= "
                    f"{MIN_UPSTREAM_VERSION}; specify outputs via 'stat_buckets'."
                )

        template_by_name[bname] = block

    orch_analyses = orchestrator_config["analyses"]
    referenced_names = set()

    for orch_analysis in orch_analyses:
        name = orch_analysis["name"]
        atype = orch_analysis["type"]
        referenced_names.add(name)

        if name not in template_by_name:
            raise OrchestratorError(
                f"Orchestrator analysis '{name}' has no matching entry in proc template. "
                f"Available template analyses: {list(template_by_name.keys())}"
            )

        tmpl_block = template_by_name[name]

        tmpl_type = tmpl_block.get("type")
        if tmpl_type != atype:
            raise OrchestratorError(
                f"Type mismatch for analysis '{name}': "
                f"orchestrator has '{atype}', proc template has '{tmpl_type}'."
            )

        if "post_id_out_pre" not in orch_analysis:
            raise OrchestratorError(
                f"[{name}] Missing required key 'post_id_out_pre'."
            )

        if "extraction" in tmpl_block:
            if "post_id_extract_pre" not in orch_analysis:
                raise OrchestratorError(
                    f"[{name}] Proc template has an 'extraction' block but orchestrator "
                    f"analysis is missing 'post_id_extract_pre'."
                )

        if atype in ("task_conn", "rest_conn"):
            if "post_id_conn_pre" not in orch_analysis:
                raise OrchestratorError(
                    f"[{name}] {atype} analysis requires 'post_id_conn_pre'."
                )

    unreferenced = [n for n in template_by_name if n not in referenced_names]
    if unreferenced:
        logger.warning(
            "Proc template contains %d analysis block(s) not referenced by "
            "orchestrator config (will be removed from generated config): %s",
            len(unreferenced), unreferenced
        )

    logger.info("Proc template cross-validation passed.")


# ============================================================================
# Local input cleanup
# ============================================================================

def cleanup_local_inputs(downloaded_paths, logger):
    """
    Delete exactly the downloaded input files after a successful upload; per-file errors only warn.

    Parameters
    ----------
    downloaded_paths : list of str
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
