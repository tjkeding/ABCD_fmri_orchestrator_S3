<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-10-26T17:34:02Z" supersedes="ABCD_fmri_orchestrator_S3_implement_plan_20261026_091842.md" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_brainstorm_20261022_174139.md" mode="brainstorm" key_items="4 (H2, H3, H7, H8)" />
    <report path="(inline user spec, G-series audit decisions)" mode="orchestrator audit" key_items="8 (G2, G3, G7a, G7b, G8, G10-G12, G14)" />
  </input_reports>

  <decisions_without_explicit_approval>
    These were resolved during this revision; each follows directly from a locked decision. Object before the build if any is wrong.
    1. Manifest metadata is stored in the discovery result as a reserved "_manifest" key per session. The two functions that iterate every session key (check_subject_input_completeness, download_session_inputs) skip keys that start with "_". The pipeline passes the metadata into the session QC JSON as provenance.producer_manifest. This implements the locked H7 item (4) "record manifest written_at and schema_version in session QC", which the superseded plan stored but never wrote out.
    2. Manifest presence and location come from the discovery listing itself (the "_complete.json" key or local path found next to the session's files), not from the S3 path template. There is no 404 round trip, and local mode works without any template.
    3. The producer-FD check also runs on a motion-file cache hit (it re-reads the confounds TSV, which is cheap). Returning "not_checkable" on every rerun would contradict the locked definition of that value (FD column absent or entirely non-finite).
    4. Frames where the recomputed FD or the producer FD is non-finite are excluded from the comparison. Non-finite motion keeps its existing exclusion reason (motion_non_finite, raised after the trim); without this, a NaN motion frame would be misclassified as producer_fd_inconsistent.
    5. A run excluded as producer_fd_inconsistent also gets motion_units_check = "producer_fd_inconsistent" in its per-run QC, so the locked three-value QC field is actually populated for that value.
    6. Batch-runner exit code: 1 if any subject is failed_unexplained (orchestrator exit 1, any other nonzero exit code, or a worker exception); otherwise 2 if any subject is failed_known_cause; otherwise 0; 130 on interrupt. This mirrors most_severe_exit in the orchestrator.
    7. Existing tests that assert removed behavior (the rotation-unit heuristic, event clipping, the events_clipped_at_start flag, the four-element timing return) are rewritten to the new contract in place, and every confounds fixture is given a producer FD column consistent with its motion. Without this the suite cannot run, because the FD guard rejects the current random FD fixtures. New-behavior tests beyond those listed in this plan (GE DV26 fixture, single-run trial-floor case, end-to-end producer_fd_inconsistent exit path, batch-runner exit mapping) stay with /test design per the brainstorm action items.
    8. The mock 3dBrickStat counts the non-zero voxels of the NIfTI it is given (it currently returns a constant 80). Otherwise the existing zero-voxel test can never reach the tissue_mask_empty branch now that production counts voxels with 3dBrickStat.
  </decisions_without_explicit_approval>

  <build_status>
    COMPLETE in the working tree (the build must NOT redo these): C1, C2 (production code and environment.yaml), C3, C4, and the `import math` line. A snapshot of every target file from before the build is in the sandbox at prebuild_backup_20261026/. Line numbers in the superseded plan are stale because C1 removed about 130 lines; every edit below is located by quoted text or by function name, never by line number.
  </build_status>

  <changes>

    <change id="C1" priority="P0" source_item="G8: remove tarball-era dead code" status="COMPLETE">
      <file path="orchestrator_utils.py" action="modify" />
      <description>COMPLETE. decompress_if_needed with its Section D banner, remove_initial_trs_tabular, and the bz2, glob, gzip and tarfile imports are removed; `import math` is added after `import sys`.</description>
    </change>

    <change id="C2" priority="P0" source_item="G3: remove nibabel from production code" status="COMPLETE">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="environment.yaml" action="modify" />
      <description>COMPLETE. The nibabel import is removed. compute_tissue_mask_regressor step 3 counts voxels with `3dBrickStat -count -non-zero` (output verified against real AFNI: a bare integer with trailing whitespace, "0" for an empty mask), and its docstring step 3 is updated. The environment.yaml nibabel comment now reads "NIfTI I/O for test fixtures (not used in production code)". The mock follow-up is C16.</description>
    </change>

    <change id="C3" priority="P0" source_item="G7a+G7b+H7a: discovery patterns" status="COMPLETE">
      <file path="orchestrator_utils.py" action="modify" />
      <description>COMPLETE. _SES requires the A suffix; BOLD and brainmask patterns accept only the bare `_bold.nii.gz` and `_brainmask.nii.gz` names; CSF and WM patterns accept an optional `_res-&lt;alnum&gt;` entity; the comment block is updated.</description>
    </change>

    <change id="C4" priority="P0" source_item="H7d: known non-input files" status="COMPLETE">
      <file path="orchestrator_utils.py" action="modify" />
      <description>COMPLETE. _KNOWN_NON_INPUT_PATTERNS is added, and the candidate loop logs matches at DEBUG and every other unmatched file at WARNING.</description>
    </change>

    <change id="C5" priority="P0" source_item="H7 item (4): producer completion gate">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Add the _complete.json gate as Phase 4 of discover_subject_inputs, plus a small manifest reader.</description>
      <spec>(1) Add this helper immediately above `def discover_subject_inputs`:
```python
def _read_completion_manifest(s3_config, ref):
    """Load a producer _complete.json from an S3 key (S3 mode) or a local path."""
    try:
        if s3_config.get("enabled", False):
            resp = _get_s3_client().get_object(Bucket=s3_config["bucket"], Key=ref)
            manifest = json.loads(resp["Body"].read())
        else:
            with open(ref) as f:
                manifest = json.load(f)
    except (ClientError, OSError, ValueError) as e:
        raise OrchestratorError(f"Could not read completion manifest {ref}: {e}")
    if not isinstance(manifest, dict):
        raise OrchestratorError(f"Completion manifest {ref} is not a JSON object")
    return manifest
```
(2) In discover_subject_inputs, insert this block after the Phase 3 loop (which ends with `result[ses][task][run][role] = value`) and before the `# Log summary` comment:
```python
    # Phase 4: producer completion gate. The producer writes _complete.json
    # into a session directory after every other file of that session; a
    # session without it is still being written and is skipped. Session
    # directories are identified from the listing itself (S3 key or local
    # path), so no path template is needed here.
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

        # Every file the manifest lists for configured-task runs, and the
        # CSF/WM masks it lists as session files, must be in the listing.
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
```
(3) Update the discover_subject_inputs docstring. Add a paragraph: "A session is returned only when its directory holds a valid producer completion manifest (_complete.json, schema_version 1, kind func-session) whose listed configured-task run files and CSF/WM masks are all present. A session without a manifest is skipped at INFO; an invalid or inconsistent manifest raises OrchestratorError." In the Returns block, add `"_manifest": {"written_at": str, "schema_version": int}` as a per-session key, and note that keys starting with "_" are metadata, not tasks.</spec>
      <dependencies>C3, C4 (COMPLETE)</dependencies>
      <risk>medium: adds one small GET per eligible session in S3 mode; it changes which sessions discovery returns, so every discovery fixture must carry a manifest (C12, C13)</risk>
      <rollback>Remove the Phase 4 block, the helper and the docstring additions</rollback>
    </change>

    <change id="C6" priority="P0" source_item="H7 item (4): manifest-aware consumers and QC provenance">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Make the two functions that iterate every session key skip "_" metadata keys, and write the manifest metadata into the session QC JSON.</description>
      <spec>In orchestrator_utils.py:
(1) check_subject_input_completeness: change `configured_tasks = [k for k in ses_data if k != "anat"]` to `configured_tasks = [k for k in ses_data if k != "anat" and not k.startswith("_")]`.
(2) download_session_inputs: as the first statement inside `for task_label, runs in local.items():`, add
```python
        if task_label.startswith("_"):
            continue  # metadata (e.g. _manifest), carried through by deepcopy
```
In its docstring, change the session_inputs description to "One session's dict from discover_subject_inputs (task dicts, anat, and the _manifest metadata, which is passed through unchanged)."
(3) consolidate_session_qc: add the keyword parameter `producer_manifest=None` after `logger` in the signature. Add `"producer_manifest": producer_manifest,` as the last entry of the "provenance" dict. Add a docstring parameter entry: "producer_manifest : dict or None. The session's producer completion manifest metadata ({written_at, schema_version}) from discovery; written to provenance.producer_manifest."

In orchestrate_first_level.py, at the consolidate_session_qc call in _process_session (the one that passes `qc_json_path, logger`), add `producer_manifest=local_session.get("_manifest"),` after `logger`.</spec>
      <dependencies>C5</dependencies>
      <risk>low: additive; the default None keeps every existing direct caller valid</risk>
      <rollback>Remove the guards, the parameter and the call-site argument</rollback>
    </change>

    <change id="C7" priority="P0" source_item="H7 item (3): config path templates">
      <file path="orch_config_production.yaml" action="modify" />
      <description>Set both prefix templates to the real layout and remove the placeholder wording.</description>
      <spec>(1) Replace the header comment lines
```
# s3.func_prefix_template / s3.anat_prefix_template are placeholder path
# templates filled after the producer rerun with the correct S3 layout.
# Both must contain {subject} and {session} placeholders.
```
with
```
# s3.func_prefix_template / s3.anat_prefix_template: S3 path templates for
# the producer per-file outputs. Both are the same prefix because run files
# and session tissue masks share one directory per session
# (derivatives/func/sub-<id>/ses-<NN>A/). Both must contain {subject} and
# {session} placeholders.
```
(2) Replace
```
  # Placeholder S3 path templates (filled after the producer rerun).
  # Both must contain {subject} and {session} placeholders.
  func_prefix_template: "derivatives/sub-{subject}/ses-{session}/func"
  anat_prefix_template: "derivatives/sub-{subject}/ses-{session}/anat"
```
with
```
  # S3 path templates (verified against the <bucket> layout, 2026-10-26).
  func_prefix_template: "derivatives/func/sub-{subject}/ses-{session}A"
  anat_prefix_template: "derivatives/func/sub-{subject}/ses-{session}A"
```
The bucket stays "__PRODUCER_BUCKET__" (locked H7 item 3).</spec>
      <dependencies>none</dependencies>
      <risk>low: config only; passes load_orchestrator_config validation (no leading or trailing slash, both placeholders present)</risk>
      <rollback>Restore the two comment blocks and template values</rollback>
    </change>

    <change id="C8" priority="P0" source_item="H3: always drop straddling events">
      <file path="orchestrator_utils.py" action="modify" />
      <description>format_task_timing drops every event whose shifted onset is negative, counts the straddling subset, and returns three values.</description>
      <spec>(1) Replace the docstring's first two paragraphs (from "Convert BIDS events.tsv to first-level timing TSV with onset shift, drop" through "(the portion that falls within the retained scan).") with:
```
    Convert BIDS events.tsv to first-level timing TSV with the onset shift
    and drop for NSS-removed frames.

    Adjusts event onsets for removed non-steady-state TRs
    (``adjusted_onset = original_onset - n_remove * TR``) and drops every
    event whose shifted onset is < 0: events that ended before the retained
    scan and events straddling the cutoff. Straddling events are dropped,
    not clipped, because a clipped event would be modeled late and would
    shorten its condition's pooled mean duration. The straddling subset is
    counted separately and named in the log.
```
(2) Replace the Returns block with:
```
    tuple of (str, int, int)
        (path, n_dropped, n_dropped_straddling). n_dropped counts every
        dropped event; n_dropped_straddling is the subset whose shifted end
        was > 0. Cache hit returns (path, 0, 0).
```
(3) Change the cache-hit `return out_path, 0, 0, 0.0` to `return out_path, 0, 0`.
(4) Replace everything from `    # Drop events where shifted end <= 0 (fully elapsed before the retained scan)` through the end of the `if n_clipped > 0:` logging block with:
```python
    # Drop every event whose shifted onset is < 0 (elapsed or straddling)
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
```
(5) Change the final `return out_path, n_dropped, n_clipped, clipped_seconds` to `return out_path, n_dropped, n_dropped_straddling`.</spec>
      <dependencies>none</dependencies>
      <risk>low: simplifies the function; every caller is updated in C9 and C17</risk>
      <rollback>Restore the clip branch and the four-element return</rollback>
    </change>

    <change id="C9" priority="P0" source_item="H3: call site, QC fields, session flag">
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="orchestrator_utils.py" action="modify" />
      <description>Update the timing call site and per-run QC fields, and remove the events_clipped_at_start flag.</description>
      <spec>In orchestrate_first_level.py _process_session: (1) change `timing_path, n_dropped, n_clipped, clipped_seconds = format_task_timing(` to `timing_path, n_dropped, n_dropped_straddling = format_task_timing(`. (2) Replace the two lines `rqc["n_events_clipped_at_start"] = n_clipped` and `rqc["clipped_seconds"] = clipped_seconds` with the one line `rqc["n_events_dropped_straddling"] = n_dropped_straddling`.

In orchestrator_utils.py consolidate_session_qc, delete the block that starts `    clipped_runs = [` and ends with the `flags.append` for code "events_clipped_at_start" (9 lines).</spec>
      <dependencies>C8</dependencies>
      <risk>low</risk>
      <rollback>Restore the unpacking, the two QC fields and the flag block</rollback>
    </change>

    <change id="C10" priority="P0" source_item="H2: producer-FD consistency guard">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Replace the rotation-unit heuristic in extract_motion_regressors with a check of Power FD recomputed against the producer's FD.</description>
      <spec>(1) Add these constants and the helper immediately above `def extract_motion_regressors`:
```python
_FD_HEAD_RADIUS_MM = 50.0
_FD_CONSISTENCY_ATOL_MM = 1e-6


def _check_producer_fd_consistency(motion_df, confounds_path, logger):
    """
    Check the degree interpretation of rotations against the producer's FD.

    Recomputes Power framewise displacement from the six base motion
    parameters (backward difference over frames 1..T-1; rotations taken as
    degrees and converted to arc length on a 50 mm sphere) and compares it
    with the producer's framewise_displacement column using
    numpy.allclose(atol=1e-6 mm, rtol=0). A degree/radian mismatch changes
    FD by about 0.01 to 1 mm, far above the tolerance. Frames where the
    recomputed or the producer value is non-finite are not compared
    (non-finite motion is handled after the NSS trim).

    Returns
    -------
    str
        "producer_fd_consistent", or "not_checkable" when the producer FD
        column is absent, a required motion column is absent, or no frame
        is comparable.

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
```
(2) In extract_motion_regressors, change the cache-hit branch to:
```python
    if os.path.isfile(out_path) and not force_recompute:
        logger.info("Motion regressors already exist: %s", out_path)
        motion_df = pd.read_csv(confounds_path, sep="\t")
        return out_path, _check_producer_fd_consistency(motion_df, confounds_path, logger)
```
(3) Replace the whole block from `    # Rotation unit validation: exploit physical constraints of MRI head coils.` through `        rotation_unit_ambiguous = True` with:
```python
    # Producer-FD consistency guard (before the NSS trim)
    motion_units_check = _check_producer_fd_consistency(motion_df, confounds_path, logger)
```
(4) Change the final `return out_path, rotation_unit_ambiguous` to `return out_path, motion_units_check`.
(5) Docstring: in the first paragraph, replace "Output units: translations in mm, rotations in degrees (per the fmri-first-level-proc input contract). No unit conversion is applied;" with "Output units: translations in mm, rotations in degrees (as written by the producer and required by the fmri-first-level-proc input contract), confirmed per run by the producer-FD consistency check. No unit conversion is applied;". Replace the Returns block with:
```
    tuple[str, str]
        (out_path, motion_units_check): motion_units_check is
        "producer_fd_consistent" or "not_checkable" (see
        _check_producer_fd_consistency); it is also evaluated on a cache hit.
```
and add the line "OrchestratorError with context reason 'producer_fd_inconsistent' is raised (before any output is written) when the producer FD disagrees." after the paragraph about non-finite values.</spec>
      <dependencies>C1 (COMPLETE; math import)</dependencies>
      <risk>medium: the second return value changes from bool to str, and every confounds fixture must now carry a consistent FD (C17)</risk>
      <rollback>Restore the max-abs-rotation block and the bool return</rollback>
    </change>

    <change id="C11" priority="P0" source_item="H2: call site, QC field, exit classification">
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="orchestrator_utils.py" action="modify" />
      <description>Record motion_units_check per run, and classify the producer_fd_inconsistent exclusion as unexplained.</description>
      <spec>In orchestrate_first_level.py _process_session: (1) change `motion_path, rot_unit_ambiguous = extract_motion_regressors(` to `motion_path, motion_units_check = extract_motion_regressors(` and change the following line to `preproc_qc_by_run.setdefault(run_label, {})["motion_units_check"] = motion_units_check`. (2) In the per-run `except OrchestratorError as e:` handler, directly after `reason = (e.context or {}).get("reason", "unexpected_error")`, add:
```python
                    if reason == "producer_fd_inconsistent":
                        preproc_qc_by_run.setdefault(run_label, {})["motion_units_check"] = reason
```
In orchestrator_utils.py: (3) directly after the EXIT_FAILED_KNOWN_CAUSE constant and its docstring, add:
```python

_UNEXPLAINED_EXCLUSION_REASONS = frozenset({"unexpected_error", "producer_fd_inconsistent"})
"""Run-exclusion reasons that make a session EXIT_FAILED_UNEXPLAINED."""
```
(4) In classify_session_exit, replace
```python
    # Any unexpected_error exclusion
    if any(e.get("reason") == "unexpected_error" for e in preproc_excluded_runs):
```
with
```python
    # Any exclusion whose cause is unexplained
    if any(e.get("reason") in _UNEXPLAINED_EXCLUSION_REASONS for e in preproc_excluded_runs):
```
(5) In the classify_session_exit docstring and the EXIT_FAILED_UNEXPLAINED constant docstring, replace "with reason 'unexpected_error'" with "with reason 'unexpected_error' or 'producer_fd_inconsistent'".</spec>
      <dependencies>C10</dependencies>
      <risk>low</risk>
      <rollback>Restore the variable name, the single-reason check and the docstrings</rollback>
    </change>

    <change id="C12" priority="P0" source_item="G14: three-way exit reporting in the batch runner">
      <file path="run_orchestrator.py" action="modify" />
      <description>Map orchestrator exit codes 0, 1 and 2 to distinct per-subject statuses, counters, progress and summary lines, and a batch exit code.</description>
      <spec>(1) Header comment "Exit codes:" block becomes:
```
# Exit codes:
#   0:   all subjects completed
#   1:   one or more subjects failed with an unexplained cause
#        (orchestrator exit 1, any other nonzero exit code, or a crash)
#   2:   one or more subjects failed with a known cause (orchestrator
#        exit 2) and none failed unexplained
#   130: interrupted by Ctrl+C
```
(2) _run_one docstring: status is one of "success", "failed_unexplained", "failed_known_cause", "cancelled".
(3) In _run_one, replace the `else:` branch after `if result.returncode == 0:` with:
```python
        else:
            # Extract last non-empty stderr line as the error summary
            stderr_lines = [ln for ln in result.stderr.splitlines() if ln.strip()]
            if stderr_lines:
                err_msg = stderr_lines[-1]
            else:
                stdout_lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
                err_msg = stdout_lines[-1] if stdout_lines else f"Exit code {result.returncode}"
            if result.returncode == 2:
                # Terminal known-cause failure; rerunning cannot change it
                with lock:
                    state["n_failed_known"] += 1
                return "failed_known_cause", runtime, err_msg, log_file
            with lock:
                state["n_failed_unexplained"] += 1
            return "failed_unexplained", runtime, err_msg, log_file
```
and in its `except Exception as e:` branch change `state["n_failed"] += 1` to `state["n_failed_unexplained"] += 1` and the returned status "failed" to "failed_unexplained".
(4) _progress_loop: replace `nf = state["n_failed"]` with `nfu = state["n_failed_unexplained"]` and `nfk = state["n_failed_known"]`; `n_done = ns + nfu + nfk + nsk`; the status text becomes `f"Done: {ns} ok, {nfu} failed, {nfk} known-cause, {nsk} skipped"`.
(5) State init: `state = {"running": set(), "n_success": 0, "n_failed_unexplained": 0, "n_failed_known": 0, "n_skipped": 0}`.
(6) In the main as_completed loop, the `except Exception as e:` around `future.result()` sets `status = "failed_unexplained"` and increments `state["n_failed_unexplained"]`. Replace `if status == "failed":\n                    exit_code = 1` with:
```python
                if status == "failed_unexplained":
                    exit_code = 1
                elif status == "failed_known_cause" and exit_code == 0:
                    exit_code = 2
```
(7) In the KeyboardInterrupt collection loop, the `except Exception as e:` sets `status = "failed_unexplained"`.
(8) Final summary: replace the `n_failed` count with `n_failed_unexplained = sum(1 for r in summary_rows if r["status"] == "failed_unexplained")` and `n_failed_known = sum(1 for r in summary_rows if r["status"] == "failed_known_cause")`; the print becomes `f"\nBatch complete: {n_success} success, {n_failed_unexplained} failed (unexplained), {n_failed_known} failed (known cause), {n_cancelled} cancelled"`.
(9) After the edits, `grep -n 'n_failed"\|"failed"' run_orchestrator.py` must return no matches.</spec>
      <dependencies>none</dependencies>
      <risk>low: utility script; no existing tests (exit-mapping tests go to /test design)</risk>
      <rollback>Restore the two-way status handling</rollback>
    </change>

    <change id="C13" priority="P0" source_item="G10+G11: mock layout matches the real v7 layout">
      <file path="tests/mock_data_utils.py" action="modify" />
      <description>Rewrite the mock session filesystem to the real single-directory v7 layout, add the producer extras and manifest, and make the mock FD consistent with the mock motion.</description>
      <spec>(1) Add a module-level helper after create_mock_mask_nifti:
```python
def power_fd(motion_df, radius_mm=50.0):
    """Power FD from the six base motion columns (rotations in degrees);
    NaN at frame 0, matching the producer's framewise_displacement."""
    cols = ["trans_x", "trans_y", "trans_z", "rot_x", "rot_y", "rot_z"]
    base = motion_df[cols].to_numpy(dtype=float)
    deltas = np.abs(np.diff(base, axis=0))
    deltas[:, 3:] *= math.pi / 180.0 * radius_mm
    return np.concatenate([[np.nan], deltas.sum(axis=1)])
```
(2) create_mock_confounds_tsv: generate the six motion columns first (same RNG draws and order as now), then set `framewise_displacement = power_fd(motion)` and write the FD column first, so the column order stays the same. Update the docstring: the FD column is computed from the mock motion with the Power formula, exactly as the producer does.
(3) create_mock_session_filesystem: add the keyword `write_manifest=True`. Use `ses_label = f"ses-{session}A"` and a single `session_dir = os.path.join(base_dir, "derivatives", "func", f"sub-{sub_id}", ses_label)`, with no func/ or anat/ subdirectory. Per run, write into session_dir: `{prefix}_space-{space}_bold.nii.gz`, `{prefix}_space-{space}_brainmask.nii.gz`, `{prefix}_desc-confounds_timeseries.tsv`, `{prefix}_events.tsv` (nback only), and `{prefix}_desc-boldref.nii.gz` (zero-byte stub). Per session: `sub-{sub_id}_{ses_label}_space-{space}_res-1_label-CSF_mask.nii.gz` and `..._res-1_label-WM_mask.nii.gz` (real masks, as now), plus zero-byte stubs `..._res-1_dseg.nii.gz`, `..._res-1_dseg.tsv`, `..._res-1_label-GM_mask.nii.gz`, `..._res-1_label-CSF_probseg.nii.gz`, `..._res-1_label-GM_probseg.nii.gz`, `..._res-1_label-WM_probseg.nii.gz`. When write_manifest is True, write `_complete.json` in session_dir: `{"file_count": N, "kind": "func-session", "runs": {"task-<task>_run-<NN>": sorted basenames of that run's files, ...}, "schema_version": 1, "session_files": sorted basenames of the 8 session files, "source_step": "func-preproc", "written_at": "2026-10-26T00:00:00+00:00"}`, where N is the number of files written excluding the manifest. Run keys use the two-digit run number, as the producer does.
(4) Return dict: remove "sub_dir", "func_dir" and "anat_dir"; add "session_dir", "manifest_path" (None when write_manifest is False) and "extra_files" (list of the stub paths). Keep every other key unchanged.
(5) Update the module docstring and the function docstring's directory tree to the layout above.</spec>
      <dependencies>C3, C4, C5</dependencies>
      <risk>medium: every pipeline-level test consumes this fixture; a repository-wide grep found no test that reads sub_dir, func_dir or anat_dir</risk>
      <rollback>Restore tests/mock_data_utils.py from the sandbox snapshot</rollback>
    </change>

    <change id="C14" priority="P0" source_item="G12 + H7 item (5): discovery, gate and config tests">
      <file path="tests/test_discovery_v4.py" action="modify" />
      <file path="tests/test_production_configs.py" action="modify" />
      <description>Move the discovery tests to the real layout, give every S3 listing a manifest, and add the inventory, no-res and completion-gate tests.</description>
      <spec>In tests/test_discovery_v4.py:
(1) The key helpers write `ses-{ses}A` in basenames, take a single prefix, and use bare names: `_bold_key` gives `..._space-{space}_bold.nii.gz`; `_mask_key` gives `..._space-{space}_brainmask.nii.gz`; `_csf_key` and `_wm_key` take `res="1"` and insert `_res-{res}` before `_label-` unless res is None. Add `_boldref_key`, and `_manifest_key(prefix)` returning `f"{prefix}_complete.json"`.
(2) Replace FUNC_PREFIX and ANAT_PREFIX with `SES_PREFIX = "derivatives/func/sub-TEST001/ses-00A/"`, and the two ses-02 prefixes with `"derivatives/func/sub-TEST001/ses-02A/"`. _mock_s3_config uses `"derivatives/func/sub-{subject}/ses-{session}A"` for both templates.
(3) Add `_manifest_for(keys)`, which builds a valid manifest from a session's data keys (runs grouped by the task-X_run-NN token in each basename, CSF/WM keys in session_files), and `_install_listing(mock_get_client, keys, manifests)`, which configures the paginator pages and a get_object side_effect returning `{"Body": io.BytesIO(json.dumps(manifests[Key]).encode())}`.
(4) Every existing S3 test adds the manifest key for each session it lists and registers that session's manifest. Assertions that compare a session dict's full key set include "_manifest". test_no_keys_at_all_produces_empty_discovery is unchanged.
(5) New class TestRealProducerLayout: test_34_file_inventory lists the 34 basenames of a real new-layout session with a synthetic subject ID (sub-TEST001, ses-00A: nback runs 01 and 02 with boldref, confounds, events, bold and brainmask; rest runs 01 to 04 with boldref, confounds, bold and brainmask; the 8 res-1 session files; _complete.json, using the producer manifest structure). It asserts 2 nback runs and 4 rest runs, each with every role set; events None for rest; both masks set; _manifest equal to {"written_at": ..., "schema_version": 1}; and no WARNING-level log records. test_masks_without_res_still_match passes res=None for both masks and asserts both are discovered. test_legacy_tarball_only_session_absent lists only `*_bold.tar.gz` keys for ses-02A beside a complete ses-00A, and asserts only "00" is returned.
(6) New class TestCompletionGate (S3 mode unless stated): manifest absent → session absent, INFO log contains "no producer completion manifest", and another session with a manifest is still returned; schema_version 2 → OrchestratorError; kind "anat-session" → OrchestratorError; a manifest-listed boldref of a configured-task run absent from the listing → OrchestratorError naming it; a manifest-listed CSF mask absent → OrchestratorError; a manifest-listed run file of an unconfigured task absent → no error; metadata stored as `discovered["00"]["_manifest"] == {"written_at": "2026-10-26T00:00:00+00:00", "schema_version": 1}`; local mode with create_mock_session_filesystem(write_manifest=False) → {}; local mode with write_manifest=True → the session with _manifest set.
(7) Update the module docstring's fixture and layout description.

In tests/test_production_configs.py: rename test_s3_enabled_with_placeholder_templates to test_s3_enabled_with_v7_templates, and add assertions that both templates equal "derivatives/func/sub-{subject}/ses-{session}A" and that the bucket equals "__PRODUCER_BUCKET__".</spec>
      <dependencies>C5, C7, C13</dependencies>
      <risk>medium: substantial test rework</risk>
      <rollback>Restore both files from the sandbox snapshot</rollback>
    </change>

    <change id="C15" priority="P0" source_item="G8 + G12: dead-code tests and old layout references">
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <file path="tests/test_coverage_gaps_v2.py" action="modify" />
      <file path="tests/test_exit_classification.py" action="modify" />
      <file path="tests/test_preprocessing.py" action="modify" />
      <description>Remove tests of the deleted functions, register them as retired symbols, and move the remaining hard-coded templates to the v7 form.</description>
      <spec>(1) tests/test_coverage_gaps.py: remove `decompress_if_needed` and `remove_initial_trs_tabular` from the import block; delete class TestDecompressIfNeeded and class TestRemoveInitialTrsTabular (located by class name) with their section banners; add `"decompress_if_needed",  # tarball-era decompression removed` and `"remove_initial_trs_tabular",  # unused tabular trim removed` to the TestRetiredSymbolsRemoved parameter list; remove the two corresponding bullets from the module docstring.
(2) tests/test_coverage_gaps_v2.py: delete class TestRemoveInitialTrsTabular and class TestDecompressIfNeeded with their section banners, and remove their module-docstring bullets. In the two template-validation tests (located by `test_s3_func_prefix_template_trailing_slash_raises` and the test after it), replace the template strings with the v7 form ("derivatives/func/sub-{subject}/ses-{session}A", with the trailing "/" kept in the trailing-slash case).
(3) tests/test_exit_classification.py and tests/test_preprocessing.py: replace every `"derivatives/sub-{subject}/ses-{session}/func"` and `"derivatives/sub-{subject}/ses-{session}/anat"` with `"derivatives/func/sub-{subject}/ses-{session}A"`.
(4) After the edits, `grep -rn "ses-{session}/\|desc-preproc_bold\|desc-brain_mask\|\"func_dir\"\|\"anat_dir\"" tests/` must return no matches.</spec>
      <dependencies>C1 (COMPLETE), C13</dependencies>
      <risk>low</risk>
      <rollback>Restore the files from the sandbox snapshot</rollback>
    </change>

    <change id="C16" priority="P0" source_item="G3 follow-up: mock 3dBrickStat fidelity">
      <file path="tests/mock_subprocess.py" action="modify" />
      <file path="tests/test_tissue_mask_regressors.py" action="modify" />
      <description>Make the mock `3dBrickStat -count -non-zero` count the real non-zero voxels, and correct the test-module docstring.</description>
      <spec>(1) In _handle_3dBrickStat, replace `return MockCompletedProcess(cmd, stdout="80")` in the `-count`/`-non-zero` branch with:
```python
        target = re.sub(r"\[.*\]$", "", cmd[-1])
        try:
            n = int(np.count_nonzero(np.asarray(nib.load(target).dataobj)))
        except Exception:
            n = 80
        return MockCompletedProcess(cmd, stdout=f"{n}  \n")
```
(nib, np and re are already imported in mock_subprocess.py; confirm before editing.)
(2) tests/test_tissue_mask_regressors.py module docstring: replace "counted directly from the intersected mask NIfTI (not via a 3dBrickStat subprocess round-trip, so the zero-voxel path is deterministically testable)" with "counted from the intersected mask with 3dBrickStat -count -non-zero (the mock counts the intersected NIfTI's non-zero voxels, so the zero-voxel path stays deterministically testable)".</spec>
      <dependencies>C2 (COMPLETE)</dependencies>
      <risk>low</risk>
      <rollback>Restore the constant return and the docstring</rollback>
    </change>

    <change id="C17" priority="P0" source_item="H2 + H3: fixtures and tests of removed behavior">
      <file path="tests/conftest.py" action="modify" />
      <file path="tests/test_cr_implementation.py" action="modify" />
      <file path="tests/test_cr_implementation_extended.py" action="modify" />
      <file path="tests/test_motion_non_finite.py" action="modify" />
      <file path="tests/test_motion_and_qc.py" action="modify" />
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <file path="tests/test_coverage_gaps_v2.py" action="modify" />
      <file path="tests/test_simulated_pipeline.py" action="modify" />
      <file path="tests/test_qc_schema_v4.py" action="modify" />
      <file path="tests/test_events_validation.py" action="modify" />
      <file path="tests/test_exit_classification.py" action="modify" />
      <description>Give every confounds fixture that reaches extract_motion_regressors an FD consistent with its motion, and rewrite the tests of the removed heuristic, clipping and flag to the new contract.</description>
      <spec>FD-consistent fixtures (use power_fd from tests.mock_data_utils; set the FD after all motion values are drawn, and before any deliberate NaN or inf injection):
(1) conftest.py sample_confounds_df: `fd = power_fd(df)` replaces the uniform draw (draw the motion first, then assign the framewise_displacement column, keeping it first in column order).
(2) test_motion_non_finite.py `_make_confounds`: FD from power_fd on the finite motion, computed before the NaN or inf is injected.
(3) test_motion_and_qc.py test_rotations_remain_in_degrees: FD = power_fd of that frame's motion. The compute_preproc_qc FD tests (producer_fd_* fields) are not routed through extract_motion_regressors and are unchanged.
(4) test_coverage_gaps_v2.py TestExtractMotionRegressorsEdgeCases._make_confounds: FD = power_fd.
(5) test_cr_implementation.py test_extract_motion_force_recompute fixture: FD = power_fd.

Removed rotation-unit heuristic:
(6) test_cr_implementation.py: replace class TestRotationUnitCheck with class TestProducerFdConsistencyGuard: consistent FD → second return value "producer_fd_consistent"; one frame of FD perturbed by 1e-3 mm → OrchestratorError, context reason "producer_fd_inconsistent", no output file; FD computed with rotations treated as radians → producer_fd_inconsistent; a difference of 1e-7 mm → consistent; FD column absent → "not_checkable"; FD column all NaN → "not_checkable"; NaN in one motion frame inside the NSS-trimmed region with n_remove covering it → "producer_fd_consistent"; a cache hit on a consistent confounds file → "producer_fd_consistent".
(7) test_cr_implementation_extended.py: delete class TestRotationUnitCheckExtended (its three cases exercise only the removed heuristic; the guard is covered by the class in item 6).

Always-drop timing (three-value return):
(8) test_simulated_pipeline.py test_timing_onset_adjustment_drops_only_fully_elapsed_events: rename to test_timing_onset_adjustment_drops_all_negative_onsets; unpack `timing_path, n_dropped, n_dropped_straddling`; assert n_dropped == 2, n_dropped_straddling == 1, len(timing_df) == 3, all onsets >= 0, and the retained onsets equal the original onsets 2.0, 4.0 and 6.0 minus 0.8; update the docstring and comments (cond_a at 0.0 straddles and is dropped).
(9) test_coverage_gaps.py, the format_task_timing section: unpack three values everywhere. Rename test_straddling_event_is_clipped_not_dropped to test_straddling_event_is_dropped and assert n_dropped == 1, n_dropped_straddling == 1, and that the straddling trial_type is absent from the output. The fully-elapsed test asserts n_dropped == 1 and n_dropped_straddling == 0. Update the section banner and module docstring wording from "drop-vs-clip" to "always drop".
(10) test_coverage_gaps_v2.py TestFormatTaskTiming: unpack three values; the short-events test asserts n_dropped_straddling == 0 in place of n_clipped; the cache-hit test asserts (n_dropped, n_dropped_straddling) == (0, 0); update the banner and docstring wording.
(11) test_events_validation.py module docstring: "the C6 onset shift/clip" becomes "the onset shift and drop".

QC schema:
(12) test_qc_schema_v4.py: in test_task_run_preproc_fields_passthrough, replace the two clipped fields with "n_events_dropped_straddling": 1 and assert it passes through. Replace test_events_clipped_at_start_flag_triggered with test_straddling_drop_raises_no_flag, asserting a run with n_events_dropped_straddling 2 produces no "events_clipped_at_start" code. In test_multiple_flags_all_present_and_status_unaffected, drop the preproc clipped entry and the "events_clipped_at_start" code from the expected set. Add test_producer_manifest_written_to_provenance (provenance.producer_manifest equals the dict passed) and test_producer_manifest_defaults_to_null. Update the module docstring (remove events_clipped_at_start, n_events_clipped_at_start and clipped_seconds; add n_events_dropped_straddling, motion_units_check and provenance.producer_manifest).

Exit classification:
(13) test_exit_classification.py, the classify_session_exit unit class: add test_producer_fd_inconsistent_exclusion_is_unexplained (an exclusion with reason "producer_fd_inconsistent", all outcomes successful and terminal → EXIT_FAILED_UNEXPLAINED).

After the edits, `grep -rn "rotation_unit_ambiguous\|rot_ambiguous\|n_clipped\|clipped_seconds\|n_events_clipped_at_start" tests/` must return no matches.</spec>
      <dependencies>C8, C9, C10, C11, C13</dependencies>
      <risk>medium: touches many files; each edit is local to a named fixture or test</risk>
      <rollback>Restore the files from the sandbox snapshot</rollback>
    </change>

    <change id="C18" priority="P0" source_item="H8: tie-aware tissue-mask integration test">
      <file path="tests/test_tissue_mask_regressors.py" action="modify" />
      <description>Add the producer-geometry integration test against real AFNI.</description>
      <spec>Add class TestProducerGeometryIntegration after TestComputeTissueMaskRegressorRealAFNI, decorated with the same `@pytest.mark.skipif(not _afni_available(), reason="AFNI not installed in this environment")` (the repository registers no "afni" marker). Build the images with nibabel (a test-only dependency per C2), with sform and qform both set to the stated affine and code:
- BOLD: shape (97, 115, 97, 12), float32, affine [[-2,0,0,95.5],[0,-2,0,95.5],[0,0,2,-78.5],[0,0,0,1]], code 4, pixdim[4] = 0.8. Voxels whose world x > 0 carry 1000 + 10*sin(t); world x < 0 carry 2000 + 10*cos(t), for t = 0..11.
- Brainmask: shape (97, 115, 97), all ones, the same affine and code.
- Tissue mask: shape (193, 229, 193), int16, affine [[1,0,0,-96],[0,1,0,-132],[0,0,1,-78],[0,0,0,1]], code 1.
Case test_tie_invariant_mask: ones at 1 mm voxel centers with world coordinates 19 to 40 mm inclusive on all three axes. compute_tissue_mask_regressor must return n_voxels == 1331, and the regressor must equal 1000 + 10*sin(t) within atol 0.01.
Case test_tie_diagnostic_mask: x from 20 to 40 mm, y and z from 19 to 40 mm. Assert `n_voxels in {1210, 1331}`, and record the observed count with `request.node.user_properties.append(("tie_count", n_voxels))` (take the `request` fixture).</spec>
      <dependencies>C2 (COMPLETE)</dependencies>
      <risk>low: new tests only; skipped without AFNI</risk>
      <rollback>Remove the class</rollback>
    </change>

  </changes>
  <execution_order>C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16, C17, C18</execution_order>
</implement_plan>
