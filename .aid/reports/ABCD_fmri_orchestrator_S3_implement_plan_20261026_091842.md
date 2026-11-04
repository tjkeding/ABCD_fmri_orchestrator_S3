<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-10-26T13:18:42Z" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_brainstorm_20261022_174139.md" mode="brainstorm" key_items="8" />
    <report path="(inline user spec)" mode="orchestrator audit" key_items="8" />
  </input_reports>
  <changes>

    <change id="C1" priority="P0" source_item="G8: remove tarball-era dead code">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Remove decompress_if_needed (lines 979-1060), remove_initial_trs_tabular (lines 1189-1230), and their dead imports: bz2 (line 20), gzip (line 23), glob as globmod (line 22), tarfile (line 28).</description>
      <spec>Delete the function body and docstring of decompress_if_needed (lines 979-1060) and the Section D comment block above it (lines 975-977). Delete remove_initial_trs_tabular (lines 1189-1230). Remove the four import lines: `import bz2` (line 20), `import glob as globmod` (line 22), `import gzip` (line 23), `import tarfile` (line 28). No call sites exist in production code.</spec>
      <dependencies>none</dependencies>
      <risk>low: no production callers; only test references (addressed in C14)</risk>
      <rollback>Restore the two functions and four imports from git</rollback>
    </change>

    <change id="C2" priority="P0" source_item="G3: remove nibabel dependency">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="environment.yaml" action="modify" />
      <description>Replace the nibabel voxel count in compute_tissue_mask_regressor with 3dBrickStat. Remove the nibabel import and the environment.yaml dependency.</description>
      <spec>In orchestrator_utils.py: (1) Remove `import nibabel as nib` (line 37). (2) In compute_tissue_mask_regressor, replace lines 1510-1512:
```python
        # Step 3: Count non-zero voxels via nibabel
        intersect_data = np.asarray(nib.load(intersect_path).dataobj)
        n_voxels = int(np.count_nonzero(intersect_data))
```
with:
```python
        # Step 3: Count non-zero voxels via AFNI 3dBrickStat
        count_result = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", intersect_path],
            capture_output=True, text=True, check=True,
        )
        n_voxels = int(float(count_result.stdout.strip()))
```
In environment.yaml: change line 23 from `  - nibabel        # NIfTI I/O for tissue mask voxel counting` to `  - nibabel        # NIfTI I/O for test fixtures (not used in production code)`.</spec>
      <dependencies>none</dependencies>
      <risk>low: 3dBrickStat is already used elsewhere in the codebase (compute_preproc_qc line 2093); the AFNI dependency is already required</risk>
      <rollback>Restore nibabel import, revert the three lines, restore the environment.yaml line</rollback>
    </change>

    <change id="C3" priority="P0" source_item="G7a+G7b: tighten discovery patterns and session regex">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Require the A suffix in the session regex, tighten BOLD/brainmask patterns to the real producer naming, and update the comment block.</description>
      <spec>Replace lines 604-641 with:
```python
# Input filename patterns: single edit point for the producer per-file
# naming convention (verified against <bucket> S3 layout 2026-10-26).
# Session directories always end with "A" (ses-00A, ses-02A, etc.).
# BOLD is bare _bold.nii.gz; brainmask is bare _brainmask.nii.gz.
_SES = r"ses-(?P<ses>[0-9]+)A"
_RUN_ENTITIES = (
    r"^sub-(?P<sub>[A-Za-z0-9]+)_"
    + _SES
    + r"_task-(?P<task>[A-Za-z0-9]+)_run-(?P<run>[0-9]+)"
)

INPUT_FILE_PATTERNS = OrderedDict([
    ("bold", re.compile(
        _RUN_ENTITIES
        + r"_space-(?P<space>[A-Za-z0-9]+)_bold\.nii\.gz$"
    )),
    ("brainmask", re.compile(
        _RUN_ENTITIES
        + r"_space-(?P<space>[A-Za-z0-9]+)_brainmask\.nii\.gz$"
    )),
    ("confounds", re.compile(
        _RUN_ENTITIES + r"_desc-confounds_timeseries\.tsv$"
    )),
    ("events", re.compile(
        _RUN_ENTITIES + r"_events\.tsv$"
    )),
    ("csf_mask", re.compile(
        r"^sub-(?P<sub>[A-Za-z0-9]+)_"
        + _SES
        + r"_space-(?P<space>[A-Za-z0-9]+)(?:_res-(?P<res>[A-Za-z0-9]+))?_label-CSF_mask\.nii\.gz$"
    )),
    ("wm_mask", re.compile(
        r"^sub-(?P<sub>[A-Za-z0-9]+)_"
        + _SES
        + r"_space-(?P<space>[A-Za-z0-9]+)(?:_res-(?P<res>[A-Za-z0-9]+))?_label-WM_mask\.nii\.gz$"
    )),
])
```
This covers H7a (res entity), G7a (require A), and G7b (tighten bold/brainmask).</spec>
      <dependencies>none</dependencies>
      <risk>low: patterns verified against all 491,069 real S3 keys</risk>
      <rollback>Restore original lines 604-641</rollback>
    </change>

    <change id="C4" priority="P0" source_item="H7d: known non-input file ignore list">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Add _KNOWN_NON_INPUT_PATTERNS after INPUT_FILE_PATTERNS and replace the blanket WARNING for non-matching files with DEBUG for known producer files.</description>
      <spec>After the INPUT_FILE_PATTERNS block (after the closing `])`) and before `def _parse_input_basename`, add:
```python

# Known producer files that the orchestrator does not consume as inputs.
# Matched files are logged at DEBUG rather than WARNING.
_KNOWN_NON_INPUT_PATTERNS = [
    re.compile(r"_res-[A-Za-z0-9]+_dseg\.nii\.gz$"),
    re.compile(r"_res-[A-Za-z0-9]+_dseg\.tsv$"),
    re.compile(r"_label-GM_mask\.nii\.gz$"),
    re.compile(r"_label-[A-Za-z]+-?[A-Za-z]*_probseg\.nii\.gz$"),
    re.compile(r"_desc-boldref\.nii\.gz$"),
    re.compile(r"^_complete\.json$"),
    re.compile(r"_bold\.tar\.gz$"),
]
```
In the candidate-parsing loop (line ~719-720), replace:
```python
        if role is None:
            logger.warning("Non-matching input file, ignoring: %s", value)
            continue
```
with:
```python
        if role is None:
            if any(p.search(basename) for p in _KNOWN_NON_INPUT_PATTERNS):
                logger.debug("Known non-input file, ignoring: %s", value)
            else:
                logger.warning("Non-matching input file, ignoring: %s", value)
            continue
```</spec>
      <dependencies>C3 (patterns must be in final form first)</dependencies>
      <risk>low: logging-only change; no functional impact</risk>
      <rollback>Remove the pattern list and restore the single WARNING line</rollback>
    </change>

    <change id="C5" priority="P0" source_item="H7c: _complete.json producer completion gate">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Add Phase 4 (completion gate) in discover_subject_inputs, after Phase 3 and before the log summary.</description>
      <spec>Insert a new block between the end of Phase 3 (after line ~803: `result[ses][task][run][role] = value`) and before the log summary (line ~805: `# Log summary`). The new Phase 4:

```python
    # Phase 4: Producer completion gate (_complete.json)
    # Sessions without a valid manifest are removed from the result.
    sessions_to_remove = []

    # Build a set of all basenames per session from the raw candidates,
    # so the cross-check can verify manifest-listed files were discovered.
    basenames_by_ses = {}
    for basename, value in candidates:
        if s3_config.get("enabled", False):
            parts = value.rsplit("/", 1)
            if len(parts) == 2:
                ses_match = re.search(r"/ses-(\d+)A(?:/|$)", parts[0])
                if ses_match:
                    basenames_by_ses.setdefault(ses_match.group(1), set()).add(basename)
        else:
            ses_match = re.search(r"/ses-(\d+)A(?:/|$)", value)
            if ses_match:
                basenames_by_ses.setdefault(ses_match.group(1), set()).add(basename)

    for ses in list(result.keys()):
        # Read the manifest
        manifest = None
        if s3_config.get("enabled", False):
            func_template = s3_config["func_prefix_template"]
            ses_prefix = func_template.replace("{subject}", sub_id).replace("{session}", ses)
            manifest_key = f"{ses_prefix}/_complete.json"
            try:
                s3_client = _get_s3_client()
                resp = s3_client.get_object(Bucket=s3_config["bucket"], Key=manifest_key)
                manifest = json.loads(resp["Body"].read())
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("404", "NoSuchKey"):
                    logger.info(
                        "Session ses-%sA: no producer completion manifest, skipping",
                        ses
                    )
                    sessions_to_remove.append(ses)
                    continue
                raise
        else:
            # Local mode: derive session directory from any discovered file
            # in this session (all files share the same parent directory).
            ses_dir = None
            for _task_or_anat, task_data in result[ses].items():
                if _task_or_anat == "anat":
                    for path in task_data.values():
                        if path is not None:
                            ses_dir = os.path.dirname(path)
                            break
                elif _task_or_anat == "_manifest":
                    continue
                else:
                    for _run, files in task_data.items():
                        for path in files.values():
                            if path is not None:
                                ses_dir = os.path.dirname(path)
                                break
                        if ses_dir:
                            break
                if ses_dir:
                    break
            if ses_dir is None:
                sessions_to_remove.append(ses)
                continue
            manifest_path = os.path.join(ses_dir, "_complete.json")
            if not os.path.isfile(manifest_path):
                logger.info(
                    "Session ses-%sA: no producer completion manifest, skipping",
                    ses
                )
                sessions_to_remove.append(ses)
                continue
            with open(manifest_path) as mf:
                manifest = json.load(mf)

        # Validate manifest schema
        if manifest.get("schema_version") != 1:
            raise OrchestratorError(
                f"Session ses-{ses}A: _complete.json has schema_version "
                f"{manifest.get('schema_version')}, expected 1"
            )
        if manifest.get("kind") != "func-session":
            raise OrchestratorError(
                f"Session ses-{ses}A: _complete.json has kind "
                f"'{manifest.get('kind')}', expected 'func-session'"
            )

        # Cross-check: every file the manifest lists for configured tasks
        # and tissue masks must have been discovered
        ses_basenames = basenames_by_ses.get(ses, set())
        missing_files = []
        for run_key, run_files in manifest.get("runs", {}).items():
            # run_key is e.g. "task-nback_run-01"
            task_match = re.match(r"task-(\w+)_run-\d+", run_key)
            if task_match and task_match.group(1) in configured_tasks:
                for fname in run_files:
                    if fname not in ses_basenames:
                        missing_files.append(fname)

        for fname in manifest.get("session_files", []):
            # Check only CSF/WM masks (the files the orchestrator needs)
            if "_label-CSF_mask" in fname or "_label-WM_mask" in fname:
                if fname not in ses_basenames:
                    missing_files.append(fname)

        if missing_files:
            raise OrchestratorError(
                f"Session ses-{ses}A: _complete.json lists file(s) not found "
                f"in S3/local listing: {missing_files}"
            )

        # Store manifest metadata for session QC
        result[ses]["_manifest"] = {
            "written_at": manifest.get("written_at"),
            "schema_version": manifest.get("schema_version"),
        }

    # Remove sessions without a valid manifest
    for ses in sessions_to_remove:
        del result[ses]
```

Note: the `_manifest` key starts with underscore so it cannot collide with task labels (task labels are alphanumeric).</spec>
      <dependencies>C3, C4</dependencies>
      <risk>medium: adds network I/O (one small GET per session); adds a removal loop</risk>
      <rollback>Remove the Phase 4 block</rollback>
    </change>

    <change id="C6" priority="P0" source_item="H7b: config path templates">
      <file path="orch_config_production.yaml" action="modify" />
      <description>Set both prefix templates to the real S3 layout and remove placeholder language from comments.</description>
      <spec>Replace lines 17-20 (the placeholder comment) with:
```yaml
# s3.func_prefix_template / s3.anat_prefix_template: S3 path templates for
# per-file producer outputs. Both point to the same prefix because all
# session files (run data and tissue masks) are under derivatives/func/.
# Both must contain {subject} and {session} placeholders.
```
Replace lines 109-112 with:
```yaml
  # S3 path templates (verified against <bucket> layout 2026-10-26).
  # Both point to derivatives/func/ because masks and run files share one prefix.
  func_prefix_template: "derivatives/func/sub-{subject}/ses-{session}A"
  anat_prefix_template: "derivatives/func/sub-{subject}/ses-{session}A"
```</spec>
      <dependencies>none</dependencies>
      <risk>low: config only; verified against real S3 layout</risk>
      <rollback>Restore the original template values and comments</rollback>
    </change>

    <change id="C7" priority="P0" source_item="H3: always-drop straddling events">
      <file path="orchestrator_utils.py" action="modify" />
      <description>In format_task_timing, remove the clip branch, drop all events with onset &lt; 0, count straddling events separately, change return signature.</description>
      <spec>In format_task_timing: (1) Update docstring lines 1670-1671 to "Convert BIDS events.tsv to first-level timing TSV with onset shift and drop for NSS-removed frames." Remove the clipping description. (2) Change return docstring (lines 1697-1699) to: `tuple of (str, int, int)` with `(path, n_dropped, n_dropped_straddling). Cache hit returns (path, 0, 0).` (3) Change cache-hit return (line 1708) to `return out_path, 0, 0`. (4) Replace lines 1743-1757 (from the drop_mask line through the `timing_df = timing_df[~drop_mask].copy()` line) with:
```python
    # Count events that straddle the cutoff (onset < 0, end > 0)
    straddle_mask = (timing_df["onset"] < 0) & (timing_df["end"] > 0)
    n_dropped_straddling = int(straddle_mask.sum())

    # Drop ALL events with onset < 0 (both fully-elapsed and straddling)
    drop_mask = timing_df["onset"] < 0
    n_dropped = int(drop_mask.sum())

    # Remove dropped events
    timing_df = timing_df[~drop_mask].copy()
    timing_df.drop(columns=["end"], inplace=True)
```
(5) Replace the log lines (1760-1771) with:
```python
    if n_dropped > 0:
        logger.warning(
            "%d event(s) dropped from %s (onset < 0 after removing "
            "%d non-steady-state TR(s), %.2fs offset; %d straddling)",
            n_dropped, events_path, n_remove, time_offset, n_dropped_straddling
        )
        if n_dropped_straddling > 0:
            straddle_types = timing_df_orig.loc[straddle_mask, "trial_type"].tolist()
            logger.info(
                "Straddling events dropped: %s", straddle_types
            )
```
Note: the straddle log requires keeping a reference to the pre-drop dataframe. Add `timing_df_orig = timing_df` before the drop_mask computation (the straddle_mask was computed before the drop, so it indexes into the original). (6) Change final return to `return out_path, n_dropped, n_dropped_straddling`.</spec>
      <dependencies>none</dependencies>
      <risk>low: simplifies existing code</risk>
      <rollback>Restore the clip branch and four-element return tuple</rollback>
    </change>

    <change id="C8" priority="P0" source_item="H3: update call sites and remove clipped QC fields">
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="orchestrator_utils.py" action="modify" />
      <description>Update the format_task_timing call site, replace clipped QC fields, remove the events_clipped_at_start session flag.</description>
      <spec>In orchestrate_first_level.py: (1) Line 645: change `timing_path, n_dropped, n_clipped, clipped_seconds = format_task_timing(` to `timing_path, n_dropped, n_dropped_straddling = format_task_timing(`. (2) Lines 662-664: replace `rqc["n_events_clipped_at_start"] = n_clipped` and `rqc["clipped_seconds"] = clipped_seconds` with `rqc["n_events_dropped_straddling"] = n_dropped_straddling`. Keep the `rqc["n_events_dropped_before_start"] = n_dropped` line unchanged.

In orchestrator_utils.py consolidate_session_qc: Remove lines 2567-2575 (the `events_clipped_at_start` flag block):
```python
    clipped_runs = [
        run_label for run_label, rqc in preproc_qc_by_run.items()
        if rqc.get("n_events_clipped_at_start", 0) > 0
    ]
    if clipped_runs:
        flags.append({
            "code": "events_clipped_at_start",
            "detail": ", ".join(sorted(clipped_runs)),
        })
```</spec>
      <dependencies>C7</dependencies>
      <risk>low: direct signature update</risk>
      <rollback>Restore four-element unpacking, two QC fields, and the flag block</rollback>
    </change>

    <change id="C9" priority="P0" source_item="H2: producer-FD consistency guard">
      <file path="orchestrator_utils.py" action="modify" />
      <description>In extract_motion_regressors, replace the rotation_unit_ambiguous heuristic with the producer-FD consistency guard.</description>
      <spec>In extract_motion_regressors: (1) Update docstring Returns section (lines 1275-1281): replace `rotation_unit_ambiguous` with `motion_units_check` (str), values `"producer_fd_consistent"`, `"producer_fd_inconsistent"`, `"not_checkable"`. (2) Change cache-hit return (line 1285) from `return out_path, False` to `return out_path, "not_checkable"`. (3) Add `import math` after line 19 (`import sys`); math is NOT currently imported in the file. (4) Replace lines 1327-1346 with:
```python
    # Producer-FD consistency guard: recompute Power FD from the base
    # motion parameters and compare against the producer's framewise_displacement.
    FD_RADIUS_MM = 50
    motion_units_check = "not_checkable"
    fd_col = motion_df.get("framewise_displacement")
    if fd_col is not None:
        fd_vals = pd.to_numeric(fd_col, errors="coerce").values
        if np.any(np.isfinite(fd_vals[1:])):
            base6 = motion_df[base_cols].values
            trans = base6[:, :3]
            rot_arc = base6[:, 3:] * (math.pi / 180.0) * FD_RADIUS_MM
            d = np.zeros_like(base6)
            d[1:] = np.abs(np.diff(np.hstack([trans, rot_arc]), axis=0))
            recomputed_fd = d.sum(axis=1)
            producer_fd = fd_vals.copy()
            ok_mask = np.isfinite(producer_fd) & (np.arange(len(producer_fd)) > 0)
            if np.allclose(recomputed_fd[ok_mask], producer_fd[ok_mask],
                           atol=1e-6, rtol=0):
                motion_units_check = "producer_fd_consistent"
                logger.info(
                    "Producer-FD consistency check PASSED for %s",
                    confounds_path
                )
            else:
                motion_units_check = "producer_fd_inconsistent"
                max_diff = float(np.max(np.abs(
                    recomputed_fd[ok_mask] - producer_fd[ok_mask]
                )))
                logger.error(
                    "Producer-FD consistency check FAILED for %s: "
                    "max absolute difference = %.6f mm. "
                    "Run excluded as producer_fd_inconsistent.",
                    confounds_path, max_diff
                )
                raise OrchestratorError(
                    f"Producer-FD consistency check failed for {confounds_path}: "
                    f"recomputed Power FD does not match the producer's "
                    f"framewise_displacement column (max diff {max_diff:.6f} mm).",
                    context={"reason": "producer_fd_inconsistent"}
                )
        else:
            logger.info(
                "Producer FD column entirely non-finite in %s; "
                "proceeding with degree assumption, check not performed.",
                confounds_path
            )
    else:
        logger.info(
            "No framewise_displacement column in %s; "
            "proceeding with degree assumption, check not performed.",
            confounds_path
        )
```
(5) Change final return (line 1392) to `return out_path, motion_units_check`.</spec>
      <dependencies>none</dependencies>
      <risk>medium: changes return type from bool to str; all call sites must be updated</risk>
      <rollback>Restore the max-abs-rotation block and bool return</rollback>
    </change>

    <change id="C10" priority="P0" source_item="H2: update call site and exit classification">
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="orchestrator_utils.py" action="modify" />
      <description>Update the extract_motion_regressors call site and wire producer_fd_inconsistent into exit classification.</description>
      <spec>In orchestrate_first_level.py lines 588-594: change `motion_path, rot_unit_ambiguous = extract_motion_regressors(` to `motion_path, motion_units_check = extract_motion_regressors(` and change `preproc_qc_by_run.setdefault(run_label, {})["rotation_unit_ambiguous"] = rot_unit_ambiguous` to `preproc_qc_by_run.setdefault(run_label, {})["motion_units_check"] = motion_units_check`.

In orchestrator_utils.py classify_session_exit (line 2432): change `if any(e.get("reason") == "unexpected_error" for e in preproc_excluded_runs):` to `unexplained_reasons = {"unexpected_error", "producer_fd_inconsistent"}` followed by `if any(e.get("reason") in unexplained_reasons for e in preproc_excluded_runs):`.</spec>
      <dependencies>C9</dependencies>
      <risk>low: direct rename and set expansion</risk>
      <rollback>Restore variable name and single-reason check</rollback>
    </change>

    <change id="C11" priority="P0" source_item="G14: three-way exit code in run_orchestrator.py">
      <file path="run_orchestrator.py" action="modify" />
      <description>Map exit codes 0, 1, 2 to distinct status strings in the per-subject worker and the summary.</description>
      <spec>In _run_one (~line 155-168): replace the if/else block with:
```python
        if result.returncode == 0:
            with lock:
                state["n_success"] += 1
            return "success", runtime, "", log_file
        elif result.returncode == 2:
            # Terminal ModelError failure (known cause; rerunning won't help)
            stderr_lines = [ln for ln in result.stderr.splitlines() if ln.strip()]
            if stderr_lines:
                err_msg = stderr_lines[-1]
            else:
                stdout_lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
                err_msg = stdout_lines[-1] if stdout_lines else f"Exit code {result.returncode}"
            with lock:
                state["n_failed_known"] = state.get("n_failed_known", 0) + 1
            return "failed_known_cause", runtime, err_msg, log_file
        else:
            stderr_lines = [ln for ln in result.stderr.splitlines() if ln.strip()]
            if stderr_lines:
                err_msg = stderr_lines[-1]
            else:
                stdout_lines = [ln for ln in result.stdout.splitlines() if ln.strip()]
                err_msg = stdout_lines[-1] if stdout_lines else f"Exit code {result.returncode}"
            with lock:
                state["n_failed_unexplained"] += 1
            return "failed_unexplained", runtime, err_msg, log_file
```
Rename `"n_failed"` to `"n_failed_unexplained"` in the state initialization dict and add `"n_failed_known": 0`. Update every reference to `state["n_failed"]` throughout the file to `state["n_failed_unexplained"]`. Update the summary line at the end to print all three categories (success, failed_unexplained, failed_known_cause).</spec>
      <dependencies>none</dependencies>
      <risk>low: additive change to a utility script</risk>
      <rollback>Restore the two-way if/else</rollback>
    </change>

    <change id="C12" priority="P0" source_item="G10+G11: update mock_data_utils to real v7 layout">
      <file path="tests/mock_data_utils.py" action="modify" />
      <description>Update create_mock_session_filesystem to match the real v7 layout: single directory, bare naming, res-1 masks, producer extras, consistent FD in confounds, ses-NNA label.</description>
      <spec>(1) Change session label from `ses-{session}` to `ses-{session}A` (line 275 and all downstream uses). (2) Change directory structure from `derivatives/sub-{sub_id}/ses-{session}/{func,anat}` to `derivatives/func/sub-{sub_id}/ses-{session}A/` (single directory, no separate anat/). (3) Change BOLD filename from `_space-{space}_desc-preproc_bold.nii.gz` to `_space-{space}_bold.nii.gz`. (4) Change brainmask from `_space-{space}_desc-brain_mask.nii.gz` to `_space-{space}_brainmask.nii.gz`. (5) Change CSF/WM mask names to include `_res-1_`: `_space-{space}_res-1_label-CSF_mask.nii.gz` and `_space-{space}_res-1_label-WM_mask.nii.gz`. (6) Add producer extra files in the session directory: per-run `_desc-boldref.nii.gz` (empty file), per-session `_res-1_dseg.nii.gz`, `_res-1_dseg.tsv`, `_res-1_label-GM_mask.nii.gz`, `_res-1_label-CSF_probseg.nii.gz`, `_res-1_label-GM_probseg.nii.gz`, `_res-1_label-WM_probseg.nii.gz` (all empty/stub files). (7) Add `_complete.json` with `{"schema_version": 1, "kind": "func-session", "runs": {...}, "session_files": [...], "written_at": "2026-10-26T00:00:00+00:00", "file_count": N, "source_step": "func-preproc"}`. (8) In create_mock_confounds_tsv (G11): compute mock framewise_displacement consistently from the mock motion columns using the Power FD formula (backward difference, rot * pi/180 * 50 mm). Frame 0 stays NaN. (9) Update the return dict keys to remove `anat_dir` (it no longer exists as a separate directory) and add the extras paths. (10) Remove `func_dir` and `anat_dir` from the return dict, replace with a single `session_dir`.</spec>
      <dependencies>C3 (patterns must match the new naming)</dependencies>
      <risk>medium: many test files depend on the return dict; all must be updated in C13/C14</risk>
      <rollback>Restore original mock_data_utils.py</rollback>
    </change>

    <change id="C13" priority="P0" source_item="G12: update test_discovery_v4.py and test_production_configs.py">
      <file path="tests/test_discovery_v4.py" action="modify" />
      <file path="tests/test_production_configs.py" action="modify" />
      <description>Update discovery test prefixes, helper functions, and config assertions to match the real v7 layout.</description>
      <spec>In test_discovery_v4.py: (1) Update `_mock_s3_config` templates to `"derivatives/func/sub-{subject}/ses-{session}A"` for both. (2) Update `FUNC_PREFIX` and `ANAT_PREFIX` to a single prefix: `"derivatives/func/sub-TEST001/ses-00A/"`. (3) Update `_bold_key` to produce bare `_bold.nii.gz` (no `desc-preproc_`). (4) Update `_mask_key` to produce bare `_brainmask.nii.gz` (no `desc-brain_mask`). (5) Update `_csf_key` and `_wm_key` to include `_res-1_` by default (add `res="1"` parameter). (6) Add a `test_masks_without_res_still_match` test that passes `res=None`. (7) Add a `TestRealProducerLayout` class with a 34-file inventory regression test. (8) Add a `TestCompletionGate` class with tests for absent manifest (session skipped), wrong schema_version, wrong kind, missing listed file, and manifest metadata stored. (9) Update the module docstring fixture list at the top.

In test_production_configs.py: Update `test_s3_enabled_with_placeholder_templates` to assert the new template values: `"derivatives/func/sub-{subject}/ses-{session}A"` for both templates.</spec>
      <dependencies>C3, C5, C6, C12</dependencies>
      <risk>medium: substantial test rework; mock S3 plumbing for gate tests</risk>
      <rollback>Restore original test files</rollback>
    </change>

    <change id="C14" priority="P0" source_item="G8+G12: update remaining test files for dead-code removal and new layout">
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <file path="tests/test_coverage_gaps_v2.py" action="modify" />
      <description>Remove all test classes and imports for decompress_if_needed and remove_initial_trs_tabular. Update any remaining test files that reference the old mock layout.</description>
      <spec>In test_coverage_gaps.py: Remove the imports of `decompress_if_needed` and `remove_initial_trs_tabular` from the import block (lines 54, 57). Remove `TestDecompressIfNeeded` class (lines ~84-148) and `TestRemoveInitialTrsTabular` class (lines ~238-272, the three test methods). If the file becomes empty or contains only unrelated tests, keep the remaining tests.

In test_coverage_gaps_v2.py: Remove `TestRemoveInitialTrsTabular` class (lines ~111-144) and `TestDecompressIfNeeded` class (lines ~347-384) and their associated comment blocks.

Scan all other test files that import from mock_data_utils for references to `func_dir`, `anat_dir`, `desc-preproc_bold`, `desc-brain_mask`, or the old `ses-{session}` (without A) pattern. Update those references to match the new layout from C12.</spec>
      <dependencies>C1, C12</dependencies>
      <risk>medium: must not break unrelated tests</risk>
      <rollback>Restore original test files</rollback>
    </change>

    <change id="C15" priority="P0" source_item="H8: tie-aware tissue mask integration test">
      <file path="tests/test_tissue_mask_regressors.py" action="modify" />
      <description>Add two integration test cases with exact producer NIfTI geometry, using 3dBrickStat for voxel counting (not nibabel).</description>
      <spec>Add a new class `TestProducerGeometryIntegration` marked with `@pytest.mark.afni`:

Build synthetic NIfTI images using nibabel (which remains in environment.yaml as a test-only dependency per C2). The existing test file already imports nibabel.

BOLD: shape (97, 115, 97, 12), 2mm, affine `[[-2,0,0,95.5],[0,-2,0,95.5],[0,0,2,-78.5],[0,0,0,1]]`, sform/qform code 4. Voxels with world x > 0: 1000 + 10*sin(t); world x < 0: 2000 + 10*cos(t).
Brain mask: same spatial shape, all ones, same affine and codes.
Tissue mask: (193, 229, 193), 1mm, affine `[[1,0,0,-96],[0,1,0,-132],[0,0,1,-78],[0,0,0,1]]`, sform/qform code 1.

Case 1 (`test_tie_invariant_mask`): mask voxels at world coords 19-40mm on all axes. Assert n_voxels == 1331 and regressor matches world-x>0 signal within atol=0.01.
Case 2 (`test_tie_diagnostic_mask`): mask 20-40mm on x, 19-40mm on y/z. Assert n_voxels in (1210, 1331), log the observed count.</spec>
      <dependencies>C2 (nibabel kept in environment.yaml for tests)</dependencies>
      <risk>low: new tests only; requires AFNI on PATH</risk>
      <rollback>Remove the new test class</rollback>
    </change>

  </changes>
  <execution_order>C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15</execution_order>
</implement_plan>
