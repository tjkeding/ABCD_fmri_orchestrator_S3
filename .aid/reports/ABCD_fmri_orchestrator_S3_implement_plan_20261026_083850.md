<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-10-26T12:38:50Z" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_brainstorm_20261022_174139.md" mode="brainstorm" key_items="8" />
  </input_reports>
  <changes>
    <change id="C1" priority="P0" source_item="H7a: mask regex res-entity">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Add optional BIDS res entity to the csf_mask and wm_mask patterns in INPUT_FILE_PATTERNS so they match the producer's res-1 filenames.</description>
      <spec>At lines 631-640, change the two mask pattern strings from:
```
r"_space-(?P&lt;space&gt;[A-Za-z0-9]+)_label-CSF_mask\.nii\.gz$"
```
to:
```
r"_space-(?P&lt;space&gt;[A-Za-z0-9]+)(?:_res-(?P&lt;res&gt;[A-Za-z0-9]+))?_label-CSF_mask\.nii\.gz$"
```
and the same for the WM pattern. The `(?:_res-(?P&lt;res&gt;[A-Za-z0-9]+))?` group is non-capturing and optional, so filenames without res still match. The named group `res` is unique within each pattern (no conflict with other named groups: sub, ses, space). The existing duplicate-mask OrchestratorError (lines 778-782) remains unchanged.</spec>
      <dependencies>none</dependencies>
      <risk>low - single regex edit per pattern; existing tests will catch regressions</risk>
      <rollback>Revert the two regex strings to their original form</rollback>
    </change>

    <change id="C2" priority="P0" source_item="H7b: config path templates">
      <file path="orch_config_production.yaml" action="modify" />
      <description>Set both func_prefix_template and anat_prefix_template to the verified producer path layout. The producer stores all session files (masks and run files) under the same derivatives/func/ prefix.</description>
      <spec>Replace lines 111-112:
```yaml
  func_prefix_template: "derivatives/sub-{subject}/ses-{session}/func"
  anat_prefix_template: "derivatives/sub-{subject}/ses-{session}/anat"
```
with:
```yaml
  func_prefix_template: "derivatives/func/sub-{subject}/ses-{session}A"
  anat_prefix_template: "derivatives/func/sub-{subject}/ses-{session}A"
```
Keep bucket as `__PRODUCER_BUCKET__`. Update the preceding comment (lines 109-110) to note that both point to the same prefix because the producer stores everything under derivatives/func/.</spec>
      <dependencies>none</dependencies>
      <risk>low - config only; validated against real S3 layout</risk>
      <rollback>Revert the two template values</rollback>
    </change>

    <change id="C3" priority="P0" source_item="H7d: known non-input file ignore list">
      <file path="orchestrator_utils.py" action="modify" />
      <description>In discover_subject_inputs, replace the blanket WARNING for non-matching files with DEBUG for known producer files that the orchestrator does not consume, keeping WARNING for genuinely unexpected files.</description>
      <spec>Add a module-level compiled regex set after INPUT_FILE_PATTERNS (around line 642):
```python
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
In the candidate-parsing loop (line 719), when `role is None`, check `basename` against `_KNOWN_NON_INPUT_PATTERNS`. If any pattern matches via `search` (or for `_complete.json`, exact basename match), log at DEBUG; otherwise keep the existing WARNING:
```python
if role is None:
    if any(p.search(basename) for p in _KNOWN_NON_INPUT_PATTERNS):
        logger.debug("Known non-input file, ignoring: %s", value)
    else:
        logger.warning("Non-matching input file, ignoring: %s", value)
    continue
```</spec>
      <dependencies>none</dependencies>
      <risk>low - logging only; no functional change</risk>
      <rollback>Remove the pattern list and the conditional; restore the single WARNING line</rollback>
    </change>

    <change id="C4" priority="P0" source_item="H7c: _complete.json producer completion gate">
      <file path="orchestrator_utils.py" action="modify" />
      <description>After discovery phases 1-3 complete, add a completion-gate phase that reads each session's _complete.json manifest from S3 (or local disk), validates it, cross-checks its file list against what was discovered, and removes sessions without a valid manifest.</description>
      <spec>Add a new phase 4 in discover_subject_inputs, after the Phase 3 loop and before the log summary (before line 805). Import json at the top of the file (already imported on line 24).

**Phase 4: Producer completion gate**

For each session code in `result`:
1. Determine the manifest path: in S3 mode, the S3 key is `{prefix for this session}/_complete.json`; compute from the func_prefix_template with subject and session substituted. In local mode, walk the session directory for `_complete.json`.
2. Try to read it (S3: `s3_client.get_object`; local: `open`). If absent (S3 404 or FileNotFoundError), log INFO "Session ses-{ses}: no producer completion manifest, skipping" and mark the session for removal. Continue to the next session.
3. Parse the JSON. Validate: `schema_version` must equal 1 and `kind` must equal `"func-session"`. If not, raise OrchestratorError with a message naming the session and the offending values.
4. Cross-check: for each configured task that has runs in this session, every file in `manifest["runs"][task_key]` (where task_key matches `task-{label}_run-{NN}`) must have been discovered as a basename in the parsed set. For `manifest["session_files"]`, check each basename that matches the csf_mask or wm_mask patterns. If any listed file is missing from discovery, raise OrchestratorError naming the session and the missing file(s).
5. Store `manifest.get("written_at")` and `manifest.get("schema_version")` in the result dict under a new key per session: `result[ses]["_manifest"] = {"written_at": ..., "schema_version": ...}`. The orchestrator reads this later for the session QC JSON.

After the loop, remove all sessions marked for removal. If the result dict is now empty, return `{}` (the caller already handles the "no sessions discovered" error).

The `_manifest` key must not collide with task labels (task labels are alphanumeric from the config; `_manifest` starts with an underscore, so no collision is possible).

For S3 mode, the session prefix is computed as `func_template.replace("{subject}", sub_id).replace("{session}", ses)` where `ses` is the session code. For the manifest key, append `/_complete.json`.</spec>
      <dependencies>C3 (the ignore list must already be in place so _complete.json logs at DEBUG, not WARNING, during the parse-candidates phase)</dependencies>
      <risk>medium - adds network I/O (one small GET per session); relies on the manifest schema staying stable; adds a removal loop that could empty the result</risk>
      <rollback>Remove the Phase 4 block and the _manifest key handling</rollback>
    </change>

    <change id="C5" priority="P0" source_item="H3: always-drop straddling events">
      <file path="orchestrator_utils.py" action="modify" />
      <description>In format_task_timing, remove the clip branch. After the onset shift, drop every event whose shifted onset is negative. Add a count of straddling events (onset &lt; 0 and end &gt; 0) as a subset of the dropped count.</description>
      <spec>In format_task_timing (starting at line 1667):

1. Change the docstring (lines 1670-1677): remove "and clip" and the description of clipping; state that events whose shifted onset &lt; 0 are dropped.

2. Change the return type in the docstring (line 1697-1698) from `(str, int, int, float)` with `(path, n_dropped, n_clipped, clipped_seconds)` to `(str, int, int)` with `(path, n_dropped, n_dropped_straddling)`.

3. Change the cache-hit return (line 1708) from `return out_path, 0, 0, 0.0` to `return out_path, 0, 0`.

4. After computing `timing_df["end"]` (line 1741), replace lines 1743-1754 with:
```python
    # Count events that straddle the cutoff (onset < 0, end > 0)
    straddle_mask = (timing_df["onset"] < 0) & (timing_df["end"] > 0)
    n_dropped_straddling = int(straddle_mask.sum())

    # Drop ALL events with onset < 0 (both fully-elapsed and straddling)
    drop_mask = timing_df["onset"] < 0
    n_dropped = int(drop_mask.sum())
```

5. Replace the log lines (1760-1771) with:
```python
    if n_dropped > 0:
        logger.warning(
            "%d event(s) dropped from %s (onset < 0 after removing "
            "%d non-steady-state TR(s), %.2fs offset; %d straddling)",
            n_dropped, events_path, n_remove, time_offset, n_dropped_straddling
        )
        if n_dropped_straddling > 0:
            straddle_types = timing_df.loc[straddle_mask, "trial_type"].tolist()
            logger.info(
                "Straddling events dropped: %s", straddle_types
            )
```

6. Change the final return (line 1776) to `return out_path, n_dropped, n_dropped_straddling`.</spec>
      <dependencies>none</dependencies>
      <risk>low - simplifies existing code; removes a branch</risk>
      <rollback>Restore the clip branch and the four-element return tuple</rollback>
    </change>

    <change id="C6" priority="P0" source_item="H3: update call sites for new return signature">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Update the format_task_timing call site and QC recording to use the new three-element return tuple, replacing clipped QC fields.</description>
      <spec>1. At line 645, change:
```python
timing_path, n_dropped, n_clipped, clipped_seconds = format_task_timing(
```
to:
```python
timing_path, n_dropped, n_dropped_straddling = format_task_timing(
```

2. At lines 662-664, replace:
```python
rqc["n_events_dropped_before_start"] = n_dropped
rqc["n_events_clipped_at_start"] = n_clipped
rqc["clipped_seconds"] = clipped_seconds
```
with:
```python
rqc["n_events_dropped_before_start"] = n_dropped
rqc["n_events_dropped_straddling"] = n_dropped_straddling
```

3. Remove the session flag block at lines 2567-2575 (the `events_clipped_at_start` flag):
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
```
Remove this entire block.</spec>
      <dependencies>C5</dependencies>
      <risk>low - direct signature update; no logic change beyond removal</risk>
      <rollback>Restore the four-element unpacking and the three QC fields and the flag block</rollback>
    </change>

    <change id="C7" priority="P0" source_item="H2: producer-FD consistency guard replacing rotation_unit_ambiguous">
      <file path="orchestrator_utils.py" action="modify" />
      <description>In extract_motion_regressors, replace the max-abs-rotation heuristic with a per-run check that recomputes Power FD from the confounds and compares it to the producer's framewise_displacement column.</description>
      <spec>In extract_motion_regressors (lines 1236-1392):

1. Add `import math` at the top of the file if not already present (it is, on line 29).

2. Change the docstring Returns section (lines 1275-1281): replace `rotation_unit_ambiguous` with `motion_units_check` (str), values: `"producer_fd_consistent"`, `"producer_fd_inconsistent"`, `"not_checkable"`.

3. Change the cache-hit return (line 1285) from `return out_path, False` to `return out_path, "not_checkable"`.

4. Replace lines 1327-1346 (the rotation unit validation block) with:
```python
    # Producer-FD consistency guard: recompute Power FD from trans/rot
    # and compare with the producer's framewise_displacement column.
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
            # Compare frames 1..T-1 (frame 0 is NaN in the producer)
            ok_mask = np.isfinite(producer_fd) & (np.arange(len(producer_fd)) > 0)
            if np.allclose(recomputed_fd[ok_mask], producer_fd[ok_mask], atol=1e-6, rtol=0):
                motion_units_check = "producer_fd_consistent"
                logger.info(
                    "Producer-FD consistency check PASSED for %s",
                    confounds_path
                )
            else:
                motion_units_check = "producer_fd_inconsistent"
                max_diff = float(np.max(np.abs(recomputed_fd[ok_mask] - producer_fd[ok_mask])))
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

5. Change the final return (line 1392) from `return out_path, rotation_unit_ambiguous` to `return out_path, motion_units_check`.</spec>
      <dependencies>none</dependencies>
      <risk>medium - changes the return type from bool to str; all call sites must be updated</risk>
      <rollback>Restore the max-abs-rotation block and the bool return</rollback>
    </change>

    <change id="C8" priority="P0" source_item="H2: update call site and exit classification for the new guard">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Update the extract_motion_regressors call site to use the new return field name, and record it in QC. Wire producer_fd_inconsistent into classify_session_exit as EXIT_FAILED_UNEXPLAINED.</description>
      <spec>1. At lines 588-594, change:
```python
motion_path, rot_unit_ambiguous = extract_motion_regressors(
    ...
)
preproc_qc_by_run.setdefault(run_label, {})["rotation_unit_ambiguous"] = rot_unit_ambiguous
```
to:
```python
motion_path, motion_units_check = extract_motion_regressors(
    ...
)
preproc_qc_by_run.setdefault(run_label, {})["motion_units_check"] = motion_units_check
```

2. In orchestrator_utils.py classify_session_exit (line 2432), change:
```python
if any(e.get("reason") == "unexpected_error" for e in preproc_excluded_runs):
    return EXIT_FAILED_UNEXPLAINED
```
to:
```python
unexplained_reasons = {"unexpected_error", "producer_fd_inconsistent"}
if any(e.get("reason") in unexplained_reasons for e in preproc_excluded_runs):
    return EXIT_FAILED_UNEXPLAINED
```</spec>
      <dependencies>C7</dependencies>
      <risk>low - direct signature update</risk>
      <rollback>Restore rot_unit_ambiguous variable name and single-reason check</rollback>
    </change>

    <change id="C9" priority="P0" source_item="H7 tests: update discovery fixtures and add regression/gate tests">
      <file path="tests/test_discovery_v4.py" action="modify" />
      <description>Update mask fixture helpers to produce res-1 names by default, keep one backward-compat test without res; add a regression test from the real 34-file session inventory; add completion-gate tests.</description>
      <spec>1. Change `_csf_key` and `_wm_key` helpers (lines 64-69) to include `_res-1` by default:
```python
def _csf_key(prefix, sub, ses, space=SPACE, res="1"):
    res_str = f"_res-{res}" if res else ""
    return f"{prefix}sub-{sub}_ses-{ses}_space-{space}{res_str}_label-CSF_mask.nii.gz"

def _wm_key(prefix, sub, ses, space=SPACE, res="1"):
    res_str = f"_res-{res}" if res else ""
    return f"{prefix}sub-{sub}_ses-{ses}_space-{space}{res_str}_label-WM_mask.nii.gz"
```

2. Update all existing calls to `_csf_key` and `_wm_key` to use the default (which now includes res-1).

3. Add a test `test_masks_without_res_still_match` that passes `res=None` and asserts both masks are discovered.

4. Change `_mock_s3_config` to use the updated templates:
```python
"func_prefix_template": "derivatives/func/sub-{subject}/ses-{session}A",
"anat_prefix_template": "derivatives/func/sub-{subject}/ses-{session}A",
```
And update `FUNC_PREFIX` and `ANAT_PREFIX` constants to match the single prefix: `"derivatives/func/sub-TEST001/ses-00A/"`.

5. Add a `TestRealProducerLayout` class with a `test_34_file_session_inventory` test: build the exact 34-file inventory (2 nback + 4 rest runs, 8 session files) using synthetic subject `TESTV7` and session `00A`, with the real producer naming convention (no desc-preproc, brainmask not desc-brain_mask, res-1 masks), plus `_complete.json`, boldref, probseg, GM mask, dseg files. Mock S3 returns these. Assert 6 runs discovered, both masks present, no WARNING logged (only DEBUG for non-input files).

6. Add a `TestCompletionGate` class (requires mocking `s3_client.get_object` for `_complete.json` reads) with tests:
- `test_session_skipped_when_manifest_absent`: mock 404 on the manifest GET; assert the session is not in the returned dict.
- `test_error_on_wrong_schema_version`: manifest with `schema_version: 2`; assert OrchestratorError.
- `test_error_on_wrong_kind`: manifest with `kind: "anat-session"`; assert OrchestratorError.
- `test_error_on_missing_listed_file`: manifest lists a file that was not in the S3 listing; assert OrchestratorError.
- `test_manifest_metadata_stored`: valid manifest; assert `result[ses]["_manifest"]` contains `written_at` and `schema_version`.</spec>
      <dependencies>C1, C2, C3, C4</dependencies>
      <risk>medium - substantial test additions; mock S3 plumbing for the gate tests must simulate get_object for _complete.json</risk>
      <rollback>Revert fixture changes and remove new test classes</rollback>
    </change>

    <change id="C10" priority="P0" source_item="H8: tie-aware tissue mask integration test">
      <file path="tests/test_tissue_mask_regressors.py" action="modify" />
      <description>Add two integration test cases using the exact producer NIfTI geometry that exercise compute_tissue_mask_regressor against real AFNI.</description>
      <spec>Add a new class `TestProducerGeometryIntegration` (requires real AFNI, mark with `@pytest.mark.afni`):

**Shared setup**: build synthetic NIfTI images with nibabel:
- BOLD: shape (97, 115, 97, 12), 2 mm voxels, affine `[[-2,0,0,95.5],[0,-2,0,95.5],[0,0,2,-78.5],[0,0,0,1]]`, sform/qform code 4. Intensity: voxels at world x > 0 get `1000 + 10*sin(t)`, world x &lt; 0 get `2000 + 10*cos(t)`.
- Brain mask: same shape (97, 115, 97), all ones, same affine and codes.
- Tissue mask: shape (193, 229, 193), 1 mm, affine `[[1,0,0,-96],[0,1,0,-132],[0,0,1,-78],[0,0,0,1]]`, sform/qform code 1.

**Case 1** (`test_tie_invariant_mask`): mask covers 1 mm voxel indices for world coordinates 19 through 40 mm on all three axes (i.e. mask[xi, yi, zi] = 1 where xi = slice(96+19, 96+41), yi = slice(132+19, 132+41), zi = slice(78+19, 78+41)). Assert:
- `n_voxels == 1331` (11^3)
- Regressor values match the world-x > 0 signal within `atol=0.01`.

**Case 2** (`test_tie_diagnostic_mask`): mask covers 20-40 mm on x and 19-40 mm on y and z (xi = slice(96+20, 96+41), others same as Case 1). Assert:
- `n_voxels in (1210, 1331)` (11*11*10 or 11^3)
- Log the observed count and tie direction for manual comparison with AFNI 26.3.02.</spec>
      <dependencies>none (compute_tissue_mask_regressor is unchanged)</dependencies>
      <risk>low - new tests only; requires AFNI on PATH</risk>
      <rollback>Remove the new test class</rollback>
    </change>
  </changes>
  <execution_order>C1, C2, C3, C4, C5, C6, C7, C8, C9, C10</execution_order>
</implement_plan>
