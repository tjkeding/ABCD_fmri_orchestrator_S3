<?xml version="1.0" encoding="UTF-8"?>
<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build" timestamp="2026-03-13T15:14:49Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20260313_151004.md</spec_ref>

  <changes_applied>

    <change id="C8" status="done">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="14" />
        <file path="orchestrate_first_level.py" lines_changed="3" />
      </files_modified>
      <notes>
        VALID_TASK_LABELS = {"rest", "nback"} added as a module-level constant in
        orchestrator_utils.py immediately after OrchestratorError, with an explanatory
        comment noting it is the authoritative definition imported by the entry point.
        Whitelist validation added in load_orchestrator_config() within the per-task
        loop, after task_labels.add(). In orchestrate_first_level.py, the local
        VALID_TASK_LABELS definition was removed and VALID_TASK_LABELS is now imported
        from orchestrator_utils alongside OrchestratorError.
      </notes>
    </change>

    <change id="C7" status="done">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="2" />
        <file path="orchestrator_utils.py" lines_changed="9" />
      </files_modified>
      <notes>
        __version__ = "3.1" added to orchestrate_first_level.py immediately after the
        fmri_first_level_proc imports (before the orchestrator_utils import block).
        In consolidate_session_qc(), a lazy import block retrieves __version__ from
        orchestrate_first_level with an ImportError fallback to "unknown". The
        hardcoded "3.1" string literal replaced with _orch_ver.
      </notes>
    </change>

    <change id="C2" status="done">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="20" />
      </files_modified>
      <notes>
        Inserted `if force_recompute and os.path.isfile(out_path): os.remove(out_path)`
        with a debug-level log in all 5 AFNI-backed caching functions:
          1. apply_brain_mask — after cache-hit early return, before cmd construction
          2. remove_initial_trs_bold — after cache-hit early return, before cmd construction
          3. concatenate_bolds — after cache-hit early return, before single-run copy branch
          4. apply_smoothing — after cache-hit early return, before method dispatch
          5. compute_mask_intersection — after cache-hit early return, before debug log (C6)
        Non-AFNI caching functions (concatenate_tabular_files, concatenate_task_timing,
        extract_motion_regressors, etc.) were not modified — they use shutil.copy2 /
        np.savetxt which overwrite silently and work correctly with force_recompute as-is.
      </notes>
    </change>

    <change id="C1" status="done">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="4" />
      </files_modified>
      <notes>
        session_start_time = time.time() added at the top of _process_session() body,
        immediately after ses_label is assigned. At Step 12b (consolidated QC JSON),
        session_wall_time = time.time() - session_start_time computed inline and passed
        as the fourth argument to consolidate_session_qc(), replacing the literal 0.0.
      </notes>
    </change>

    <change id="C3" status="done">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="25" />
      </files_modified>
      <notes>
        _derive_session_status() extracted as a module-level helper with full docstring,
        placed immediately before process_participant(). Both inline if/elif/else blocks
        replaced with single-line calls:
          - process_participant(): session_status = _derive_session_status(analysis_outcomes)
          - _process_session() Step 12b: qc_session_status = _derive_session_status(analysis_outcomes)
        Net reduction of ~14 lines of duplicated logic. Test suite note: existing
        TestSessionStatusDerivation tests a private _derive_status() replica — run /test
        after this change to update those tests to import and exercise the production function.
      </notes>
    </change>

    <change id="C4" status="done">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="5" />
      </files_modified>
      <notes>
        per_run_labels = [] added to the per-run variable initializations alongside
        per_run_masks. per_run_labels.append(run_label) inserted immediately after the
        existing per_run_masks.append(rd["mask_path"]) at the bottom of the success
        path for each run. Rest smoothing loop updated to use per_run_masks[i] (mask)
        and per_run_labels[i] (run label prefix) instead of run_dicts[i]["mask_path"]
        and run_dicts[i]["run_label"]. Index parity between per_run_bolds, per_run_masks,
        and per_run_labels is now guaranteed by construction.
      </notes>
    </change>

    <change id="C5" status="done">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="8" />
      </files_modified>
      <notes>
        detect_non_steady_state_trs() call moved to immediately after the
        decompress_if_needed block (before Step 5), with a comment explaining that
        n_remove is reused for both QC (Step 6) and trimming (Step 7). The redundant
        second call at Step 7 was removed. The local variable n_remove_for_qc was
        eliminated; compute_preproc_qc() now receives n_remove directly. The Step 7
        section comment updated from "Detect & remove non-steady-state TRs" to
        "Remove non-steady-state TRs" to reflect that detection now occurs earlier.
      </notes>
    </change>

    <change id="C6" status="done">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="12" />
      </files_modified>
      <notes>
        Debug-level log block inserted after the force_recompute os.remove (C2) and
        before the 3dmask_tool command construction in compute_mask_intersection().
        Uses subprocess.run(["3dinfo", "-n4", mask_paths[0]]) to retrieve reference
        mask grid dimensions. Any exception (FileNotFoundError, CalledProcessError)
        is caught and a second debug-level message is emitted instead. No production
        impact — debug logs are suppressed at INFO level.
      </notes>
    </change>

  </changes_applied>

  <summary>
    <total_changes>8</total_changes>
    <completed>8</completed>
    <deferred>0</deferred>
  </summary>

  <deferred_findings>
    <finding id="F3" reason="Conservative NaN handling is scientifically warranted; deferred by user decision." />
    <finding id="F5" reason="Rotation unit check false-positive risk is outside ABCD scope; deferred by user decision." />
    <finding id="F11" reason="Automatically resolved by C3 (_derive_session_status now importable); update tests via /test." />
  </deferred_findings>

  <next_steps>
    Recommended: run /test to validate all changes. Priority test updates:
    1. TestSessionStatusDerivation — update to import and call _derive_session_status()
       from orchestrate_first_level instead of the internal _derive_status() replica.
    2. force_recompute tests for AFNI-backed functions — add at least one test verifying
       os.remove is called (or the file is recreated) when force_recompute=True and the
       output file already exists (mock subprocess is sufficient).
    3. Regression tests for the per-run smoothing path (C4) — verify that when run 1
       fails preprocessing and run 2 succeeds, the correct mask and label are used.
  </next_steps>

</implement_report>
```
