# Implementation Build Report: CR Findings F1–F19

```xml
<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build"
        timestamp="2026-03-13T18:14:03Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20260313_165151.md</spec_ref>

  <changes_applied>

    <change id="C1" status="done">
      <!-- Per-analysis outcome tracking + qualified session reporting (F1) -->
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="~60" />
      </files_modified>
      <notes>
        _process_session() returns analysis_outcomes list. process_participant()
        derives session_status (success / partial / failed) from outcomes.
        Three counters (n_success, n_partial, n_failed) in SESSION SUMMARY.
        Per-analysis [OK] / [FAILED] log lines implemented.
        Raise guard updated: only raises if n_success == 0 AND n_partial == 0.
      </notes>
    </change>

    <change id="C2" status="done">
      <!-- Remove orchestrator FD from QC; Phase 1 non-motion only (F2) -->
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~30" />
      </files_modified>
      <notes>
        compute_preproc_qc() docstring updated to Phase 1 scope (non-motion).
        motion_tsv_path parameter removed from signature.
        Motion block (FD/censor computation) removed.
        DVARS retained from fMRIPrep confounds_df.
        Call site in orchestrate_first_level.py (Step 6) updated accordingly:
        motion_tsv_path and fd_threshold arguments removed.
      </notes>
    </change>

    <change id="C3" status="done">
      <!-- Consolidated session-level QC JSON (F2 QC enrichment) -->
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~70" />
        <file path="orchestrate_first_level.py" lines_changed="~25" />
      </files_modified>
      <notes>
        consolidate_session_qc() added to orchestrator_utils.py (Section K).
        Provenance block includes orchestrator version (3.1), fmri_first_level_proc
        version, AFNI version, and UTC timestamp.
        preproc_qc_by_run accumulates Phase 1 QC dicts (no per-run JSON writes).
        fl_qc attached to each analysis_outcome["fl_qc"] after compute_first_level_qc().
        Consolidated JSON written as sub-{ID}_{ses}_orchestrator_qc.json per session.
        Step 12b block in _process_session() calls consolidate_session_qc().
        consolidate_session_qc imported in orchestrate_first_level.py.
      </notes>
    </change>

    <change id="C4" status="done">
      <!-- Structured run-loss warning (F3) -->
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="~8" />
      </files_modified>
      <notes>
        Warning message now states N_skipped of N_total run(s) failed preprocessing
        (N_surviving surviving) and explicitly notes downstream analyses use only
        surviving runs.
      </notes>
    </change>

    <change id="C5" status="done">
      <!-- Two-tier rotation unit check (F4) -->
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~20" />
      </files_modified>
      <notes>
        Rotation unit validation block inserted in extract_motion_regressors() after
        all-NaN check and before deg2rad conversion. Tier 1: max_rot > 1.0 → DEBUG
        pass; Tier 2: max_rot <= 1.0 → OrchestratorError with diagnostic message.
      </notes>
    </change>

    <change id="C6" status="done">
      <!-- force_recompute config flag (F8) -->
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~15" />
        <file path="orchestrate_first_level.py" lines_changed="~15" />
        <file path="example_orchestrator_config.yaml" lines_changed="~5" />
      </files_modified>
      <notes>
        force_recompute=False added to all 10 caching functions in orchestrator_utils.py:
        apply_brain_mask, remove_initial_trs_bold, extract_motion_regressors,
        extract_tissue_signals, fix_nback_cue_labels, format_task_timing,
        concatenate_bolds, concatenate_tabular_files, concatenate_task_timing,
        apply_smoothing (and also compute_mask_intersection, added as part of C10).
        load_orchestrator_config() validates and defaults force_recompute.
        _process_session() extracts force_recompute and threads it to all calls.
        example_orchestrator_config.yaml documents the flag under study section.
      </notes>
    </change>

    <change id="C7" status="done">
      <!-- NaN motion handling: "unknown = censor" (F10) -->
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~20" />
      </files_modified>
      <notes>
        np.nan_to_num(nan=0.0) replaced with "unknown = censor" policy.
        NaN detection via np.isnan(); affected TRs logged at WARNING level with
        positional column labels (col_0 ... col_N). np.where(nan_mask, 999.0, motion_data)
        imputes 999.0 to guarantee upstream censoring.
      </notes>
    </change>

    <change id="C8" status="done">
      <!-- Strict task label whitelist (F11) -->
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="~10" />
        <file path="orchestrator_utils.py" lines_changed="~4" />
      </files_modified>
      <notes>
        VALID_TASK_LABELS = {"rest", "nback"} defined as module-level constant
        in orchestrate_first_level.py (before imports from orchestrator_utils).
        Whitelist check with OrchestratorError on unknown label added in per-task loop.
        is_rest = (task_label == "rest") replaces startswith("rest") in all locations
        (orchestrate_first_level.py and orchestrator_utils.py: download_session_data,
        discover_local_mmps_files, discover_session_files).
      </notes>
    </change>

    <change id="C9" status="done">
      <!-- S3 run discovery: probe all 9 indices (F12) -->
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~8" />
      </files_modified>
      <notes>
        All break statements in download_session_data() (events loop and motion loop)
        replaced with continue. Both 404/NoSuchKey and ClientError failure handlers
        now continue rather than break. Docstring updated to reflect unconditional
        probing of all run indices 1-9.
      </notes>
    </change>

    <change id="C10" status="done">
      <!-- Mask intersection for concatenated tasks (F9) -->
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~45" />
        <file path="orchestrate_first_level.py" lines_changed="~10" />
      </files_modified>
      <notes>
        compute_mask_intersection() added to orchestrator_utils.py (Section J+).
        Single-mask no-op returns mask_paths[0] directly.
        Multi-mask path: 3dmask_tool -inter with force_recompute support.
        In orchestrate_first_level.py: concat_mask = per_run_masks[0] replaced with
        compute_mask_intersection() call using concat_mask_path in concat_dir.
        compute_mask_intersection imported in orchestrate_first_level.py.
      </notes>
    </change>

  </changes_applied>

  <summary>
    <total_changes>10</total_changes>
    <completed>10</completed>
    <notes>
      All 10 changes from the implementation plan were already fully applied in the
      codebase prior to this build session. The implementation is complete.
      Version headers in both orchestrate_first_level.py and orchestrator_utils.py
      were updated to 3.1 as specified.
    </notes>
  </summary>

  <next_steps>
    All 10 planned changes are implemented. Recommended next step: run /test to
    validate all changes against the existing 161-test suite, update any tests
    that reference removed function signatures (e.g., compute_preproc_qc() no
    longer accepts motion_tsv_path), and add new tests for consolidate_session_qc(),
    compute_mask_intersection(), the rotation unit check (C5), and the NaN
    imputation policy (C7).
  </next_steps>

</implement_report>
```
