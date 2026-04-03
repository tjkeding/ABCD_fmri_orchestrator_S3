<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build" timestamp="2026-03-14T07:53:14" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20260314_074946.md</spec_ref>
  <changes_applied>

    <change id="C1" status="done">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="22" />
      </files_modified>
      <notes>
        Three edits applied:
        1. Return statement on cache-hit path: `return out_path` → `return out_path, False`.
        2. Rotation unit check block: replaced the `else: raise OrchestratorError(...)` branch
           with a WARNING log + `rotation_unit_ambiguous = True`. Added `rotation_unit_ambiguous = False`
           initialization before the if/else. Extended docstring Returns to document tuple.
        3. Final return: `return out_path` → `return out_path, rotation_unit_ambiguous`.
        The > 1.0 DEBUG branch is unchanged. The WARNING message includes the TSV path and
        max_rot value for actionable QC review.
      </notes>
    </change>

    <change id="C2" status="done">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="5" />
      </files_modified>
      <notes>
        Call site at Step 8 updated: `motion_path = extract_motion_regressors(...)` →
        `motion_path, rot_unit_ambiguous = extract_motion_regressors(...)`.
        Added `preproc_qc_by_run.setdefault(run_label, {})["rotation_unit_ambiguous"] = rot_unit_ambiguous`
        immediately after, using setdefault so the flag is recorded regardless of whether
        preproc QC (Step 6) is enabled.
      </notes>
    </change>

    <change id="C3" status="done">
      <files_modified>
        <file path="tests/test_cr_implementation.py" lines_changed="28" />
      </files_modified>
      <notes>
        TestRotationUnitCheck updated:
        - test_degrees_pass_tier1: unpacks tuple, asserts rot_ambiguous is False.
        - test_small_rotation_raises_tier2 → test_small_rotation_warns_tier2: removed
          pytest.raises(OrchestratorError); now asserts file written, rot_ambiguous=True,
          "Rotation unit check AMBIGUOUS" in log.
        - test_exactly_one_raises → test_exactly_one_warns_tier2: same pattern.
        - test_just_above_one_passes: unpacks tuple, asserts rot_ambiguous is False.
        TestNaNMotionHandling and TestForceRecompute tests that do not capture the return
        value were left unchanged (Python ignores unused tuple returns silently).
      </notes>
    </change>

    <change id="C4" status="done">
      <files_modified>
        <file path="tests/test_phase2_refactored.py" lines_changed="6" />
      </files_modified>
      <notes>
        test_reads_from_motion_tsv: unpacks to (result_path, _), updates isfile/loadtxt assertions.
        test_idempotent: unpacks second call to (result_path, _).
        Seven other tests in this file that call extract_motion_regressors without
        capturing the return (side-effect-only calls) are unaffected.
      </notes>
    </change>

    <change id="C5" status="done">
      <files_modified>
        <file path="tests/test_phase2_baseline.py" lines_changed="8" />
      </files_modified>
      <notes>
        test_base_columns_only, test_n_remove_trims_rows, test_nan_replaced_with_zero,
        test_idempotent: all updated to unpack (result_path, _) and use result_path for
        loadtxt/getmtime calls. test_empty_confounds_raises and test_missing_column_raises
        do not capture return value (OrchestratorError raised before return) — unaffected.
      </notes>
    </change>

    <change id="C6" status="done">
      <files_modified>
        <file path="tests/test_cr_implementation_extended.py" lines_changed="18" />
      </files_modified>
      <notes>
        TestRotationUnitCheckExtended updated:
        - test_negative_rotation_above_threshold: unpacks, asserts rot_ambiguous=False.
        - test_small_negative_rotation_raises → test_small_negative_rotation_warns:
          converted from pytest.raises to tuple-unpack + asserts (file written,
          rot_ambiguous=True, AMBIGUOUS in log).
        - test_large_translation_does_not_affect_rotation_check: converted from
          pytest.raises to tuple-unpack + asserts (file written, rot_ambiguous=True).
          Updated docstring to reflect new behavior.
        NaN extended tests do not capture return value — unaffected.
      </notes>
    </change>

    <change id="C7" status="done">
      <files_modified>
        <file path="tests/test_coverage_gaps.py" lines_changed="2" />
      </files_modified>
      <notes>
        test_motion_extraction_on_real_data: `result = _emr(...)` → `result_path, _ = _emr(...)`;
        `np.loadtxt(result)` → `np.loadtxt(result_path)`.
      </notes>
    </change>

  </changes_applied>
  <summary>
    <total_changes>7</total_changes>
    <completed>7</completed>
  </summary>
  <next_steps>Recommended: run /test to validate all changes. Key behaviors to verify:
    1. Low-motion runs (max rot <= 1.0) now produce output files and rotation_unit_ambiguous=True.
    2. rotation_unit_ambiguous flag appears in preproc_qc_by_run[run_label] in the consolidated QC JSON.
    3. Previously-passing rotation unit tests (Tier 1, degrees) still produce rotation_unit_ambiguous=False.
    4. All 161 previously-passing tests continue to pass; the 2 previously-skipped tests remain skipped.
  </next_steps>
</implement_report>
