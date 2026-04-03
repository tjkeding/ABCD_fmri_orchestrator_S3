<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-03-16T15:09:56Z" />
  <pre_design_run>
    <total>217</total>
    <passed>203</passed>
    <failed>2</failed>
    <errors>0</errors>
    <skipped>12</skipped>
    <warnings>3</warnings>
    <coverage_pct>N/A</coverage_pct>
    <failures>
      <failure test="TestRotationUnitCheck::test_degrees_pass_tier1" file="tests/test_cr_implementation.py" line="111">
        <error_type>AssertionError</error_type>
        <message>assert 'Rotation unit check PASSED' in log (actual log: 'Rotation unit check: PASSED ...')</message>
        <traceback>String match failure: test asserts "Rotation unit check PASSED" but source code emits "Rotation unit check: PASSED" (colon added in prior refactor).</traceback>
      </failure>
      <failure test="TestRotationUnitCheckExtended::test_negative_rotation_above_threshold" file="tests/test_cr_implementation_extended.py" line="175">
        <error_type>AssertionError</error_type>
        <message>assert 'Rotation unit check PASSED' in log (actual log: 'Rotation unit check: PASSED ...')</message>
        <traceback>Same root cause as above: log message format mismatch after refactor.</traceback>
      </failure>
    </failures>
  </pre_design_run>
  <design_phase>
    <tests_created>57</tests_created>
    <tests_modified>2</tests_modified>
    <files_created>
      <file path="tests/test_coverage_gaps_v2.py" test_count="57" coverage_target="detect_non_steady_state_trs, remove_initial_trs_tabular, extract_tissue_signals, format_task_timing, fix_nback_cue_labels, write_temp_config, save_qc_json, decompress_if_needed, load_orchestrator_config (extended), validate_proc_template (extended), concatenate_tabular_files (extended), _derive_session_status (direct import), apply_smoothing validation, compute_mask_intersection edge cases, extract_motion_regressors edge cases, build_first_level_config edge cases" />
    </files_created>
    <files_modified>
      <file path="tests/test_cr_implementation.py" change="Fixed log assertion: 'Rotation unit check PASSED' -> 'Rotation unit check: PASSED'" />
      <file path="tests/test_cr_implementation_extended.py" change="Fixed log assertion: 'Rotation unit check PASSED' -> 'Rotation unit check: PASSED'" />
    </files_modified>
    <design_rationale>The 2 pre-existing failures were string-matching regressions caused by a log message format change (colon insertion) in a prior refactor. The 57 new tests target functions and code paths that had zero or minimal coverage: NSS detection, tabular TR removal, tissue signal extraction (including cache/force_recompute), task timing formatting edge cases (conditions_exclude, negative onset drops, missing columns), nback cue label edge cases, config writing/serialization, decompression branches (.gz, .nii.gz handling), extensive orchestrator config validation paths (smoothing, S3 bucket/prefix rules, session codes, force_recompute, calc_n_motion_derivs, censor_prev_tr), proc template cross-validation (type mismatch, extraction prefix requirement, unreferenced template warnings), concatenation format correctness (integer/float/multi-column), direct _derive_session_status testing, smoothing method validation, mask intersection empty-list edge case, motion regressor derivative counts and NaN imputation, and build_first_level_config error paths.</design_rationale>
  </design_phase>
  <post_design_run>
    <total>274</total>
    <passed>274</passed>
    <failed>0</failed>
    <errors>0</errors>
    <skipped>12</skipped>
    <warnings>3</warnings>
    <coverage_pct>N/A</coverage_pct>
    <failures>
    </failures>
  </post_design_run>
  <summary>
    <all_passing>true</all_passing>
    <delta>+57 new tests, 2 fixes, net +71 passing (203 -> 274)</delta>
    <recommendation>proceed_to_document</recommendation>
  </summary>
  <action_items>
    <item priority="P2" target_mode="implement" description="DeprecationWarning: Python 3.14 will filter extracted tar archives by default. Add filter='data' argument to tarfile.extractall() and tar.extract() calls in orchestrator_utils.py (lines ~477, ~857) to suppress warnings and prepare for Python 3.14 compatibility." />
    <item priority="P2" target_mode="implement" description="concatenate_tabular_files has a latent edge case: a multi-column file with exactly 1 row is loaded by np.loadtxt as 1D and reshaped to (-1, 1), losing multi-column structure. In practice this cannot occur (concatenation is only invoked with multi-row run files), but a defensive ndmin=2 in np.loadtxt would make the function robust to degenerate inputs." />
  </action_items>
</test_report>
