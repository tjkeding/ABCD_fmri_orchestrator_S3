# Test Report: CR Implementation C1-C10

```xml
<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-03-13T18:42:53Z" />
  <pre_design_run>
    <total>163</total>
    <passed>140</passed>
    <failed>21</failed>
    <errors>0</errors>
    <skipped>2</skipped>
    <coverage_pct>N/A</coverage_pct>
    <failures>
      <failure test="test_rest_conn_qc_failure_documented" file="tests/test_coverage_gaps.py" line="1217">
        <error_type>AssertionError</error_type>
        <message>assert True is False — real-world QC now shows rest_conn success after v2.3.0 bug fix</message>
        <traceback>Stale assertion: expected completed_successfully=False but v2.3.0 fixed BUG-003</traceback>
      </failure>
      <failure test="test_mean_fd_from_raw_motion" file="tests/test_phase2_refactored.py" line="241">
        <error_type>TypeError</error_type>
        <message>compute_preproc_qc() takes from 10 to 11 positional arguments but 13 were given</message>
        <traceback>C2: motion_tsv_path, TR, fd_threshold params removed from compute_preproc_qc()</traceback>
      </failure>
      <failure test="test_confounds_fd_not_used" file="tests/test_phase2_refactored.py" line="257">
        <error_type>TypeError</error_type>
        <message>Same signature mismatch as above</message>
        <traceback>C2: signature change</traceback>
      </failure>
      <failure test="test_dvars_still_from_confounds" file="tests/test_phase2_refactored.py" line="277">
        <error_type>TypeError</error_type>
        <message>Same signature mismatch</message>
        <traceback>C2: signature change</traceback>
      </failure>
      <failure test="test_censor_counts_from_raw_motion" file="tests/test_phase2_refactored.py" line="295">
        <error_type>TypeError</error_type>
        <message>Same signature mismatch; also tests removed functionality (censor)</message>
        <traceback>C2: motion/censor removed from Phase 1 QC</traceback>
      </failure>
      <failure test="test_n_remove_applied" file="tests/test_phase2_refactored.py" line="314">
        <error_type>TypeError</error_type>
        <message>Same signature mismatch</message>
        <traceback>C2: signature change</traceback>
      </failure>
      <failure test="test_missing_motion_tsv_raises" file="tests/test_phase2_refactored.py" line="326">
        <error_type>TypeError</error_type>
        <message>Same signature mismatch; motion_tsv_path no longer a param</message>
        <traceback>C2: motion_tsv_path removed</traceback>
      </failure>
      <failure test="test_accepts_fd_array" file="tests/test_phase2_refactored.py" line="365">
        <error_type>TypeError</error_type>
        <message>generate_carpet_plot() takes 6 positional arguments but 8 were given</message>
        <traceback>C2: fd_array and fd_threshold removed from generate_carpet_plot()</traceback>
      </failure>
      <failure test="test_fd_from_passed_array" file="tests/test_phase2_refactored.py" line="387">
        <error_type>TypeError</error_type>
        <message>Same signature mismatch</message>
        <traceback>C2: signature change</traceback>
      </failure>
      <failure test="test_n_remove_trims_fd_array" file="tests/test_phase2_refactored.py" line="419">
        <error_type>TypeError</error_type>
        <message>Same signature mismatch</message>
        <traceback>C2: signature change</traceback>
      </failure>
      <failure test="test_anat_mask_path_passthrough" file="tests/test_phase2_refactored.py" line="677">
        <error_type>TypeError</error_type>
        <message>compute_preproc_qc() signature mismatch</message>
        <traceback>C2: removed motion_tsv_path, TR, fd_threshold</traceback>
      </failure>
      <failure test="test_no_anat_mask_falls_through" file="tests/test_phase2_refactored.py" line="693">
        <error_type>TypeError</error_type>
        <message>Same signature mismatch</message>
        <traceback>C2: signature change</traceback>
      </failure>
      <failure test="test_404_stops_probing" file="tests/test_phase2_refactored.py" line="728">
        <error_type>AssertionError</error_type>
        <message>Test expected 404 to stop probing (break), but C9 changed to continue</message>
        <traceback>C9: break replaced with continue; all 9 indices now probed</traceback>
      </failure>
      <failure test="test_full_session_nback_and_rest" file="tests/test_simulated_pipeline.py" line="137">
        <error_type>RecursionError</error_type>
        <message>maximum recursion depth exceeded — 3dmask_tool not in mock dispatcher</message>
        <traceback>C10: compute_mask_intersection() calls 3dmask_tool, not mocked</traceback>
      </failure>
      <failure test="test_process_participant_multi_session" file="tests/test_simulated_pipeline.py" line="185">
        <error_type>RecursionError</error_type>
        <message>Same 3dmask_tool mock gap</message>
        <traceback>C10: missing mock handler</traceback>
      </failure>
      <failure test="test_nback_cue_relabeling_produces_13_conditions" file="tests/test_simulated_pipeline.py" line="212">
        <error_type>RecursionError</error_type>
        <message>Same 3dmask_tool mock gap</message>
        <traceback>C10: missing mock handler</traceback>
      </failure>
      <failure test="test_concatenated_timing_onset_offsets" file="tests/test_simulated_pipeline.py" line="253">
        <error_type>RecursionError</error_type>
        <message>Same 3dmask_tool mock gap</message>
        <traceback>C10: missing mock handler</traceback>
      </failure>
      <failure test="test_generated_first_level_config_structure" file="tests/test_simulated_pipeline.py" line="325">
        <error_type>RecursionError</error_type>
        <message>Same 3dmask_tool mock gap</message>
        <traceback>C10: missing mock handler</traceback>
      </failure>
      <failure test="test_config_paths_are_real_files" file="tests/test_simulated_pipeline.py" line="370">
        <error_type>RecursionError</error_type>
        <message>Same 3dmask_tool mock gap</message>
        <traceback>C10: missing mock handler</traceback>
      </failure>
      <failure test="test_preproc_qc_json_structure" file="tests/test_simulated_pipeline.py" line="416">
        <error_type>RecursionError + stale QC structure assertions</error_type>
        <message>Expected per-run preproc_qc.json with motion/censor keys; both removed</message>
        <traceback>C2/C3: QC consolidated; motion/censor removed from Phase 1</traceback>
      </failure>
      <failure test="test_first_level_qc_success" file="tests/test_simulated_pipeline.py" line="466">
        <error_type>RecursionError + stale QC file expectations</error_type>
        <message>Expected per-analysis first_level_qc.json; replaced by consolidated JSON</message>
        <traceback>C3: consolidated QC JSON replaces per-analysis files</traceback>
      </failure>
    </failures>
  </pre_design_run>
  <design_phase>
    <tests_created>30</tests_created>
    <tests_modified>21</tests_modified>
    <files_created>
      <file path="tests/test_cr_implementation.py" test_count="30"
            coverage_target="C1 session status, C3 consolidated QC, C4 run-loss warning, C5 rotation unit check, C6 force_recompute, C7 NaN motion, C8 task label whitelist, C10 mask intersection" />
    </files_created>
    <files_modified>
      <file path="tests/mock_subprocess.py" changes="Added _handle_3dmask_tool mock handler + afni --version support" />
      <file path="tests/test_phase2_refactored.py" changes="Updated 12 tests: compute_preproc_qc signature (C2), generate_carpet_plot signature (C2), registration QC path (C2), S3 404 probing (C9)" />
      <file path="tests/test_simulated_pipeline.py" changes="Updated 9 tests: QC validation tests (C3 consolidated JSON), happy path tests (C1 return value + C10 mock), process_participant (C1)" />
      <file path="tests/test_coverage_gaps.py" changes="Updated 1 test: rest_conn QC status (stale assertion from v2.3.0 bug fix)" />
    </files_modified>
    <design_rationale>
      1. All 21 pre-design failures were caused by the C1-C10 implementation changes:
         - C2 removed motion_tsv_path/TR/fd_threshold from compute_preproc_qc and
           fd_array/fd_threshold from generate_carpet_plot (12 tests)
         - C3 replaced per-run/per-analysis QC JSONs with consolidated orchestrator_qc.json (3 tests)
         - C9 changed S3 probing from break-on-404 to continue (1 test)
         - C10 added compute_mask_intersection with 3dmask_tool, not in mock dispatcher (6 tests)
         - Stale assertion from upstream v2.3.0 bug fix (1 test)

      2. Existing tests were updated to match the new function signatures, QC structure,
         and behavioral changes. No tests were weakened — assertions were updated to
         validate the new behavior (e.g., consolidated QC structure, Phase 1 non-motion scope).

      3. New tests (test_cr_implementation.py) cover all implementation changes:
         - C1: Session status derivation (6 tests for success/partial/failed/empty/single)
         - C3: consolidate_session_qc structure, analysis entries, empty outcomes (3 tests)
         - C4: Structured run-loss warning format with surviving count (1 test)
         - C5: Rotation unit check tiers + boundary conditions (4 tests)
         - C6: force_recompute config flag default/true/invalid + cache bypass (4 tests)
         - C7: NaN motion handling 999.0 imputation, warning, n_remove, all-NaN (5 tests)
         - C8: Task label whitelist validation (3 tests)
         - C10: Mask intersection single/multi/idempotent/force_recompute (4 tests)
    </design_rationale>
  </design_phase>
  <post_design_run>
    <total>193</total>
    <passed>191</passed>
    <failed>0</failed>
    <errors>0</errors>
    <skipped>2</skipped>
    <coverage_pct>N/A</coverage_pct>
    <failures />
  </post_design_run>
  <summary>
    <all_passing>true</all_passing>
    <recommendation>proceed_to_document</recommendation>
  </summary>
  <action_items>
    <item priority="P2" target_mode="implement"
          description="The 2 skipped tests (test_phase1_baseline.py, test_phase1_refactored.py) reference golden config baselines that may need regeneration after the C1-C10 changes. Verify golden files are still valid." />
  </action_items>
</test_report>
```
