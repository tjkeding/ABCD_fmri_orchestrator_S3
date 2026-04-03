```xml
<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-03-13T19:30:11Z" />
  <pre_design_run>
    <total>193</total>
    <passed>191</passed>
    <failed>0</failed>
    <errors>0</errors>
    <skipped>2</skipped>
    <coverage_pct>n/a</coverage_pct>
    <failures />
  </pre_design_run>
  <design_phase>
    <tests_created>39</tests_created>
    <tests_modified>0</tests_modified>
    <files_created>
      <file path="tests/test_cr_implementation_extended.py" test_count="39"
            coverage_target="Extended coverage for C1-C10 implementation changes: _derive_session_status production import, force_recompute for AFNI-backed and non-AFNI functions, rotation unit check edge cases, NaN imputation boundary conditions, task label whitelist at config-load time, consolidated QC provenance validation, mask intersection edge cases, VALID_TASK_LABELS cross-module consistency, process_participant session_results structure" />
    </files_created>
    <design_rationale>
      The existing test_cr_implementation.py (23 tests) covered the core happy paths for each
      C1-C10 change. The extended suite targets gaps identified from the implementation reports:

      1. _derive_session_status was tested via a test-local replica, not the production function.
         New tests import from orchestrate_first_level directly and verify edge cases (many-to-one
         ratios of success/failure).

      2. force_recompute for AFNI-backed functions (apply_brain_mask, concatenate_bolds,
         apply_smoothing) needed validation that os.remove() is called before AFNI re-execution.
         Non-AFNI functions (format_task_timing, extract_tissue_signals, concatenate_tabular_files,
         fix_nback_cue_labels) also needed force_recompute coverage.

      3. Rotation unit check had no coverage for negative rotations or the interaction between
         large translations and small rotations (ensuring translations do not affect the check).

      4. NaN imputation had no coverage for: multiple NaN in the same TR, NaN only in translation
         columns, NaN propagation through derivative computation, or warning message accuracy
         (count + unique TR count).

      5. Task label whitelist was tested at the orchestrate_first_level.py level but not at the
         config-loading level (load_orchestrator_config). Case sensitivity was also untested.

      6. Consolidated QC JSON provenance fields (version string, ISO 8601 timestamp, AFNI version)
         and edge cases (missing fl_qc key, complex preproc passthrough) needed coverage.

      7. compute_mask_intersection needed edge case coverage for empty input list and verification
         that single-mask input does not invoke any subprocess calls.

      8. VALID_TASK_LABELS cross-module consistency (orchestrator_utils vs orchestrate_first_level
         using the same object, not a copy) was untested.

      9. _process_session return value structure (list of analysis outcome dicts with required
         keys) was untested in isolation.
    </design_rationale>
  </design_phase>
  <post_design_run>
    <total>232</total>
    <passed>230</passed>
    <failed>0</failed>
    <errors>0</errors>
    <skipped>2</skipped>
    <coverage_pct>n/a</coverage_pct>
    <failures />
  </post_design_run>
  <summary>
    <all_passing>true</all_passing>
    <recommendation>proceed_to_document</recommendation>
  </summary>
  <action_items>
    <!-- No P0/P1 items — all tests pass. Minor notes below. -->
    <item priority="P2" target_mode="implement"
          description="The 2 skipped tests (from test_coverage_gaps.py) are pre-existing and unrelated to the CR implementation. They concern tar archive extraction with DeprecationWarning for Python 3.14 filter argument. Consider updating tarfile.extract/extractall calls to use the filter parameter before Python 3.14 release." />
  </action_items>
</test_report>
```
