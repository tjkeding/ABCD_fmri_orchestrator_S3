<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-10-28T17:24:04Z" />

  <pre_design_run>
    <total>0</total>
    <passed>0</passed>
    <failed>0</failed>
    <errors>1</errors>
    <coverage_pct></coverage_pct>
    <failures>
      <failure test="collection" file="tests/conftest.py" line="40">
        <error_type>ImportError</error_type>
        <message>cannot import name 'PRIMARY_OUTPUT_SUFFIX' from 'orchestrator_utils'</message>
        <traceback>conftest.py:40: from orchestrator_utils import PRIMARY_OUTPUT_SUFFIX; ImportError raised at collection time, blocking collection of every test file that depends on conftest.py fixtures (the entire suite).</traceback>
      </failure>
    </failures>
  </pre_design_run>

  <failing_test_dispositions>
    <disposition test="collection (all files, via conftest.py import)" file="tests/conftest.py" classification="obsolete-test">
      <intended_contract>orchestrator_utils.py no longer exports PRIMARY_OUTPUT_SUFFIX as of the implement build's dead-code removal (F23); output filenames for upstream dispatch results are resolved via fmri_first_level_proc.names.frozen_primary(analysis_type, pre), confirmed as the real installed replacement for task_act, task_conn, and rest_conn.</intended_contract>
      <current_test_claim>conftest.py's _mock_run_fn (used by the _make_mock_dispatch fixture helper) imports and indexes the removed PRIMARY_OUTPUT_SUFFIX dict directly from orchestrator_utils to build the mock's output filename.</current_test_claim>
      <evidence>orchestrator_utils.py defines no PRIMARY_OUTPUT_SUFFIX symbol (grep confirms zero matches); the implement build report's next_steps section flags this exact break as expected follow-up for /test; fmri_first_level_proc.names.frozen_primary(analysis_type, pre) was verified directly against the installed package for all three analysis types before use.</evidence>
      <action>re-express: replaced the PRIMARY_OUTPUT_SUFFIX import and dict lookup with fmri_first_level_proc.names.frozen_primary(analysis_type, ns.out_file_pre); same two call sites in tests/test_coverage_gaps.py and tests/test_simulated_pipeline.py re-expressed identically.</action>
    </disposition>
  </failing_test_dispositions>

  <design_phase>
    <tests_created>15</tests_created>
    <tests_modified>32</tests_modified>
    <files_created>
      <file path="tests/test_example_configs.py" test_count="9" coverage_target="Loads example_orchestrator_config.yaml via load_orchestrator_config and example_proc_config.yaml via validate_proc_template (the published, sanitized counterparts to the production <bucket> configs); checks both parse, retired keys stay absent, and the two files remain structurally cross-consistent." />
    </files_created>
    <design_rationale>
      This design pass was driven entirely by the implement build's own signature and behavior changes, not by fresh coverage-gap discovery: every shared-infrastructure and re-expressed file traces to a specific production change (PRIMARY_OUTPUT_SUFFIX removal, four function-signature changes, the carpet-plot 3dmaskdump-to-Popen streaming rewrite, and the determine_session_routing single-head_object-call simplification). Three categories of work were done: (1) re-expression of existing assertions/call sites to match the new, already-approved production signatures, strengthening one assertion (test_restart_safety.py's skip-path test now also asserts exactly one head_object call, per the implement report's explicit request); (2) a new TestUploadProgressCadence test class with hand-computed known-answer expectations (N=1,10,25,200 against step=max(1,ceil(N/10))) for the ~10-notch upload progress log cadence, plus a new carpet-plot row-count-mismatch guard test, both covering new production behavior that had no prior test; (3) removal of planning-identifier markers from comments and docstrings across the entire tests/ directory per the project's standing no-arbitrary-markers rule, and removal of dead test-infrastructure code (an unused mock helper, an unused mock function, several unused imports) left behind by the production changes.

      One genuine test-infrastructure defect was caught and fixed during this pass, not during the original per-file edits: tests/test_qc_schema_v4.py's TestConsolidateSessionQCSchemaV4 and TestSessionFlags classes each had an autouse fixture monkeypatching orchestrator_utils.get_peak_memory_usage with a lambda still requiring a logger argument, left over from before that function's signature was reduced to zero arguments. This only surfaced when running the full suite together (both classes passed in file-level isolation, since no other test in that file exercised the code path that calls get_peak_memory_usage with zero arguments at import time of consolidate_session_qc); fixed by dropping the stale parameter from both lambdas. This is an obsolete-test disposition, not a product bug: orchestrator_utils.py's own call site already used zero arguments correctly.

      Two stale documentation claims were also corrected while in the affected files, since leaving them would misdescribe current state to a future reader: a docstring in test_cr_implementation_extended.py claimed two tests fail under the installed environment due to an upstream-version gap that no longer exists (the conda environment has carried fmri_first_level_proc 4.0.0 since the prior /test pass); and test_production_configs.py's module docstring and skip-reason text described the <bucket> production configs as "deliverables that do not exist yet," though they have existed on disk since the implement build. Both were verified to actually pass/run before the prose was corrected.
    </design_rationale>
  </design_phase>

  <post_design_run>
    <total>454</total>
    <passed>454</passed>
    <failed>0</failed>
    <errors>0</errors>
    <coverage_pct></coverage_pct>
    <failures></failures>
  </post_design_run>

  <summary>
    <assertions_preserved_or_strengthened>true</assertions_preserved_or_strengthened>
    <bugs_routed_to_implement>0</bugs_routed_to_implement>
    <recommendation>proceed_to_document</recommendation>
  </summary>

  <action_items>
    <item priority="P2" target_mode="implement" description="orchestrator_utils.py's upload_session_to_s3 still has 8 numbered inline comments (# 1. Enumerate expected targets through # 8. Conditionally write sentinel) that were not caught by the implement build's earlier Step/Phase marker sweep and violate the project's no-arbitrary-markers rule. This is production code, outside /test's tests/-only write scope. Raised once during the implement build and not yet answered by the user; still open." />
  </action_items>
</test_report>
