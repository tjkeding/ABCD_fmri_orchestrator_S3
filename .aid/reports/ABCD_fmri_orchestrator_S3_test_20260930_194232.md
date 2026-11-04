<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-09-30T19:42:32Z" />

  <pre_design_run>
    <total>229</total>
    <passed>197</passed>
    <failed>31</failed>
    <errors>1</errors>
    <coverage_pct>n/a</coverage_pct>
    <notes>1 error was a collection error (import failure) that masked additional latent failures in tests/test_coverage_gaps.py until resolved during the design phase. Individual per-test records from this run were not retained verbatim past the disposition ledger review; dispositions below were reviewed and approved by the user at cluster granularity (per-file, grouped by shared root cause), which is the granularity at which this report's failing_test_dispositions are also expressed.</notes>
  </pre_design_run>

  <failing_test_dispositions>
    <disposition test="cluster: BIDS TSV timing contract (multiple tests)" file="tests/test_coverage_gaps_v2.py, tests/test_motion_and_qc.py, tests/test_simulated_pipeline.py, tests/test_coverage_gaps.py" classification="obsolete-test">
      <intended_contract>format_task_timing writes BIDS-standard column names (trial_type, onset) per the current production implementation, not the legacy CONDITION/ONSET column names.</intended_contract>
      <current_test_claim>Multiple tests asserted df["CONDITION"] / df["ONSET"] against the written timing file.</current_test_claim>
      <evidence>orchestrator_utils.py format_task_timing (read in full): output DataFrame columns are trial_type, onset, duration; to_csv writes sep="\t".</evidence>
      <action>re-express: CONDITION -> trial_type, ONSET -> onset across all affected assertions. In tests/test_coverage_gaps.py::TestFormatTaskTiming::test_conditions_exclude_filter, re-expression additionally exposed a second, previously-masked separator bug (pd.read_csv default comma separator against a tab-separated file); fixed in the same pass as a strengthening, not a weakening, of the assertion (the test now actually executes against real values instead of raising).</action>
    </disposition>

    <disposition test="cluster: _derive_session_status two-argument signature (multiple tests)" file="tests/test_coverage_gaps_v2.py, tests/test_cr_implementation_extended.py" classification="obsolete-test">
      <intended_contract>_derive_session_status(routing, analysis_outcomes) takes two positional arguments as of the v3.1.0-aligned refactor.</intended_contract>
      <current_test_claim>Tests called _derive_session_status(outcomes) with a single argument (pre-refactor signature).</current_test_claim>
      <evidence>orchestrator_utils.py _derive_session_status signature (read directly): def _derive_session_status(routing, analysis_outcomes).</evidence>
      <action>re-express: add the routing argument to every call site (9 call sites: 7 in test_coverage_gaps_v2.py, 8 in test_cr_implementation_extended.py per the disposition ledger's original count spanning both files under this same pattern).</action>
    </disposition>

    <disposition test="cluster: _process_session dict-vs-list return shape (multiple tests)" file="tests/test_cr_implementation_extended.py, tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>_process_session returns a dict with keys routing, analysis_outcomes, qc_path, remote_metadata (per the session-centric refactor), not a bare list of outcomes.</intended_contract>
      <current_test_claim>Tests treated the return value as a bare list of outcome dicts and indexed/iterated it directly.</current_test_claim>
      <evidence>orchestrate_first_level.py _process_session return statement and process_participant's consumption of session_result["routing"] / session_result["analysis_outcomes"] (read directly).</evidence>
      <action>re-express: unwrap result["analysis_outcomes"] before asserting on the outcomes list; assert result["routing"] separately. Applied to 2 assertions in test_cr_implementation_extended.py per the original disposition count.</action>
    </disposition>

    <disposition test="cluster: stale version string literals (multiple tests)" file="tests/test_simulated_pipeline.py, tests/test_cr_implementation.py" classification="obsolete-test">
      <intended_contract>Provenance version strings reflect the current fmri_first_level_proc target version (3.1.0) established by this migration.</intended_contract>
      <current_test_claim>Tests asserted a stale prior version literal (e.g., 3.1 as an upper bound, or an unrelated stale literal later corrected to 4.0 in test_cr_implementation.py's case).</current_test_claim>
      <evidence>Migration tech spec (ABCD_fmri_orchestrator_S3_implement_plan_20260930_142102.md) establishes v3.1.0 as the target; consolidate_session_qc's provenance block writes the installed fmri_first_level_proc.__version__ at runtime.</evidence>
      <action>re-express: update literal version-string assertions to match the v3.1.0 migration target.</action>
    </disposition>

    <disposition test="TestUploadToS3, TestCompressSessionOutputs (module obsolescence)" file="tests/test_coverage_gaps.py" classification="obsolete-test">
      <intended_contract>Session outputs are uploaded to S3 per-file via enumerate_upload_targets (pure filesystem-walk enumeration) and upload_session_to_s3 (ThreadPoolExecutor upload + gap-detection + post-upload verification + _COMPLETE sentinel), per commit 46ad7f9 (2026-05-14), which predates this v3.1.0 migration by several months.</intended_contract>
      <current_test_claim>TestCompressSessionOutputs and TestUploadToS3 tested compress_session_outputs and upload_to_s3, neither of which exists in current production code; these tests were only reachable for execution once an unrelated, more-recent import error (verify_afni_installation) was fixed earlier in this same design pass, which is why this defect was not visible in the original 31-failure pre-design run.</current_test_claim>
      <evidence>Direct read of orchestrator_utils.py: enumerate_upload_targets (lines 73-165) and upload_session_to_s3 (lines 347-504) are the sole production upload entry points; compress_session_outputs and upload_to_s3 are absent from the module. Confirmed via grep across the module.</evidence>
      <action>Surfaced to the user as a new, previously out-of-scope discovery per the Scope Authority doctrine (AskUserQuestion). User selected "Fix now, same design pass." Replaced with TestEnumerateUploadTargets (7 new tests: empty dir, first_level_out/qc recursive-inclusion, preproc/concat single-level nii.gz exclusion, sort order, target dict shape) and TestUploadSessionToS3 (5 new tests: empty enumeration, stale sentinel, gap-only upload with sentinel write, upload failure with sentinel skip, post-upload verification failure), following the existing @patch("orchestrator_utils._get_s3_client") mocking convention from tests/test_legacy_routing_migration.py.</action>
    </disposition>

    <disposition test="import collection error: verify_afni_installation" file="tests/test_coverage_gaps.py" classification="obsolete-test">
      <intended_contract>The module's import block should reference only names that currently exist in orchestrator_utils.py.</intended_contract>
      <current_test_claim>from orchestrator_utils import (..., verify_afni_installation, ...) raised ImportError at collection time, masking every other test and defect in the file (including the compress_session_outputs/upload_to_s3 obsolescence and the BIDS TSV bug above) behind Python's first-unresolvable-name import-masking behavior.</current_test_claim>
      <evidence>verify_afni_installation confirmed absent from orchestrator_utils.py via grep.</evidence>
      <action>re-express: removed the stale import; net-new coverage added per the implement-plan handoff once collection was restored.</action>
    </disposition>

    <disposition test="mock_first_level fixture (_make_mock_load_and_validate) contract mismatch, 7 tests: TestHappyPath::test_full_session_nback_and_rest, TestHappyPath::test_process_participant_multi_session, TestHappyPath::test_nback_cue_relabeling_produces_13_conditions, TestConfigGeneration::test_generated_first_level_config_structure, TestConfigGeneration::test_config_paths_are_real_files, TestQCValidation::test_consolidated_qc_with_first_level, TestQCValidation::test_consolidated_qc_failure_detection" file="tests/test_simulated_pipeline.py" classification="product-bug">
      <intended_contract>load_and_validate (fmri_first_level_proc.first_level_config, v3.1.0) returns list[ResolvedConfig], where each ResolvedConfig exposes analysis_type and analysis_name as attributes. orchestrate_first_level.py:841-842 (already updated for this migration) calls cfg.analysis_type / cfg.analysis_name directly on each returned item.</intended_contract>
      <current_test_claim>tests/test_simulated_pipeline.py's shared mock_first_level fixture (_make_mock_load_and_validate, lines 80-99) mocks load_and_validate to return the pre-v3.1.0 tuple shape [(type, namespace, name), ...]; production code's cfg.analysis_type raises AttributeError: 'tuple' object has no attribute 'analysis_type' against this mock.</current_test_claim>
      <evidence>Direct read of the installed fmri_first_level_proc v3.1.0 package (load_and_validate source and ResolvedConfig dataclass fields, confirmed via python introspection): analysis_type and analysis_name are genuine ResolvedConfig fields. Direct read of orchestrate_first_level.py:838-848. Direct read of tests/test_simulated_pipeline.py:80-116. This was not part of the 3 items already dispositioned for this file in the original pre-design ledger (2 stale BIDS column assertions + 1 stale version string literal); it surfaced only when the full suite was run after Cluster F completed.</evidence>
      <action>route-to-implement: surfaced to the user per the Scope Authority doctrine (AskUserQuestion) as a newly-discovered, out-of-original-scope defect. User selected "Route to /implement as a product-bug" rather than fixing in this design pass, despite the defect being entirely in test fixture code. No test edit was made. Routed via action_items below.</action>
    </disposition>
  </failing_test_dispositions>

  <design_phase>
    <tests_created>28</tests_created>
    <tests_modified>13</tests_modified>
    <files_created>
      <file path="tests/test_coverage_gaps.py" test_count="97" coverage_target="verify_afni_installation import removal; enumerate_upload_targets and upload_session_to_s3 net-new coverage (per-file S3 upload contract); format_task_timing BIDS TSV re-expression; net-new coverage per implement-plan handoff" />
      <file path="tests/test_cr_implementation_extended.py" test_count="48" coverage_target="_derive_session_status two-argument re-expression; _process_session dict-vs-list return-shape re-expression; consolidate_session_qc net-new coverage (upstream_status, error_type, error_context, warnings propagation/defaults, fmri_first_level_proc_version detection); process_participant [OK] high-severity-warning summary-line integration test" />
      <file path="tests/test_coverage_gaps_v2.py" test_count="n/a (verified no edit needed this session; prior segment's obsolete-test re-expressions confirmed still valid)" coverage_target="_derive_session_status two-argument re-expression; BIDS column re-expression" />
      <file path="tests/test_motion_and_qc.py" test_count="n/a (edited in an earlier segment of this design pass)" coverage_target="BIDS ONSET-to-onset column re-expression" />
      <file path="tests/test_simulated_pipeline.py" test_count="n/a (edited in an earlier segment for the 3 originally-dispositioned items; the 7th item this session was routed to implement without a test edit)" coverage_target="BIDS column and version-string re-expression" />
      <file path="tests/test_cr_implementation.py" test_count="n/a (edited in an earlier segment of this design pass)" coverage_target="stale version string re-expression (3.1 -> 4.0)" />
    </files_created>
    <design_rationale>Pre-design failures were dispositioned at cluster granularity by shared root cause (BIDS TSV contract, _derive_session_status signature, _process_session return shape, version literals, module obsolescence, import masking) per the Test Design Discipline ledger, approved by the user as a batch. Net-new coverage (S3 upload contract, consolidate_session_qc field propagation/defaults, the [OK] warning-count summary line) was added per the implement-plan handoff and the mid-pass Scope Authority discovery, grounded directly in production source reads rather than inferred behavior.</design_rationale>
  </design_phase>

  <post_design_run>
    <total>334</total>
    <passed>315</passed>
    <failed>7</failed>
    <errors>0</errors>
    <coverage_pct>n/a</coverage_pct>
    <failures>
      <failure test="TestHappyPath::test_full_session_nback_and_rest" file="tests/test_simulated_pipeline.py" line="126">
        <error_type>AttributeError</error_type>
        <message>'tuple' object has no attribute 'analysis_type'</message>
        <traceback>orchestrate_first_level.py:842: AttributeError: 'tuple' object has no attribute 'analysis_type'</traceback>
        <likely_cause>mock_first_level fixture's _make_mock_load_and_validate returns pre-v3.1.0 tuples; see failing_test_dispositions entry above.</likely_cause>
      </failure>
      <failure test="TestHappyPath::test_process_participant_multi_session" file="tests/test_simulated_pipeline.py" line="182">
        <error_type>OrchestratorError</error_type>
        <message>All 2 session(s) failed for sub-MULTI001 (root: 'tuple' object has no attribute 'analysis_type' at orchestrate_first_level.py:842)</message>
        <traceback>orchestrate_first_level.py:842: AttributeError; orchestrate_first_level.py:332: OrchestratorError: All 2 session(s) failed; tests/test_simulated_pipeline.py:243: AssertionError: assert 0 == 1</traceback>
        <likely_cause>Same root cause as above, surfaced through process_participant's per-session exception handling.</likely_cause>
      </failure>
      <failure test="TestHappyPath::test_nback_cue_relabeling_produces_13_conditions" file="tests/test_simulated_pipeline.py" line="222">
        <error_type>AttributeError</error_type>
        <message>'tuple' object has no attribute 'analysis_type'</message>
        <traceback>orchestrate_first_level.py:842: AttributeError: 'tuple' object has no attribute 'analysis_type'</traceback>
        <likely_cause>Same root cause.</likely_cause>
      </failure>
      <failure test="TestConfigGeneration::test_generated_first_level_config_structure" file="tests/test_simulated_pipeline.py" line="335">
        <error_type>AttributeError</error_type>
        <message>'tuple' object has no attribute 'analysis_type'</message>
        <traceback>orchestrate_first_level.py:842: AttributeError: 'tuple' object has no attribute 'analysis_type'</traceback>
        <likely_cause>Same root cause.</likely_cause>
      </failure>
      <failure test="TestConfigGeneration::test_config_paths_are_real_files" file="tests/test_simulated_pipeline.py" line="380">
        <error_type>AttributeError</error_type>
        <message>'tuple' object has no attribute 'analysis_type'</message>
        <traceback>orchestrate_first_level.py:842: AttributeError: 'tuple' object has no attribute 'analysis_type'</traceback>
        <likely_cause>Same root cause.</likely_cause>
      </failure>
      <failure test="TestQCValidation::test_consolidated_qc_with_first_level" file="tests/test_simulated_pipeline.py" line="482">
        <error_type>AttributeError</error_type>
        <message>'tuple' object has no attribute 'analysis_type'</message>
        <traceback>orchestrate_first_level.py:842: AttributeError: 'tuple' object has no attribute 'analysis_type'</traceback>
        <likely_cause>Same root cause.</likely_cause>
      </failure>
      <failure test="TestQCValidation::test_consolidated_qc_failure_detection" file="tests/test_simulated_pipeline.py" line="519">
        <error_type>AttributeError</error_type>
        <message>'tuple' object has no attribute 'analysis_type'</message>
        <traceback>orchestrate_first_level.py:842: AttributeError: 'tuple' object has no attribute 'analysis_type'</traceback>
        <likely_cause>Same root cause.</likely_cause>
      </failure>
    </failures>
  </post_design_run>

  <summary>
    <assertions_preserved_or_strengthened>true</assertions_preserved_or_strengthened>
    <bugs_routed_to_implement>1</bugs_routed_to_implement>
    <recommendation>implement_fixes</recommendation>
  </summary>

  <action_items>
    <item priority="P1" target_mode="implement" description="Re-express tests/test_simulated_pipeline.py's _make_mock_load_and_validate (mock_first_level fixture, lines 80-99) to return a single namespace object per analysis block carrying analysis_type and analysis_name attributes (aliased from the parsed YAML block's type/name keys) plus all other block keys as attributes, matching the real fmri_first_level_proc v3.1.0 ResolvedConfig contract, instead of the current (type, namespace, name) tuple shape. This will resolve 7 currently-failing tests: TestHappyPath::test_full_session_nback_and_rest, TestHappyPath::test_process_participant_multi_session, TestHappyPath::test_nback_cue_relabeling_produces_13_conditions, TestConfigGeneration::test_generated_first_level_config_structure, TestConfigGeneration::test_config_paths_are_real_files, TestQCValidation::test_consolidated_qc_with_first_level, TestQCValidation::test_consolidated_qc_failure_detection. User explicitly elected to route this as a product-bug /implement action item rather than fix it within /test design, despite the defect residing entirely in test fixture code." />
  </action_items>
</test_report>
