<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-10-16T16:53:00Z" sequence="design -> environment pre-flight -> run_suite -> fix -> run_suite (followup) -> fix -> run_suite (final)" />

  <active_spec>
    <file path="ABCD_fmri_orchestrator_S3_implement_build_20261016_160934.md" role="the v4.0.0/producer build this invocation validates; 14/14 changes applied, 0 skipped, 0 blocked" />
    <file path="ABCD_fmri_orchestrator_S3_test_20261016_130307.md" role="prior /test design report; its post-design 13-root-cause punch list is what the referenced build was meant to resolve" />
  </active_spec>

  <pre_design_run skipped="true">
    <note>The user's invocation explicitly sequenced this session as design first, environment-permission grant second, run_suite third ("design the new tests... Then, I will grant you permission for the conda environment and you should run the suite"). A pre-design baseline was not run: the installed environment at design-start (fmri_first_level_proc 3.1.0) could not even import orchestrator_utils.py (unguarded `from fmri_first_level_proc import names as upstream_names`, confirmed directly via ModuleNotFoundError), so any baseline run would have been a universal, uninformative collection failure across all 21 pre-existing test files. This is a deliberate, user-directed deviation from the default Full Sequence, not an omission.</note>
  </pre_design_run>

  <design_phase>
    <pre_write_approval_gate>User approved the full consolidated plan (one new file, ~29 tests; one modified file, two defect fixes) before any write; no redirects.</pre_write_approval_gate>

    <files_written category="new">
      <file path="tests/test_exit_classification.py" note="Three-way exit classification scheme (classify_session_exit, most_severe_exit, EXIT_COMPLETE/EXIT_FAILED_UNEXPLAINED/EXIT_FAILED_KNOWN_CAUSE). 29 tests across 5 classes: TestClassifySessionExit (12, pure unit, every branch and precedence order), TestMostSevereExit (6, pure unit), TestProcessSessionExitCategory (5, integration via _process_session: all-success, ModelError, RuntimeError, unexpected_error exclusion despite otherwise-clean analyses, SKIP-routed), TestSentinelSuppressionByExitCategory (3, integration, mocked S3: verifies _process_session computes write_sentinel from the exit category and passes it to upload_session_to_s3), TestProcessParticipantExitCode (3, integration: dry-run, single clean session, multi-session most-severe-wins)." />
    </files_written>

    <files_modified category="pre_existing_defect_fix">
      <file path="tests/test_production_configs.py" note="Fix 1: orch_config fixture changed from scope=module to function scope (it depends on the function-scoped mock_logger fixture; pytest ScopeMismatch otherwise). Fix 2: TestProcConfigProductionUpstreamClosedSchema's test now builds a stubbed copy of the real proc_config_production.yaml (new _stub_file/_proc_config_with_stub_paths helpers substitute real empty files for global.template_path and every per-run path-list entry) before calling the real upstream load_and_validate, since the deliverable's orchestrator-filled placeholders are intentionally non-resolvable and the loader checks file existence." />
    </files_modified>
  </design_phase>

  <environment_preflight>
    <gap_identified package="fmri_first_level_proc" installed="3.1.0" required=">=4.0.0,&lt;5.0.0">
      <evidence>orchestrator_utils.py now hard-imports fmri_first_level_proc.names (locked C9 decision); 3.1.0 has no names module. Confirmed directly: ModuleNotFoundError: No module named 'fmri_first_level_proc.names'.</evidence>
      <resolution approved="true" command="<conda_env>/bin/python3 -m pip install --force-reinstall --no-deps &quot;<local_path>/fmri-first-level-proc&quot;" result="fmri_first_level_proc 3.1.0 -> 4.0.0 (installed from the local v4.0.0-build workspace checkout, which has a pyproject.toml but no upstream v4.0.0 git tag yet); numpy 2.4.2, pandas 3.0.1, nibabel 5.4.2, python 3.14.3 already satisfied the pins and needed no change." />
    </gap_identified>
    <gap_identified package="scipy" installed="absent" required="any (transitive, nibabel.processing)">
      <evidence>TestComputeTissueMaskRegressorRealAFNI's real-AFNI acceptance test computes its own independent ground truth via nibabel.processing.resample_from_to, which raised TripWireError: We need package scipy.ndimage... No module named 'scipy'. Production code (orchestrator_utils.py) does not use nibabel.processing and has no scipy dependency; this is a test-only gap.</evidence>
      <resolution approved="true" command="<conda_env>/bin/python3 -m pip install scipy" result="scipy 1.18.1 installed cleanly, no conflicts with the existing numpy 2.4.2 pin." />
    </gap_identified>
  </environment_preflight>

  <post_design_run>
    <dispatch nonce="session-4" receipt_verified="true" />
    <result collected="415" passed="396" failed="18" errors="0" skipped="1" />
  </post_design_run>

  <failing_test_dispositions phase="post_design" note="Every one of the 18 initial failures traced to a test-infrastructure gap, not a defect in the /implement build (orchestrator_utils.py / orchestrate_first_level.py). This is the expected first-run outcome: this was the first execution of the test suite against the real fmri_first_level_proc 4.0.0 package and real AFNI, rather than the assumed/documented contract the pre-existing tests (from the prior /test design session, before /implement) were authored against. assertions_preserved_or_strengthened: true. bugs_routed_to_implement: 0.">
    <entry disposition="obsolete-test">
      <summary>conftest.py's shared mock FirstLevelResult(...) construction omits the real dataclass's required intermediate_files field (a fmri_first_level_proc 4.0.0 field with no default), raising TypeError inside every mocked "successful" dispatch.</summary>
      <intended_contract>The mock stands in faithfully for the real FirstLevelResult so integration tests exercise the orchestrator's own logic, not the mock's construction.</intended_contract>
      <current_test_claim>_mock_run_fn returns FirstLevelResult(status="success", output_dir=..., warnings=[], qc_summary=..., output_files=[...]) with no intermediate_files argument.</current_test_claim>
      <evidence>TypeError: FirstLevelResult.__init__() missing 1 required positional argument: 'intermediate_files'.</evidence>
      <scope>10 tests: test_events_validation.py::TestEventsMissingConditionsIntegration::test_condition_absent_from_every_surviving_run_skips_analysis; test_exit_classification.py::TestProcessSessionExitCategory::test_all_success_session_returns_exit_complete, test_model_error_failure_returns_exit_known_cause, test_unexpected_error_run_exclusion_forces_unexplained_despite_clean_analyses; test_exit_classification.py::TestSentinelSuppressionByExitCategory::test_known_cause_session_calls_upload_with_write_sentinel_true, test_complete_session_calls_upload_with_write_sentinel_true; test_exit_classification.py::TestProcessParticipantExitCode::test_single_clean_session_returns_zero; test_simulated_pipeline.py::TestHappyPath::test_full_session_nback_and_rest; test_simulated_pipeline.py::TestQCValidation::test_consolidated_qc_with_first_level, test_consolidated_qc_failure_detection_non_terminal.</scope>
      <action>Fixed directly in tests/conftest.py: added intermediate_files=[] to the FirstLevelResult construction. No assertion in any of the 10 affected tests was changed.</action>
    </entry>
    <entry disposition="obsolete-test">
      <summary>Three older fixtures use a minimal global: {num_cores: 1} proc template with no tr key; the newly-added (locked, correct) "global.tr is required" validator check now fires before the specific behavior each test targets.</summary>
      <intended_contract>validate_proc_template raises OrchestratorError naming the specific defect under test (unmatched analysis name; censor_path; censor_paths) once its other preconditions are satisfied.</intended_contract>
      <current_test_claim>pytest.raises(OrchestratorError, match="no matching entry" / "censor_path" / "censor_paths") against a template missing global.tr.</current_test_claim>
      <evidence>Actual raised message was "global.tr is required in the proc template (it is never injected by the orchestrator)." in all three cases, confirming the new check (added per the locked T8/C9 decision) runs earlier than the behavior each test is designed to isolate.</evidence>
      <scope>test_coverage_gaps.py::TestValidateProcTemplateAdditional::test_analysis_name_not_in_template_raises; test_preprocessing.py::TestValidateProcTemplateV4::test_censor_path_in_template_raises, test_censor_paths_in_template_raises.</scope>
      <action>Added "tr": 0.8 to each fixture's global block (matching sample_orchestrator_config's study.TR). No assertion changed; each test still asserts exactly the same OrchestratorError match pattern it always did.</action>
    </entry>
    <entry disposition="obsolete-test">
      <summary>Two fixtures in test_cr_implementation_extended.py declare an analysis block with an extraction section but global.template_path: None; the real upstream fmri_first_level_proc 4.0.0 schema now requires template_path whenever extraction is present.</summary>
      <intended_contract>_process_session/process_participant correctly record per-analysis outcomes and summary log lines when first-level analyses are dispatched and succeed.</intended_contract>
      <current_test_claim>A minimal proc_template with template_path: None and an extraction block, expected to pass through load_and_validate (mocked DISPATCH) successfully.</current_test_claim>
      <evidence>ConfigError: [nback_act] Extraction features require global.template_path to be set., raised by the real upstream first_level_config.py (DISPATCH is mocked, load_and_validate is not).</evidence>
      <scope>test_cr_implementation_extended.py::TestProcessParticipantSessionResults::test_session_returns_outcomes_with_analyses, test_high_severity_warning_logged_in_summary.</scope>
      <action>Each fixture now creates a real empty stub file under its own tmp_dir and sets global.template_path to it. No assertion changed.</action>
    </entry>
    <entry disposition="obsolete-test">
      <summary>The closed-schema test's own stub-path helper (written earlier in this same invocation) stubs global.template_path and every per-run path-list entry, but not each analysis block's out_dir; the real upstream loader tries to create /__ORCHESTRATOR_FILLED__ as a directory and hits a read-only root filesystem.</summary>
      <intended_contract>The closed-schema test checks schema acceptance of proc_config_production.yaml's structure, not whether a literal placeholder path happens to be writable.</intended_contract>
      <current_test_claim>_proc_config_with_stub_paths stubs template_path and every paths[*] list entry but leaves block["out_dir"] as the literal deliverable placeholder.</current_test_claim>
      <evidence>ConfigError: [nback_act] Cannot create out_dir '/__ORCHESTRATOR_FILLED__': [Errno 30] Read-only file system.</evidence>
      <scope>test_production_configs.py::TestProcConfigProductionUpstreamClosedSchema::test_loads_under_real_upstream_closed_schema.</scope>
      <action>Extended _proc_config_with_stub_paths to also set each block's out_dir to a real, already-created writable directory under tmp_dir. No assertion changed; this is a direct continuation of the already-approved stub-path fix, completing one stub category it had missed.</action>
    </entry>
    <entry disposition="obsolete-test">
      <summary>test_production_configs.py's own _referenced_labels helper (from the prior /test design session) strips only single-digit coefficient tokens ("0".."9"), not multi-digit ones; two of the 29 locked contrasts (ave_zback, ave_tback) use "0.25*" coefficients, which the [A-Za-z0-9_]+ tokenizer splits into "0" and "25" since "." is not in the character class, and "25" leaks through as a false undeclared label.</summary>
      <intended_contract>Every symbolic term in every contrast function must be one of the analysis's own declared cond_labels/cond_beta_labels; no undeclared label may appear.</intended_contract>
      <current_test_claim>_referenced_labels(expr) = tokens(expr) - {"0",...,"9"}, assumed sufficient to strip all numeric coefficients.</current_test_claim>
      <evidence>AssertionError: [nback_act] contrast '0.25*0_back_negface+0.25*0_back_posface+...' references undeclared label(s): {'25'}. The "0.25*" coefficients are verbatim from the locked C8e contrast table (contrasts 5 and 6, ave_zback/ave_tback), not an orchestrator-introduced value; proc_config_production.yaml is correct as written.</evidence>
      <scope>test_production_configs.py::TestProcConfigProductionStructuralSelfConsistency::test_contrasts_reference_only_declared_labels.</scope>
      <action>Changed _referenced_labels to strip any purely-numeric token (token.isdigit()) rather than only single digits 0-9. This is a strengthening/correction of the helper's own parsing, not a weakening of the assertion: the "no undeclared labels" check remains exactly as strict, and no real condition label (all contain a letter or underscore) is ever purely numeric.</action>
    </entry>
    <entry disposition="aligned">
      <summary>scipy is not installed; nibabel.processing.resample_from_to, used only by the real-AFNI acceptance test's own independent ground-truth computation (not by any production code path), requires scipy.ndimage.</summary>
      <intended_contract>compute_tissue_mask_regressor's AFNI 3dresample/3dcalc/3dmaskave pipeline output matches an independent ground truth within numerical tolerance, verified against real AFNI when available.</intended_contract>
      <current_test_claim>TestComputeTissueMaskRegressorRealAFNI::test_resample_intersect_average_matches_numpy_ground_truth, skipped unless real AFNI binaries are present (they are, on this machine).</current_test_claim>
      <evidence>nibabel.tripwire.TripWireError: We need package scipy.ndimage for these functions, but import scipy.ndimage raised No module named 'scipy'. Confirmed as a test-environment gap, not a regression: the test's assertion logic is unchanged and correct; it simply could not execute.</evidence>
      <scope>test_tissue_mask_regressors.py::TestComputeTissueMaskRegressorRealAFNI::test_resample_intersect_average_matches_numpy_ground_truth.</scope>
      <action>No assertion edit. Resolved via environment installation (scipy 1.18.1), approved separately per the Environment Pre-Flight doctrine; see environment_preflight above.</action>
    </entry>
  </failing_test_dispositions>

  <followup_run_1>
    <dispatch nonce="session-5" receipt_verified="true" note="Re-dispatched (not a new design pass) after the 5 test-infrastructure fixes above and the scipy install, to confirm the corrected state." />
    <result collected="415" passed="414" failed="1" errors="0" skipped="0" />
  </followup_run_1>

  <mid_run_bug_found_and_fixed>
    The one remaining failure (test_exit_classification.py::TestProcessSessionExitCategory::test_unexpected_error_run_exclusion_forces_unexplained_despite_clean_analyses) was a defect in test content newly written in THIS invocation, not a pre-existing test-infrastructure gap or a product defect. The test's monkeypatch crashed any run whose output prefix contained "run-02", intending to exclude only the rest task's second run; it inadvertently also crashed the nback task's second run. Real ABCD n-back block/cue timing distributes the 8 stimulus x nback-level conditions across a session's two runs rather than duplicating all 8 in each run, so with only nback run 1 surviving, nback_act/nback_conn were correctly skipped (required conditions missing) rather than succeeding as the test expected. This is a test-authoring defect introduced during this session's own design phase, not a product-contract issue, so it was corrected immediately (same precedent as the prior /test design session's mid-run fixture-visibility fix): the monkeypatch now matches specifically on "task-rest_run-02", leaving both nback runs untouched. No assertion was weakened; the test still asserts the same exit_category, excluded_reasons, and nback_act success postconditions, now against a scenario that actually produces them.
  </mid_run_bug_found_and_fixed>

  <followup_run_2>
    <dispatch nonce="session-6" receipt_verified="true" note="Re-dispatched (not a new design pass) after the monkeypatch-scope fix above, to confirm the final corrected state." />
    <result collected="415" passed="415" failed="0" errors="0" skipped="0" />
  </followup_run_2>

  <summary>
    This invocation validated the v4.0.0/producer /implement build (14/14 changes applied) against a newly-installed real environment (fmri_first_level_proc 3.1.0 -> 4.0.0 from the local v4.0.0-build workspace; scipy added) rather than the mocked/assumed contract the pre-existing test suite had been authored against. Per the user's explicit sequencing, design (the new three-way exit classification test file, 29 tests, plus the two pre-existing test_production_configs.py defect fixes) ran before any environment change or suite execution; a pre-design baseline was skipped as uninformative given the known import-time break in the then-current environment.

    The first real run_suite (415 collected, 396 passed, 18 failed) found zero defects in the /implement build itself. All 18 failures were test-infrastructure gaps: a shared mock fixture missing a newly-required real-dataclass field (10 tests), three older fixtures missing a newly-and-correctly-required global.tr key (3 tests), two fixtures missing a newly-and-correctly-required global.template_path (2 tests), one incomplete stub-path helper from this invocation's own earlier fix (1 test), one test helper's incomplete numeric-token stripping exposed by the locked contrast table's own "0.25*" coefficients (1 test), and one missing environment package needed only by a test's own ground-truth computation (1 test). All five test-content fixes (disposition obsolete-test) plus the environment gap (disposition aligned) were applied, followed by one re-dispatched (not re-designed) verification run, which surfaced one further defect: a monkeypatch in the brand-new exit-classification test file matched too broadly and crashed an unintended run. That defect was corrected immediately per the same "test-authoring defect found mid-run, fixed without a disposition-ledger entry" precedent used in the prior /test design session. A final re-dispatched verification run confirms 415/415 passing, 0 failed, 0 errors, 0 skipped.

    assertions_preserved_or_strengthened: true (every edit either added a missing fixture field/stub, or made a test helper's parsing logic more correct; no assertion was weakened, removed, or made a tautology). bugs_routed_to_implement: 0 -- this is itself informative: it is direct evidence that the prior /implement build correctly resolved all 13 root causes from the prior /test design report's punch list, since re-running the full locked-decision-encoding suite against the real upstream package and real AFNI surfaced no product regressions, only artifacts of having never previously run the suite against the real (rather than assumed) v4.0.0 contract.
  </summary>

  <action_items>
    <item priority="P2" target_mode="none" description="environment.yaml's pip git URL still pins fmri_first_level_proc@v3.2.0 (locked exclusion from the prior /implement build, A3: waits for the upstream v4.0.0 git tag to exist). The conda environment itself was separately upgraded to 4.0.0 in this session by installing directly from the local v4.0.0-build workspace checkout (not via environment.yaml), which has no upstream git tag yet. No action until the upstream tag is published." />
    <item priority="P2" target_mode="none" description="environment.yaml does not declare scipy, which this session installed directly into the conda env. Production code has no scipy dependency (only one test's own nibabel.processing ground-truth computation does); if the scipy-backed real-AFNI acceptance test is to keep passing on a freshly-built environment in the future, environment.yaml should add scipy as a test-only dependency. This is a documentation-pin gap, not a blocking defect, and touches a file outside tests/, so it is deferred as an action item rather than edited in this test:design-scoped session." />
  </action_items>

  <next_steps>No further action required from /implement or /test for this build; the full suite is green under the real v4.0.0 environment. The two P2 documentation-pin items above may be picked up whenever environment.yaml is next touched.</next_steps>
</test_report>
