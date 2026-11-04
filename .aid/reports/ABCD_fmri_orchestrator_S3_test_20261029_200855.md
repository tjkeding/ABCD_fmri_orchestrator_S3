<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-10-29T20:08:55Z" />

  <pre_design_run>
    <total>454</total>
    <passed>391</passed>
    <failed>63</failed>
    <errors>0</errors>
    <coverage_pct></coverage_pct>
    <failures>
      <failure test="TestLoadOrchestratorConfigExtended::test_valid_config_succeeds (representative of 15)" file="tests/test_coverage_gaps_v2.py" line="431">
        <error_type>OrchestratorError</error_type>
        <message>s3.enabled must be true: this orchestrator reads its inputs from S3 and writes its outputs to S3. Representative of 15 tests whose shared config-builder fixture/helper predates the S3-only build and never included a valid s3 block.</message>
        <traceback>load_orchestrator_config raises before reaching the test's own intended check, because the new s3 required-section/enabled gate runs structurally earlier in validation than most of these tests' target assertions.</traceback>
      </failure>
      <failure test="TestHappyPath::test_full_session_nback_and_rest (representative of 35)" file="tests/test_simulated_pipeline.py" line="63">
        <error_type>KeyError</error_type>
        <message>'bucket'. Representative of 35 full-pipeline integration tests (_process_session/process_participant) whose fixtures predate the removal of local-mode discovery/routing/download and never supplied an s3 config or a mocked S3 client.</message>
        <traceback>orchestrator_utils.py: determine_session_routing/get_session_sentinel and discover_subject_inputs now read s3_config["bucket"] unconditionally; with no s3 client mocked, the real _get_s3_client() call also fails in this environment.</traceback>
      </failure>
      <failure test="TestForceRecompute::test_extract_motion_force_recompute (representative of 4)" file="tests/test_cr_implementation.py" line="237">
        <error_type>TypeError</error_type>
        <message>extract_motion_regressors() got an unexpected keyword argument 'force_recompute'. Representative of 4 direct unit tests passing the now-removed force_recompute kwarg to one of 5 preprocessing functions.</message>
        <traceback>orchestrator_utils.py: the reuse-if-exists caching and its force_recompute parameter were removed from remove_initial_trs_bold, extract_motion_regressors, compute_tissue_mask_regressor, fix_nback_cue_labels, and format_task_timing by the approved S3-only redesign.</traceback>
      </failure>
      <failure test="TestExtractMotionRegressorsV4::test_idempotent (representative of 4)" file="tests/test_motion_and_qc.py" line="141">
        <error_type>AssertionError</error_type>
        <message>mtime_1 == mtime_2 is False. Representative of 4 mtime/idempotent "cache hit" assertions on functions whose reuse-if-exists caching was removed; the function now always recomputes and rewrites its output.</message>
        <traceback>Same root cause as the force_recompute TypeError cluster: the caching contract these tests assert against no longer exists in the implementation.</traceback>
      </failure>
      <failure test="TestOrchConfigProduction::test_s3_enabled_with_v7_templates" file="tests/test_production_configs.py" line="161">
        <error_type>AssertionError</error_type>
        <message>assert '<bucket>' == '__PRODUCER_BUCKET__'. The production config's bucket was filled in with the real approved value (<bucket>) during the v4.0.0/producer build; the test still expected the placeholder.</message>
        <traceback>orch_config_production.yaml line 109: bucket: "<bucket>" (confirmed by direct file read).</traceback>
      </failure>
      <failure test="TestDiscoverSubjectInputsLocal::test_local_mode_mirrors_s3_structure (representative of 4)" file="tests/test_discovery_v4.py" line="513">
        <error_type>OrchestratorError</error_type>
        <message>s3.enabled must be true. Representative of 4 tests that explicitly exercised the now-deleted local-mode discovery code path (study.input_dir mirroring S3 without an s3 config), a feature removed entirely by this build's approved S3-only redesign.</message>
        <traceback>orchestrator_utils.py: discover_subject_inputs no longer has a local-mode branch; it is S3-only.</traceback>
      </failure>
    </failures>
  </pre_design_run>

  <failing_test_dispositions>
    <disposition test="TestLoadOrchestratorConfigV4::test_valid_v4_config_loads" file="tests/test_preprocessing.py" classification="obsolete-test">
      <intended_contract>load_orchestrator_config accepts a complete, valid v4 config and returns it loaded.</intended_contract>
      <current_test_claim>Uses the shared conftest.sample_orchestrator_config fixture, whose s3 block was {"enabled": False}.</current_test_claim>
      <evidence>orchestrator_utils.py required_sections now includes "s3" and enforces s3.enabled is True; tests/conftest.py:251 (pre-fix) set s3.enabled False.</evidence>
      <action>re-express: gave sample_orchestrator_config a complete valid s3 block (test file itself unchanged).</action>
    </disposition>
    <disposition test="TestAbsolutePathRejection::test_absolute_output_and_input_dir_pass" file="tests/test_absolute_paths.py" classification="obsolete-test">
      <intended_contract>Absolute input_dir/output_dir paths are accepted by load_orchestrator_config.</intended_contract>
      <current_test_claim>Uses copy.deepcopy(sample_orchestrator_config), same disabled s3 block as above.</current_test_claim>
      <evidence>Same shared-fixture root cause as the prior entry.</evidence>
      <action>re-express: resolved transitively by the sample_orchestrator_config fixture fix.</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_negative_tr_raises" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>A negative study.TR raises OrchestratorError before any S3 check is relevant.</intended_contract>
      <current_test_claim>_minimal_config() helper omitted the s3 key entirely.</current_test_claim>
      <evidence>required_sections check (orchestrator_utils.py ~line 2876) raises "Config missing required section 's3'" before reaching the TR check.</evidence>
      <action>re-express: added a valid s3 block to _minimal_config().</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_empty_tasks_raises" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>An empty tasks list raises OrchestratorError.</intended_contract>
      <current_test_claim>Same _minimal_config() helper, same missing-s3-key root cause.</current_test_claim>
      <evidence>Same as test_negative_tr_raises.</evidence>
      <action>re-express: resolved by the _minimal_config() fix.</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_invalid_analysis_type_raises" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>An unrecognized analysis type raises OrchestratorError.</intended_contract>
      <current_test_claim>Same _minimal_config() helper.</current_test_claim>
      <evidence>Same as test_negative_tr_raises.</evidence>
      <action>re-express: resolved by the _minimal_config() fix.</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_analysis_task_label_not_in_tasks_raises" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>An analysis referencing a task_label absent from tasks raises OrchestratorError.</intended_contract>
      <current_test_claim>Same _minimal_config() helper.</current_test_claim>
      <evidence>Same as test_negative_tr_raises.</evidence>
      <action>re-express: resolved by the _minimal_config() fix.</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_calc_n_motion_derivs_present_raises" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>A retired calc_n_motion_derivs key raises OrchestratorError.</intended_contract>
      <current_test_claim>Same _minimal_config() helper.</current_test_claim>
      <evidence>Same as test_negative_tr_raises.</evidence>
      <action>re-express: resolved by the _minimal_config() fix.</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_force_recompute_non_bool_raises" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>A non-boolean study.force_recompute raises OrchestratorError.</intended_contract>
      <current_test_claim>Same _minimal_config() helper.</current_test_claim>
      <evidence>Same as test_negative_tr_raises.</evidence>
      <action>re-express: resolved by the _minimal_config() fix.</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_missing_post_id_out_pre_raises" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>An analysis block missing post_id_out_pre raises OrchestratorError.</intended_contract>
      <current_test_claim>Same _minimal_config() helper.</current_test_claim>
      <evidence>Same as test_negative_tr_raises.</evidence>
      <action>re-express: resolved by the _minimal_config() fix.</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_valid_config_succeeds" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>A fully valid config loads without raising.</intended_contract>
      <current_test_claim>Same _minimal_config() helper.</current_test_claim>
      <evidence>Same as test_negative_tr_raises.</evidence>
      <action>re-express: resolved by the _minimal_config() fix.</action>
    </disposition>
    <disposition test="TestLoadOrchestratorConfigExtended::test_missing_post_id_conn_pre_for_conn_raises" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>A *_conn analysis missing post_id_conn_pre raises OrchestratorError.</intended_contract>
      <current_test_claim>Same _minimal_config() helper.</current_test_claim>
      <evidence>Same as test_negative_tr_raises.</evidence>
      <action>re-express: resolved by the _minimal_config() fix.</action>
    </disposition>
    <disposition test="TestForceRecompute::test_config_default_false" file="tests/test_cr_implementation.py" classification="obsolete-test">
      <intended_contract>load_orchestrator_config defaults study.force_recompute to False when omitted.</intended_contract>
      <current_test_claim>_make_valid_config() helper set s3: {"enabled": False}.</current_test_claim>
      <evidence>Confirmed by direct error capture: raised "s3.enabled must be true" instead of exercising the force_recompute default.</evidence>
      <action>re-express: added a valid s3 block to _make_valid_config().</action>
    </disposition>
    <disposition test="TestForceRecompute::test_config_force_recompute_true" file="tests/test_cr_implementation.py" classification="obsolete-test">
      <intended_contract>study.force_recompute: true is preserved through config loading.</intended_contract>
      <current_test_claim>Same _make_valid_config() helper.</current_test_claim>
      <evidence>Same as test_config_default_false.</evidence>
      <action>re-express: resolved by the _make_valid_config() fix.</action>
    </disposition>
    <disposition test="TestForceRecompute::test_config_invalid_force_recompute_raises" file="tests/test_cr_implementation.py" classification="obsolete-test">
      <intended_contract>A non-boolean study.force_recompute raises OrchestratorError with message "force_recompute must be a boolean".</intended_contract>
      <current_test_claim>Same _make_valid_config() helper; test asserted the force_recompute message but got the s3 message instead.</current_test_claim>
      <evidence>Confirmed exact mismatch via pytest -rA capture: expected regex "force_recompute must be a boolean", actual "s3.enabled must be true...".</evidence>
      <action>re-express: resolved by the _make_valid_config() fix; assertion text unchanged.</action>
    </disposition>
    <disposition test="TestTaskLabelWhitelistConfig::test_valid_task_labels_accepted_at_config_load" file="tests/test_cr_implementation_extended.py" classification="obsolete-test">
      <intended_contract>Valid task labels ("rest", "nback") pass config-level whitelist validation.</intended_contract>
      <current_test_claim>_make_config() helper set s3: {"enabled": False}.</current_test_claim>
      <evidence>The sibling invalid-label tests in the same class pass because the task-label-whitelist check (orchestrator_utils.py ~line 2940) runs before the s3 check (~line 3020); the valid-label test reaches the s3 gate because it has no earlier error to raise.</evidence>
      <action>re-express: added a valid s3 block to _make_config().</action>
    </disposition>
    <disposition test="TestConfigGeneration::test_generated_first_level_config_structure" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>_process_session generates a well-formed first-level config YAML for the session.</intended_contract>
      <current_test_claim>Uses sim_session_dir + sim_orchestrator_config, whose s3 block was {"enabled": False}; _process_session's routing/discovery/download/upload are now unconditional S3 calls.</current_test_claim>
      <evidence>orchestrator_utils.py: determine_session_routing and discover_subject_inputs read s3_config["bucket"] unconditionally; no s3 client was mocked.</evidence>
      <action>re-express: gave sim_orchestrator_config a valid s3 block and added a new mock_s3_client fixture (tests/mock_s3.py-backed) to the test signature.</action>
    </disposition>
    <disposition test="TestConfigGeneration::test_config_paths_are_real_files" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>Every path written into the generated first-level config resolves to a real file on disk.</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config fixtures.</current_test_claim>
      <evidence>Same as test_generated_first_level_config_structure.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestEdgeCases::test_dry_run_no_processing" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>dry_run=True performs discovery/routing but no actual processing.</intended_contract>
      <current_test_claim>Uses tmp_test_dir + sim_orchestrator_config directly.</current_test_claim>
      <evidence>Same root cause as test_generated_first_level_config_structure.</evidence>
      <action>re-express: same fixture fix plus mock_s3_client added to the test signature.</action>
    </disposition>
    <disposition test="TestEdgeCases::test_multiple_nss_trs" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>Multiple non-steady-state TRs are trimmed correctly across runs.</intended_contract>
      <current_test_claim>Same tmp_test_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestEdgeCases::test_single_run_collected_per_run" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>A single-run task still produces correctly per-run-collected paths.</intended_contract>
      <current_test_claim>Same tmp_test_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestEdgeCases::test_zero_nss_trs" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>n_nss=0 is handled without trimming any volumes.</intended_contract>
      <current_test_claim>Same tmp_test_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestHappyPath::test_full_session_nback_and_rest" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>A full nback+rest session processes end to end with correct per-analysis outcomes.</intended_contract>
      <current_test_claim>Uses sim_session_dir + sim_orchestrator_config.</current_test_claim>
      <evidence>Same root cause as test_generated_first_level_config_structure.</evidence>
      <action>re-express: same fixture fix plus mock_s3_client added to the test signature.</action>
    </disposition>
    <disposition test="TestHappyPath::test_nback_cue_relabeling_produces_13_conditions" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>N-back cue relabeling produces exactly 13 conditions.</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestHappyPath::test_per_run_timing_onsets_are_run_local" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>Per-run timing onsets are run-local, not session-global.</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestHappyPath::test_process_participant_multi_session" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>process_participant processes every discovered session for a subject.</intended_contract>
      <current_test_claim>Uses tmp_test_dir + sim_orchestrator_config.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix plus mock_s3_client added to the test signature.</action>
    </disposition>
    <disposition test="TestQCValidation::test_consolidated_qc_failure_detection_non_terminal" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>A non-terminal QC failure is recorded in the consolidated QC JSON without stopping the session.</intended_contract>
      <current_test_claim>Uses sim_session_dir + sim_orchestrator_config (monkeypatch variant, no mock_first_level).</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix plus mock_s3_client added to the test signature.</action>
    </disposition>
    <disposition test="TestQCValidation::test_consolidated_qc_json_structure" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>The consolidated session QC JSON has the documented schema.</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestQCValidation::test_consolidated_qc_with_first_level" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>First-level QC fields are merged into the consolidated session QC JSON.</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestSkipPartialFailure::test_all_runs_missing_confounds_gate_raises" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>A session where every run is missing confounds raises at the input-completeness gate.</intended_contract>
      <current_test_claim>Uses tmp_test_dir + sim_orchestrator_config.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix plus mock_s3_client added to the test signature.</action>
    </disposition>
    <disposition test="TestSkipPartialFailure::test_missing_events_excludes_run" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>A run missing its events file is excluded, not fatal to the session.</intended_contract>
      <current_test_claim>Same tmp_test_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestSkipPartialFailure::test_session_filter_nonexistent" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>A --session filter naming a session absent from discovery raises a specific error.</intended_contract>
      <current_test_claim>Same tmp_test_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fixture fix.</action>
    </disposition>
    <disposition test="TestNonContiguousRunLabels::test_middle_run_excluded_survivors_are_non_contiguous" file="tests/test_run_labels.py" classification="obsolete-test">
      <intended_contract>When a middle run is excluded, surviving run_labels and all per-run path lists stay index-aligned.</intended_contract>
      <current_test_claim>Uses tmp_test_dir + sim_orchestrator_config via the local _rest_only_config() wrapper.</current_test_claim>
      <evidence>Same root cause as the test_simulated_pipeline.py cluster.</evidence>
      <action>re-express: added mock_s3_client to the test signature (sim_orchestrator_config fixed via conftest.py).</action>
    </disposition>
    <disposition test="TestNonContiguousRunLabels::test_no_exclusions_run_labels_are_contiguous" file="tests/test_run_labels.py" classification="obsolete-test">
      <intended_contract>With no exclusions, run_labels are contiguous starting at 1.</intended_contract>
      <current_test_claim>Same _rest_only_config()/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestNonContiguousRunLabels::test_first_run_excluded_survivors_start_above_one" file="tests/test_run_labels.py" classification="obsolete-test">
      <intended_contract>Excluding the first run leaves run_labels starting above 1 (survivors are not renumbered from 1).</intended_contract>
      <current_test_claim>Same _rest_only_config()/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestEventsMissingConditionsIntegration::test_condition_absent_from_every_surviving_run_skips_analysis" file="tests/test_events_validation.py" classification="obsolete-test">
      <intended_contract>An analysis whose required condition never appears in any surviving run is skipped with reason events_missing_conditions; unrelated analyses still run.</intended_contract>
      <current_test_claim>Uses tmp_test_dir + sim_orchestrator_config.</current_test_claim>
      <evidence>Same root cause as the test_simulated_pipeline.py cluster.</evidence>
      <action>re-express: added mock_s3_client to the test signature.</action>
    </disposition>
    <disposition test="TestProcessSessionExitCategory::test_all_success_session_returns_exit_complete" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>A session where every analysis succeeds returns exit category 0 (complete).</intended_contract>
      <current_test_claim>Uses sim_session_dir + sim_orchestrator_config; previously relied on local-mode's unconditional skip of routing/discovery/download/upload under s3.enabled=False.</current_test_claim>
      <evidence>orchestrate_first_level.py: the routing/discovery/download/upload calls in _process_session are now unconditional (no s3_cfg.get("enabled", False) guard), so this test needed real S3 mediation for the first time.</evidence>
      <action>re-express: added mock_s3_client to the test signature.</action>
    </disposition>
    <disposition test="TestProcessSessionExitCategory::test_model_error_failure_returns_exit_known_cause" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>A terminal ModelError in one analysis yields exit category 2 (known cause).</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestProcessSessionExitCategory::test_runtime_error_failure_returns_exit_unexplained" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>A non-ModelError exception from dispatch yields exit category 1 (unexplained).</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestProcessSessionExitCategory::test_producer_fd_mismatch_returns_exit_unexplained" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>A producer/recomputed FD mismatch excludes the run with reason producer_fd_inconsistent and yields exit category 1.</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestProcessSessionExitCategory::test_unexpected_error_run_exclusion_forces_unexplained_despite_clean_analyses" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>An unexpected_error run exclusion forces exit category 1 even when every analysis that ran succeeded.</intended_contract>
      <current_test_claim>Same sim_session_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestSentinelSuppressionByExitCategory::test_unexplained_session_calls_upload_with_write_sentinel_false" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>A session exiting unexplained calls upload_session_to_s3 with write_sentinel=False.</intended_contract>
      <current_test_claim>Pre-computed discovery via a local helper calling discover_subject_inputs with s3={"enabled": False}, then pre-supplied the result to _process_session and monkeypatched download_session_inputs to return it unchanged.</current_test_claim>
      <evidence>discover_subject_inputs is now S3-only (the {"enabled": False} call itself fails); additionally, this test was not in the initial 63 failures but was found during post-design verification to pass for the wrong reason: a second-order bug (below) made every variant of this helper fail the same way regardless of the test's own simulated RuntimeError.</evidence>
      <action>re-express: removed the pre-computed discovery/download bypass entirely; the test now lets _process_session perform real S3-mediated discovery and download through the mocked S3 client, exercising the intended RuntimeError-in-task_act mechanism.</action>
    </disposition>
    <disposition test="TestSentinelSuppressionByExitCategory::test_known_cause_session_calls_upload_with_write_sentinel_true" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>A session exiting with a known-cause (ModelError) failure calls upload_session_to_s3 with write_sentinel=True.</intended_contract>
      <current_test_claim>Same pre-computed discovery/download-bypass pattern as the sibling test above.</current_test_claim>
      <evidence>Surfaced by the post-design verification run (not the pre-design baseline): discover_subject_inputs now returns S3 keys, not local paths; download_session_inputs is the function that converts S3 keys to local paths via os.path.join(input_dir, key), and this test's monkeypatch bypassed that conversion, so downstream preprocessing tried to open a relative S3 key as a local file and raised FileNotFoundError, which the pipeline classifies as an unexpected error, masking the intended ModelError path and producing EXIT_FAILED_UNEXPLAINED instead of EXIT_FAILED_KNOWN_CAUSE.</evidence>
      <action>re-express: removed the pre-computed discovery/download bypass; the test now relies on the mocked S3 client for real discovery and download, which correctly materializes local paths.</action>
    </disposition>
    <disposition test="TestSentinelSuppressionByExitCategory::test_complete_session_calls_upload_with_write_sentinel_true" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>A fully successful session calls upload_session_to_s3 with write_sentinel=True.</intended_contract>
      <current_test_claim>Same pre-computed discovery/download-bypass pattern.</current_test_claim>
      <evidence>Same second-order bug as test_known_cause_session_calls_upload_with_write_sentinel_true, confirmed by the post-design verification run (EXIT_FAILED_UNEXPLAINED instead of EXIT_COMPLETE).</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestProcessParticipantExitCode::test_dry_run_returns_exit_complete" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>process_participant with dry_run=True returns exit code 0.</intended_contract>
      <current_test_claim>Uses tmp_test_dir + sim_orchestrator_config.</current_test_claim>
      <evidence>Same root cause as TestProcessSessionExitCategory.</evidence>
      <action>re-express: added mock_s3_client to the test signature.</action>
    </disposition>
    <disposition test="TestProcessParticipantExitCode::test_single_clean_session_returns_zero" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>A single clean session returns exit code 0.</intended_contract>
      <current_test_claim>Same tmp_test_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestProcessParticipantExitCode::test_multi_session_most_severe_code_wins" file="tests/test_exit_classification.py" classification="obsolete-test">
      <intended_contract>Across multiple sessions, process_participant returns the most severe exit code.</intended_contract>
      <current_test_claim>Same tmp_test_dir/sim_orchestrator_config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestProcessParticipantSessionResults::test_session_results_structure" file="tests/test_cr_implementation_extended.py" classification="obsolete-test">
      <intended_contract>process_participant's session_results dict has the documented {status, analyses} structure.</intended_contract>
      <current_test_claim>Builds its own inline config with s3: {"enabled": False} and tmp_dir (not a shared fixture).</current_test_claim>
      <evidence>Same structural root cause as the test_simulated_pipeline.py cluster, confirmed independently since this test does not use any shared sim_* fixture.</evidence>
      <action>re-express: gave the inline config a valid s3 block and added an in-test fake S3 client (make_fake_s3_client rooted at tmp_dir) via monkeypatch.</action>
    </disposition>
    <disposition test="TestProcessParticipantSessionResults::test_session_returns_outcomes_with_analyses" file="tests/test_cr_implementation_extended.py" classification="obsolete-test">
      <intended_contract>When analyses are defined and dispatched, outcomes contain per-analysis status/error/wall_time_seconds.</intended_contract>
      <current_test_claim>Same inline-config pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestProcessParticipantSessionResults::test_high_severity_warning_logged_in_summary" file="tests/test_cr_implementation_extended.py" classification="obsolete-test">
      <intended_contract>A high-severity upstream warning on a successful analysis appends a warning-count suffix to the session's [OK] summary line.</intended_contract>
      <current_test_claim>Same inline-config pattern, calling process_participant directly.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>re-express: same fix.</action>
    </disposition>
    <disposition test="TestStructuredRunLossWarning::test_warning_format_in_simulated_pipeline" file="tests/test_cr_implementation.py" classification="obsolete-test">
      <intended_contract>When a run fails preprocessing, the session log reports the surviving-run count and that downstream analyses are affected.</intended_contract>
      <current_test_claim>Builds its own inline config with s3: {"enabled": False} and tmp_dir.</current_test_claim>
      <evidence>Same structural root cause as the test_simulated_pipeline.py cluster.</evidence>
      <action>re-express: gave the inline config a valid s3 block and added an in-test fake S3 client via monkeypatch.</action>
    </disposition>
    <disposition test="TestForceRecompute::test_extract_motion_force_recompute" file="tests/test_cr_implementation.py" classification="obsolete-test">
      <intended_contract>extract_motion_regressors(force_recompute=True) bypasses its output cache and overwrites.</intended_contract>
      <current_test_claim>Calls extract_motion_regressors with a force_recompute kwarg that no longer exists on the function.</current_test_claim>
      <evidence>orchestrator_utils.py: the reuse-if-exists caching and force_recompute parameter were removed from extract_motion_regressors by the approved S3-only redesign (the per-function cache could only ever be exercised in local mode, which is now removed; S3 mode always wipes the session output directory first).</evidence>
      <action>delete: the premise of the test (a cache to bypass) no longer exists; there is no equivalent behavior to re-express against.</action>
    </disposition>
    <disposition test="TestForceRecomputeNonAFNI::test_format_task_timing_force_recompute" file="tests/test_cr_implementation_extended.py" classification="obsolete-test">
      <intended_contract>format_task_timing(force_recompute=True) bypasses its output cache and overwrites.</intended_contract>
      <current_test_claim>Calls format_task_timing with the now-removed force_recompute kwarg.</current_test_claim>
      <evidence>Same removal as test_extract_motion_force_recompute.</evidence>
      <action>delete: same reasoning; this was the only non-cache-hit member of its class besides the sibling below, so the whole TestForceRecomputeNonAFNI class was removed.</action>
    </disposition>
    <disposition test="TestForceRecomputeNonAFNI::test_fix_nback_cue_labels_force_recompute" file="tests/test_cr_implementation_extended.py" classification="obsolete-test">
      <intended_contract>fix_nback_cue_labels(force_recompute=True) bypasses its output cache and overwrites.</intended_contract>
      <current_test_claim>Calls fix_nback_cue_labels with the now-removed force_recompute kwarg.</current_test_claim>
      <evidence>Same removal as test_extract_motion_force_recompute.</evidence>
      <action>delete: same reasoning.</action>
    </disposition>
    <disposition test="TestComputeTissueMaskRegressor::test_force_recompute_overwrites" file="tests/test_tissue_mask_regressors.py" classification="obsolete-test">
      <intended_contract>compute_tissue_mask_regressor(force_recompute=True) bypasses its output cache and overwrites.</intended_contract>
      <current_test_claim>Calls compute_tissue_mask_regressor with the now-removed force_recompute kwarg.</current_test_claim>
      <evidence>Same removal as test_extract_motion_force_recompute.</evidence>
      <action>delete: same reasoning.</action>
    </disposition>
    <disposition test="TestFixNbackCueLabels::test_idempotent_if_output_exists" file="tests/test_coverage_gaps.py" classification="obsolete-test">
      <intended_contract>A pre-existing output file is returned without recomputation (cache hit).</intended_contract>
      <current_test_claim>Writes a sentinel "cached" string to the output path, calls fix_nback_cue_labels, and asserts the output is unchanged.</current_test_claim>
      <evidence>fix_nback_cue_labels no longer has a reuse-if-exists branch; it always recomputes and overwrites.</evidence>
      <action>delete: the cache-hit contract no longer exists.</action>
    </disposition>
    <disposition test="TestFixNbackCueLabels::test_cache_hit" file="tests/test_coverage_gaps_v2.py" classification="obsolete-test">
      <intended_contract>A pre-existing output file is returned without recomputation (cache hit).</intended_contract>
      <current_test_claim>Same pattern as test_idempotent_if_output_exists.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>delete: same reasoning. Note: a different, same-named test_cache_hit in TestFormatTaskTiming in the same file was NOT in this failure set (it currently passes, coincidentally, because its specific input has nothing droppable); this was surfaced to the user as a drive-by observation and the user elected to leave it alone.</action>
    </disposition>
    <disposition test="TestExtractMotionRegressorsV4::test_idempotent" file="tests/test_motion_and_qc.py" classification="obsolete-test">
      <intended_contract>A pre-existing motion output file is returned without recomputation (cache hit).</intended_contract>
      <current_test_claim>Compares mtimes across two calls and asserts they are equal.</current_test_claim>
      <evidence>extract_motion_regressors no longer has a reuse-if-exists branch.</evidence>
      <action>delete: same reasoning.</action>
    </disposition>
    <disposition test="TestComputeTissueMaskRegressor::test_idempotent_cache_hit_returns_none_voxel_count" file="tests/test_tissue_mask_regressors.py" classification="obsolete-test">
      <intended_contract>A cache hit returns (out_path, None) for n_voxels without recounting.</intended_contract>
      <current_test_claim>Compares mtimes and asserts n_voxels is None on the second call.</current_test_claim>
      <evidence>compute_tissue_mask_regressor no longer has a cache-hit branch; it always recomputes and returns a real voxel count.</evidence>
      <action>delete: same reasoning; also updated the module docstring and function signature description that documented the retired cache-hit/force_recompute contract.</action>
    </disposition>
    <disposition test="TestCompletionGate::test_local_mode_with_manifest_attaches_metadata" file="tests/test_discovery_v4.py" classification="obsolete-test">
      <intended_contract>In local mode, a session with a producer completion manifest has that manifest's metadata attached to the discovery result.</intended_contract>
      <current_test_claim>Calls discover_subject_inputs with s3={"enabled": False} and study.input_dir mirroring the S3 layout on local disk.</current_test_claim>
      <evidence>discover_subject_inputs has no local-mode branch; local mode (the orchestrator operating without S3 at all) was removed entirely by user-approved decision ("Remove it" in this session's scope discussion).</evidence>
      <action>delete: the feature under test no longer exists in any form; this and the 3 other local-mode tests in this file were removed, while the file's 7 S3-based TestCompletionGate members and its TestDiscoverSubjectInputsS3/TestRealProducerLayout classes were left untouched.</action>
    </disposition>
    <disposition test="TestCompletionGate::test_local_mode_without_manifest_returns_empty" file="tests/test_discovery_v4.py" classification="obsolete-test">
      <intended_contract>In local mode, a session without a producer completion manifest is skipped (empty result).</intended_contract>
      <current_test_claim>Same local-mode discover_subject_inputs call pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>delete: same reasoning.</action>
    </disposition>
    <disposition test="TestDiscoverSubjectInputsLocal::test_local_mode_mirrors_s3_structure" file="tests/test_discovery_v4.py" classification="obsolete-test">
      <intended_contract>Local-mode discovery parses the same per-file layout as S3 mode and produces an identical nested structure.</intended_contract>
      <current_test_claim>Same local-mode discover_subject_inputs call pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>delete: same reasoning; the whole TestDiscoverSubjectInputsLocal class (2 members) was removed.</action>
    </disposition>
    <disposition test="TestDiscoverSubjectInputsLocal::test_local_mode_multiple_sessions" file="tests/test_discovery_v4.py" classification="obsolete-test">
      <intended_contract>Local-mode discovery finds every session present under input_dir in a single call.</intended_contract>
      <current_test_claim>Same local-mode discover_subject_inputs call pattern.</current_test_claim>
      <evidence>Same root cause.</evidence>
      <action>delete: same reasoning.</action>
    </disposition>
    <disposition test="TestOrchConfigProduction::test_s3_enabled_with_v7_templates" file="tests/test_production_configs.py" classification="obsolete-test">
      <intended_contract>The production <bucket> orchestrator config's s3 section has the correct enabled flag, bucket, and prefix templates.</intended_contract>
      <current_test_claim>assert orch_config["s3"]["bucket"] == "__PRODUCER_BUCKET__".</current_test_claim>
      <evidence>orch_config_production.yaml line 109: bucket: "<bucket>" (the placeholder was filled in with the real approved production value during the v4.0.0/producer build; this was already flagged as a known action item in the preceding /document report).</evidence>
      <action>re-express: changed the expected literal to "<bucket>" to match the current production config.</action>
    </disposition>
  </failing_test_dispositions>

  <design_phase>
    <tests_created>0</tests_created>
    <tests_modified>51</tests_modified>
    <files_created>
      <file path="tests/mock_s3.py" test_count="0" coverage_target="Shared fake S3 client factory (make_fake_s3_client) backing get_paginator/list_objects_v2, head_object, get_object, download_file, upload_file, put_object, and delete_object from a local directory, so S3-only orchestrator code paths (discovery, routing, download, upload, sentinel handling) can be exercised without a real S3 connection." />
    </files_created>
    <design_rationale>
      All 63 pre-design failures were caused by this session's approved S3-only and no-cache redesign (prior /implement build), not by product defects. 51 tests (15 config-fixture tests plus 35 full-pipeline integration tests plus 1 bucket-literal test) were re-expressed to match the new contract: config-fixture tests got a complete valid s3 block in their shared config-builder helpers; full-pipeline integration tests were wired to the new mock_s3_client fixture (tests/conftest.py), which patches orchestrator_utils._get_s3_client with a fake client backed by the same tmp_test_dir the test's mock session filesystem already uses, so discovery/routing/download/upload all resolve against real files without a network call. 3 of the 35 (test_exit_classification.py::TestSentinelSuppressionByExitCategory) additionally had their own pre-existing ad hoc S3 mocking refactored: their original pattern pre-computed discovery under a local-mode shim and bypassed download_session_inputs with a pass-through lambda, which (once local mode was removed) silently fed raw S3 keys to downstream code as if they were local paths. This was not just a dispatch-layer change but a correctness fix to the tests' own setup, surfaced during the post-design verification run (first pass: 2 explicit failures; closer inspection showed a third sibling test was passing for the wrong reason, masking its own intended failure mechanism). All three were corrected by removing the bypass entirely and letting _process_session perform real S3-mediated discovery and download through the mocked client.
      12 tests were deleted, not re-expressed, because the behavior they asserted was removed by the same approved redesign and has no successor to assert against: 4 tested the force_recompute keyword on 5 preprocessing functions (the per-function reuse-if-exists caching that kwarg controlled was removed because it could only ever be exercised in local mode, which no longer exists); 4 tested mtime/cache-hit idempotency on the same now-cacheless functions; 4 tested local-mode discovery directly, a feature removed in full.
      No assertion was weakened or removed to force a pass; every surviving assertion is unchanged, and the 12 deletions target tests whose premise (a cache, or local mode itself) no longer exists in the implementation.
    </design_rationale>
  </design_phase>

  <post_design_run>
    <total>442</total>
    <passed>442</passed>
    <failed>0</failed>
    <errors>0</errors>
    <coverage_pct></coverage_pct>
    <failures>
    </failures>
  </post_design_run>

  <summary>
    <assertions_preserved_or_strengthened>true</assertions_preserved_or_strengthened>
    <bugs_routed_to_implement>0</bugs_routed_to_implement>
    <recommendation>proceed_to_document</recommendation>
  </summary>

  <action_items>
  </action_items>
</test_report>
