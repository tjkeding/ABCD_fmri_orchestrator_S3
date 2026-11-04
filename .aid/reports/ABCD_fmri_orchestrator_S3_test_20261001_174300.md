<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-10-01T17:43:00Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_build_20261001_164600.md</spec_ref>

  <pre_design_run>
    <total>307</total>
    <passed>290</passed>
    <failed>5</failed>
    <errors>0</errors>
    <skipped>12</skipped>
    <note>Baseline run after the C1-C19 harmonization build. Regressed from a prior 322 passed/0 failed baseline.</note>
  </pre_design_run>

  <failing_test_dispositions>
    <disposition test="TestProcessParticipantSessionResults::test_session_returns_outcomes_with_analyses" file="tests/test_cr_implementation_extended.py" status="aligned">
      <intended_contract>The orchestrator builds a first-level config using plural path keys (scan_paths/motion_paths/task_timing_paths) for task_act/task_conn analyses, per the harmonization build's target fmri_first_level_proc contract.</intended_contract>
      <current_test_claim>_process_session succeeds for a session with an nback_act analysis using plural-key fixtures.</current_test_claim>
      <evidence>Orchestrator code (orchestrator_utils.py build_first_level_config, lines 3018-3023) correctly emits plural keys. The installed fmri-first-level-proc in the ABCD_fmri_orchestrator_S3 conda env is 3.1.0 (pip show), whose first_level_config.py still requires singular scan_path/motion_path/task_timing_path for task_act/task_conn (confirmed by direct GitHub inspection of the tjkeding/fmri_first_level_proc main branch, commit 0ad04451, the v3.1.0 tag and the repository's only branch). No v3.2.0 tag, branch, or PR exists anywhere in that repository; the plural-path contract for task_act/task_conn has not been implemented upstream at all.</evidence>
      <action>No test edit. This is not an installed-package staleness issue resolvable by an environment upgrade; it is a cross-project sequencing dependency. The orchestrator code is correct per its intended (not-yet-implemented) upstream contract. Resolution requires implementing the plural-path contract for task_act/task_conn in the fmri_first_level_proc project itself, in a separate session, before this test can pass.</action>
    </disposition>

    <disposition test="TestProcessParticipantSessionResults::test_high_severity_warning_logged_in_summary" file="tests/test_cr_implementation_extended.py" status="aligned">
      <intended_contract>Same as above; this test additionally exercises the high-severity-warning logging path, which is gated behind successful config validation.</intended_contract>
      <current_test_claim>process_participant logs a high-severity warning in the session summary for a flagged analysis.</current_test_claim>
      <evidence>Identical root cause and evidence as the disposition above: ConfigError: [nback_act] Missing required path key 'scan_path'/'motion_path', raised by the installed fmri_first_level_proc 3.1.0, surfaced through process_participant -> _process_session -> load_and_validate before the warning-logging code path is reached.</evidence>
      <action>No test edit. Same cross-project resolution path as above.</action>
    </disposition>

    <disposition test="TestValidateProcTemplateRefactored::test_valid_config_with_global_passes" file="tests/test_preprocessing.py" status="obsolete-test">
      <intended_contract>A proc template analysis's extraction block must use stat_buckets (list of stat-type strings) rather than the legacy extract/extract_labels/extract_stat triplet, per the orchestrator's C11 validation added in this build.</intended_contract>
      <current_test_claim>validate_proc_template raises no exception for the proc_template_no_censor fixture.</current_test_claim>
      <evidence>orchestrator_utils.py:3414-3422 rejects the legacy key set {extract, extract_labels, extract_stat}. example_proc_config.yaml confirms the stat_buckets replacement schema. The fixture's nback_act.extraction block (tests/test_preprocessing.py, formerly lines 89-95) still used the legacy triplet.</evidence>
      <action>Re-expressed the fixture's extraction block as {"stat_buckets": ["Zstat"], "extract_out_file_pre": "placeholder_Shen368", "extract_resids": False}, preserving semantic intent (a single z-stat bucket under the same output prefix) without altering the assertion. Also applied identically to the structurally duplicate nback_act.extraction block in the sibling proc_template_with_task_conn fixture (used by the golden-file test below), to avoid baking stale legacy keys into the regenerated golden file. Verified post-fix: test passes.</action>
    </disposition>

    <disposition test="TestGoldenFileRefactored::test_deterministic_config_output" file="tests/test_preprocessing.py" status="obsolete-test">
      <intended_contract>build_first_level_config output is deterministic and matches a stored reference reflecting the current (C9) plural-paths, forced-remove_previous contract.</intended_contract>
      <current_test_claim>Generated config equals the stored golden YAML.</current_test_claim>
      <evidence>Direct read of build_first_level_config (orchestrator_utils.py:2915-3058) confirmed the function's current output is correct per the C9 spec. The stored golden file predated C9 and still had the flat/singular-key layout. The test's own self-healing branch (pytest.skip with an explicit "delete the golden file and re-run" message) is the designed remedy for exactly this situation.</evidence>
      <action>Deleted tests/golden_config_refactored.yaml (user-approved destructive operation) and re-ran the test to regenerate it via the test's built-in mechanism. Manually inspected the regenerated YAML: confirms plural paths keys, remove_previous: true on every analysis, and the stat_buckets extraction schema from the disposition above. Verified post-fix: test passes against the regenerated golden file on a second run.</action>
    </disposition>

    <disposition test="TestHappyPath::test_per_run_timing_onsets_are_run_local" file="tests/test_simulated_pipeline.py" status="obsolete-test">
      <intended_contract>Per-run timing TSVs carry run-local onsets strictly less than that run's own retained duration, reflecting the C8 removal of cross-run concatenation.</intended_contract>
      <current_test_claim>onsets.max() &lt; retained_trs * TR for each per-run nback timing file (159.2s for this fixture).</current_test_claim>
      <evidence>Direct read of format_task_timing (orchestrator_utils.py:2037-2134) confirmed it applies only a fixed NSS-removal offset with no cross-run accumulation, ruling out concatenation as the mechanism. Direct execution of the shared mock fixture create_mock_nback_events_tsv (tests/mock_data_utils.py) showed it generates a fixed 8-block structure spanning 270.0s (max onset 255.0s) regardless of the simulated run's BOLD duration (160s for n_trs=200, TR=0.8s), independent of any orchestrator behavior. The fixture predates the shift to shorter, strictly per-run simulated scan durations.</evidence>
      <action>Parametrized create_mock_nback_events_tsv to accept run_num/n_runs and partition its fixed 8 blocks evenly across runs (4 blocks per run for n_nback_runs=2), updating the single call site in create_mock_session_filesystem accordingly. Hand-verified the 4/4 block split preserves full coverage of all 13 conditions via the union across runs, consistent with the sibling test_nback_cue_relabeling_produces_13_conditions assertion (which already takes the union across per-run timing files). The two unrelated direct call sites in tests/test_cr_implementation_extended.py (duration-agnostic mtime-overwrite tests) are unaffected via the backward-compatible default (run_num=None preserves the full 8-block structure). No assertion weakened. Verified post-fix: test passes.</action>
    </disposition>
  </failing_test_dispositions>

  <design_phase>
    <file_modified path="tests/test_preprocessing.py" description="Re-expressed the nback_act extraction block (both proc_template_no_censor and proc_template_with_task_conn fixtures) from the legacy extract/extract_labels/extract_stat triplet to stat_buckets." />
    <file_modified path="tests/mock_data_utils.py" description="Parametrized create_mock_nback_events_tsv with run_num/n_runs to partition its fixed 8-block event structure evenly across simulated runs; updated the single call site in create_mock_session_filesystem." />
    <file_deleted_and_regenerated path="tests/golden_config_refactored.yaml" description="Deleted the stale pre-C9 golden file; regenerated via the test's own self-healing mechanism and manually verified against the current build_first_level_config contract." />
  </design_phase>

  <post_design_run>
    <total>307</total>
    <passed>293</passed>
    <failed>2</failed>
    <errors>0</errors>
    <skipped>12</skipped>
    <note>Remaining 2 failures are the aligned-disposition, upstream-contract-blocked tests (see dispositions above); no further test-design action applies to them.</note>
  </post_design_run>

  <summary>
    <assertions_preserved_or_strengthened>true</assertions_preserved_or_strengthened>
    <bugs_routed_to_implement>0</bugs_routed_to_implement>
    <overall_assessment>The ABCD_fmri_orchestrator_S3 codebase and its test suite are internally consistent and correctly harmonized with the fmri_first_level_proc &gt;= 3.2.0 contract as specified in the implement build. The 2 remaining failures are not defects in this codebase: they correctly detect that the target upstream contract (plural scan_paths/motion_paths/task_timing_paths for task_act/task_conn) does not yet exist in fmri_first_level_proc at any released or unreleased version (verified directly against the GitHub repository: only v3.1.0 is tagged, only one branch exists, and no PR implements this contract).</overall_assessment>
  </summary>

  <action_items>
    <item priority="P0" target_mode="implement" project="fmri_first_level_proc" description="Implement the plural-path contract (scan_paths/motion_paths/task_timing_paths) for task_act and task_conn analysis types in fmri_first_level_proc's first_level_config.py, matching the pattern rest_conn already uses, and tag a v3.2.0 release. This requires a separate session scoped to the fmri_first_level_proc repository/working directory." />
  </action_items>

  <known_issues>
    <issue severity="P0" description="ABCD_fmri_orchestrator_S3's C1-C19 harmonization build (MIN_UPSTREAM_VERSION=3.2.0, environment.yaml pinned to @v3.2.0) targets a fmri_first_level_proc contract that has not been implemented upstream. 2 tests (test_session_returns_outcomes_with_analyses, test_high_severity_warning_logged_in_summary) will continue to fail until that upstream work is completed and the conda env is upgraded accordingly." />
  </known_issues>
</test_report>
