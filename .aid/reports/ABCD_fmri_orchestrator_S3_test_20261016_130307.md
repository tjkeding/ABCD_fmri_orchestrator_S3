<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-10-16T13:03:07Z" sequence="run_suite -> design -> run_suite" />

  <active_spec>
    <file path="ABCD_fmri_orchestrator_S3_brainstorm_20261015_150913.md" role="orchestrator-side locked decisions (T1-T16, C1-C9, C8e)" />
    <file path="ABCD_fmri_orchestrator_S3_brainstorm_upstream_20261015_150913.md" role="upstream fmri-first-level-proc locked decisions (U1-U5, D1-D3); not this orchestrator's /implement scope" />
  </active_spec>

  <pre_design_run>
    <dispatch nonce="session-1" receipt_verified="true" />
    <result collected="307" passed="293" failed="2" errors="0" skipped="12" />
    <note>Matches the 2026-10-01 documented baseline exactly, confirming the working tree at design-start was the uncommitted, superseded v3.2.0-target harmonization build (C1-C19), never committed.</note>
  </pre_design_run>

  <failing_test_dispositions phase="pre_design">
    <entry disposition="aligned">
      <test>tests/test_cr_implementation_extended.py::TestProcessParticipantSessionResults::test_session_returns_outcomes_with_analyses</test>
      <test>tests/test_cr_implementation_extended.py::TestProcessParticipantSessionResults::test_high_severity_warning_logged_in_summary</test>
      <intended_contract>Mode A per-run task_act/task_conn paths use the plural motion_paths key (list form), per the v3.2.0+ and v4 contracts alike.</intended_contract>
      <current_test_claim>Both tests build a proc_template with plural motion_paths and dispatch through the real installed fmri_first_level_proc.load_and_validate.</current_test_claim>
      <evidence>The installed conda env is fmri_first_level_proc v3.1.0 (confirmed via direct `__version__` inspection), which accepts only the singular motion_path key for task_act; the separate v4.0.0-build checkout confirms both forms are schema-valid under v4. first_level_config.py:170 raises "Missing required path key 'motion_path'".</evidence>
      <action>No assertion edit. Environment/upstream-version gap, not a test or orchestrator defect; resolves once the conda env is rebuilt against the tagged upstream v4.0.0 release (T1). Re-expressed in test design with this disposition documented in the class docstring (both tests still present, fixtures modernized to full v4 shape).</action>
    </entry>
  </failing_test_dispositions>

  <design_phase>
    <pre_write_approval_gate>User approved the full 21-file consolidated plan before any write (per SKILL.md gate); no redirects.</pre_write_approval_gate>

    <files_written category="shared_infrastructure_rewrite">
      <file path="tests/conftest.py" note="v4 fixtures (sample_proc_template/sample_orchestrator_config/sample_processed_files_task/_rest with run_labels and real PATH_KEYS, sim_* producer fixtures, NBACK_COND_LABELS/NBACK_BETA_LABELS constants); shared mock_first_level fixture and its _make_mock_dispatch/_make_mock_load_and_validate helpers promoted here mid-run_suite after a fixture-scope bug was found (see note below)." />
      <file path="tests/mock_data_utils.py" note="producer per-file mock filesystem (derivatives/sub-*/ses-*/{func,anat}); real ABCD n-back block/cue/trial timing structure; nback_trs_needed sizing." />
      <file path="tests/mock_subprocess.py" note="added _handle_3dmaskave for the C5-MASK tissue-regressor procedure." />
    </files_written>

    <files_deleted>
      <file path="tests/test_legacy_routing_migration.py" note="C1-REV retires tarball/legacy-layout routing entirely; required a session-bound destructive-Bash grant scoped to this one file, explicitly approved by the user before deletion." />
    </files_deleted>

    <files_written category="re_expressed">
      <file path="tests/test_preprocessing.py" note="build_first_level_config/validate_proc_template/load_orchestrator_config v4 contract (T2, T4, T6, T8, C5); golden-file test renamed to golden_config_v4.yaml (old golden_config_refactored.yaml orphaned, not deleted)." />
      <file path="tests/test_motion_and_qc.py" note="motion source from confounds TSV (C3); compute_preproc_qc without registration/anat_mask_path, with producer_fd_mean/max (C7, T12); processed_files shape (T2, C5)." />
      <file path="tests/test_simulated_pipeline.py" note="full _process_session/process_participant v4 integration coverage: input_dir, cue relabeling to target_0back, C6 drop-vs-clip, all_terminal." />
      <file path="tests/test_cr_implementation.py" note="rotation-unit-check and force_recompute re-based on confounds TSV; consolidate_session_qc baseline structure with the new preproc_excluded_runs parameter; run-loss-warning mechanism updated since apply_brain_mask is removed (C7)." />
      <file path="tests/test_cr_implementation_extended.py" note="apply_brain_mask/apply_smoothing force_recompute coverage retired; NaN-imputation edge cases retired (T13); the two environment-gap tests re-expressed with v4 fixtures and the disposition documented in-file." />
      <file path="tests/test_coverage_gaps.py" note="new TestRetiredSymbolsRemoved regression guard (11 symbols, C7/T5/C5/C1-REV); fix_nback_cue_labels/format_task_timing re-expressed (C8e-NBACK/C6); enumerate_upload_targets re-expressed for the explicit first-level-file-list contract and concat/ removal (T11); verify_upstream_version extended with max_version (T1); TestRealDataMotion/Events/QC retired." />
      <file path="tests/test_coverage_gaps_v2.py" note="extract_tissue_signals and apply_smoothing coverage retired; NaN-imputation edge case retired; load_orchestrator_config v4 fixture base." />
    </files_written>

    <files_written category="new">
      <file path="tests/test_tissue_mask_regressors.py" note="NEW function compute_tissue_mask_regressor (C5-MASK); mocked unit coverage plus one real-AFNI acceptance test (skip if AFNI absent) cross-checked against an independent nibabel.processing ground truth." />
      <file path="tests/test_input_gate.py" note="NEW function check_subject_input_completeness (C1-GATE); aggregated-error listing, tissue-mask carve-out logic, presence-only semantics." />
      <file path="tests/test_restart_safety.py" note="NEW function is_outcome_terminal (C9); determine_session_routing migrate-path removal (C7); upload_session_to_s3 write_sentinel parameter and remote-key sweep (T11)." />
      <file path="tests/test_tissue_mask_regressors.py" note="(listed above)" />
      <file path="tests/test_events_validation.py" note="NEW functions validate_task_timing and check_required_conditions_present (T7-A/B); includes one events_missing_conditions terminal-skip integration test." />
      <file path="tests/test_run_labels.py" note="T2 non-contiguous run_labels (1,3,4 and 2,3 survivor patterns) via rest runs, independent of the task events/conditions machinery." />
      <file path="tests/test_absolute_paths.py" note="T6 relative-path rejection for study.output_dir/input_dir and global.template_path; chdir-removal source regression guard." />
      <file path="tests/test_qc_schema_v4.py" note="NEW function get_peak_memory_usage (C8d); consolidate_session_qc v4 schema fields (preproc_excluded_runs, all_terminal, n_analyses_skipped, peak_memory_*, per-analysis run_labels/terminal) and session.flags/any_flag (T10)." />
      <file path="tests/test_motion_non_finite.py" note="T13 motion non-finite exclusion, including the derivative-leak-across-trim-boundary edge case; OrchestratorError.context mechanism (C4), shared infrastructure for T13/C4/C5-MASK." />
      <file path="tests/test_production_configs.py" note="<bucket> config acceptance (C8e); module-level skip until the /implement deliverables exist; validates against the orchestrator's own validators plus the real upstream closed-schema loader (skipped on version gap)." />
    </files_written>

    <mid_run_bug_found_and_fixed>
      tests/test_run_labels.py referenced a `mock_first_level` pytest fixture that existed only locally in tests/test_simulated_pipeline.py, invisible to other files. This was a test-authoring defect introduced during design, not a product-contract issue, so it was corrected immediately (not deferred to a disposition ledger entry): the fixture and its two helper functions were promoted to tests/conftest.py, and the now-duplicate local definitions in test_simulated_pipeline.py were replaced with an import. Both files re-verified to compile and collect cleanly after the fix.
    </mid_run_bug_found_and_fixed>
  </design_phase>

  <post_design_run>
    <dispatch nonce="session-2" receipt_verified="true" note="Initial dispatch, before the mock_first_level fixture fix." />
    <result collected="316" passed="150" failed="140" errors="9" skipped="17" />
    <followup_verification nonce="FINAL-VERIFY-session-3" note="Re-run directly (not re-dispatched; same environment) after the fixture fix, to confirm the corrected final state." />
    <result collected="316" passed="150" failed="143" errors="6" skipped="17" />
  </post_design_run>

  <failing_test_dispositions phase="post_design" note="Every item below is disposition product-bug: each new/re-expressed test correctly encodes a locked decision from the brainstorm report; the implementation has simply not been built yet. This is the expected, designed-for outcome of test-first /test design preceding /implement (see Single-Pass Discipline note in summary). No assertion in any of these was weakened to pass.">
    <entry disposition="product-bug" locked_decision="T6">
      <summary>load_orchestrator_config still requires the legacy study.fmriprep_dir key; v4 configs use study.input_dir.</summary>
      <evidence>orchestrator_utils.py:3152, `OrchestratorError: Config study section missing required key 'fmriprep_dir'`.</evidence>
      <scope>~30 tests across test_preprocessing.py, test_absolute_paths.py, test_cr_implementation.py, test_cr_implementation_extended.py, test_coverage_gaps_v2.py, test_simulated_pipeline.py.</scope>
      <action target_mode="implement">Rename the required study key to input_dir; raise OrchestratorError naming input_dir if the legacy fmriprep_dir key is present.</action>
    </entry>
    <entry disposition="product-bug" locked_decision="T8">
      <summary>build_first_level_config still reads orch_analysis['fd_threshold'] to inject into the proc-config block; v4 orchestrator analysis configs never carry fd_threshold/censor_prev_tr (proc-config authority).</summary>
      <evidence>orchestrator_utils.py:2987, `KeyError: 'fd_threshold'`.</evidence>
      <scope>~15 tests: TestBuildFirstLevelConfigV4, TestGoldenFileV4, TestBuildFirstLevelConfigAdditional, TestBuildFirstLevelConfigEdgeCases.</scope>
      <action target_mode="implement">Remove the fd_threshold/censor_prev_tr read-and-inject lines entirely; remove the forced remove_previous=True override; make global.tr a consistency check only (raise if missing, never inject); fill CSF_paths/WM_paths/GS_paths/run_labels per T2/T4/T8.</action>
    </entry>
    <entry disposition="product-bug" locked_decision="C7, T5, C5, C1-REV">
      <summary>11 retired symbols are still importable from orchestrator_utils: apply_brain_mask, compute_registration_quality, apply_smoothing, extract_tissue_signals, discover_session_files, extract_session_archive, discover_local_mmps_files, download_session_data, discover_available_sessions, migrate_session_from_archive, LegacyArchiveCorruptError.</summary>
      <scope>tests/test_coverage_gaps.py::TestRetiredSymbolsRemoved (11 parametrized cases).</scope>
      <action target_mode="implement">Delete all 11 symbols and every call site, per the respective locked decision (C7: brain-mask/registration/archive-migration removal; T5: smoothing removal; C5: tissue-signal extraction superseded by compute_tissue_mask_regressor; C1-REV: tarball/mmps_mproc/legacy-S3 discovery removal).</action>
    </entry>
    <entry disposition="product-bug" locked_decision="C5-MASK, C1-REV, C1-GATE, C8d, C9, T7-A/B">
      <summary>6 new functions specified during /test design do not exist yet, so the 6 files that import them fail to collect at all (ImportError, not a test failure).</summary>
      <scope>
        tests/test_tissue_mask_regressors.py (compute_tissue_mask_regressor, C5-MASK)
        tests/test_discovery_v4.py (discover_subject_inputs, C1-REV)
        tests/test_input_gate.py (check_subject_input_completeness, C1-GATE)
        tests/test_qc_schema_v4.py (get_peak_memory_usage, C8d)
        tests/test_restart_safety.py (is_outcome_terminal, C9)
        tests/test_events_validation.py (validate_task_timing and check_required_conditions_present, T7-A/B)
      </scope>
      <action target_mode="implement">Implement all 6 new functions with the exact signatures documented in each test file's module docstring (locked during /test design since no prior implementation existed to anchor a signature).</action>
    </entry>
    <entry disposition="product-bug" locked_decision="C4">
      <summary>OrchestratorError is still a bare Exception subclass with no optional context kwarg/attribute.</summary>
      <evidence>tests/test_motion_non_finite.py: `getattr(OrchestratorError('plain message'), 'context', 'MISSING')` returns the sentinel, not None.</evidence>
      <scope>4 TestOrchestratorErrorContext tests, cascading into 6 TestMotionNonFiniteHandling tests and every other new test that asserts on `.context`.</scope>
      <action target_mode="implement">Add an optional `context=None` kwarg to OrchestratorError.__init__, storing it as `self.context`, mirroring upstream's FmriFirstLevelError (confirm no existing exception class already does this before adding, per standing user feedback).</action>
    </entry>
    <entry disposition="product-bug" locked_decision="T1">
      <summary>verify_upstream_version has no max_version parameter; T1 requires rejecting upstream >= 5.0.0.</summary>
      <evidence>`TypeError: verify_upstream_version() got an unexpected keyword argument 'max_version'`.</evidence>
      <scope>4 TestVerifyUpstreamVersion tests (test_coverage_gaps.py).</scope>
      <action target_mode="implement">Add max_version parameter (default MAX_UPSTREAM_VERSION = "5.0.0" module constant); raise OrchestratorError when installed >= max_version.</action>
    </entry>
    <entry disposition="product-bug" locked_decision="T11">
      <summary>enumerate_upload_targets still takes 3 positional arguments (blind recursive walk of first_level_out/); v4 requires a 4th parameter carrying the explicit list of expected first-level file paths, and removal of the concat/ branch.</summary>
      <evidence>`TypeError: enumerate_upload_targets() takes 3 positional arguments but 4 were given`.</evidence>
      <scope>9 TestEnumerateUploadTargets tests.</scope>
      <action target_mode="implement">Add the first_level_file_paths parameter; replace the first_level_out/ recursive walk with this explicit list; delete the concat/ branch; add the rendered session config YAML to the upload set.</action>
    </entry>
    <entry disposition="product-bug" locked_decision="T9">
      <summary>consolidate_session_qc still takes 8 positional arguments; v4 requires a 9th (preproc_excluded_runs), plus new output fields (all_terminal, n_analyses_skipped, peak_memory_bytes/source, per-analysis run_labels/terminal, session.flags/any_flag).</summary>
      <evidence>`TypeError: consolidate_session_qc() takes 8 positional arguments but 9 were given`.</evidence>
      <scope>3 TestConsolidateSessionQC tests, 12 TestConsolidateSessionQCExtended tests, cascading into 3 TestProcessParticipantSessionResults tests and all of test_qc_schema_v4.py's non-skipped-by-ImportError assertions (file itself is a separate collection error, see above).</scope>
      <action target_mode="implement">Add the preproc_excluded_runs parameter and every new output field per T9/T10, call get_peak_memory_usage internally, and derive session.flags per the five T10 codes.</action>
    </entry>
    <entry disposition="product-bug" locked_decision="T12, C7">
      <summary>compute_preproc_qc still has the anat_mask_path parameter and a 'registration' output key (both retired, C7), and does not yet compute producer_fd_mean/producer_fd_max from the confounds framewise_displacement column (T12).</summary>
      <scope>TestComputePreprocQCV4::test_phase1_has_required_keys, test_no_motion_or_registration_keys_in_output, test_no_anat_mask_path_parameter, test_producer_fd_populated_from_confounds, test_producer_fd_respects_n_remove (test_motion_and_qc.py).</scope>
      <action target_mode="implement">Remove anat_mask_path and the entire registration-quality block; add producer_fd_mean/producer_fd_max computed from confounds_df, ignoring NaN, over retained (post-n_remove) frames.</action>
    </entry>
    <entry disposition="product-bug" locked_decision="C6">
      <summary>format_task_timing still returns a 2-tuple (path, n_dropped) with hard-drop-on-negative-onset semantics; C6 requires a 4-tuple (path, n_dropped, n_clipped, clipped_seconds) with drop-only-if-fully-elapsed and clip-if-straddling semantics.</summary>
      <evidence>`ValueError: not enough values to unpack (expected 4, got 2)`.</evidence>
      <scope>test_simulated_pipeline.py::test_timing_onset_adjustment_drops_only_fully_elapsed_events and every other call site in the new/re-expressed suite that unpacks 4 values (test_coverage_gaps.py::TestFormatTaskTiming, test_coverage_gaps_v2.py::TestFormatTaskTiming).</scope>
      <action target_mode="implement">Implement the drop-vs-clip logic exactly as specified in C6 and each test file's inline comments (shifted end &lt;= 0 -&gt; drop; shifted onset &lt; 0 &lt; shifted end -&gt; clip to onset 0 with reduced duration; track n_dropped, n_clipped, clipped_seconds).</action>
    </entry>
    <entry disposition="product-bug" locked_decision="C8e-NBACK">
      <summary>fix_nback_cue_labels still relabels 0-back cues to the bare stimulus name (e.g. "posface"); C8e-NBACK requires the pooled label "target_0back" for every 0-back cue regardless of the following block's stimulus.</summary>
      <evidence>`AssertionError: assert 'posface' == 'target_0back'`.</evidence>
      <scope>3 tests in test_coverage_gaps.py::TestFixNbackCueLabels, plus the cue-relabeling and condition-count assertions in test_simulated_pipeline.py.</scope>
      <action target_mode="implement">Single-line change: the `level == "0"` branch (orchestrator_utils.py ~2019) writes the literal "target_0back" instead of the bare `condition` variable.</action>
    </entry>
    <entry disposition="product-bug" locked_decision="T6">
      <summary>orchestrate_first_level.py still contains an os.chdir call; T6 requires its removal entirely.</summary>
      <evidence>Textual source scan found "chdir" present.</evidence>
      <scope>test_absolute_paths.py::TestChdirRemoved::test_no_chdir_in_orchestrate_first_level_source.</scope>
      <action target_mode="implement">Remove the os.chdir wrapper and its comment at both call sites (~817-821, ~912-913).</action>
    </entry>
    <entry disposition="product-bug" locked_decision="C5, T4">
      <summary>validate_proc_template does not yet reject the singular CSF_path/WM_path keys in task_act/task_conn blocks (only the Mode A plural CSF_paths/WM_paths form is valid).</summary>
      <evidence>`Failed: DID NOT RAISE <class 'orchestrator_utils.OrchestratorError'>`.</evidence>
      <scope>test_preprocessing.py::TestValidateProcTemplateV4::test_singular_csf_path_in_task_block_raises and test_singular_wm_path_in_task_block_raises.</scope>
      <action target_mode="implement">Add the singular-key rejection check for task_act/task_conn blocks' paths dict.</action>
    </entry>
  </failing_test_dispositions>

  <summary>
    Pre-design baseline (293/307 passed, 2 upstream-environment-gap failures) confirmed the working tree was the superseded, never-committed v3.2.0 harmonization build. The design phase produced a 21-file test suite (3 shared-infrastructure rewrites, 1 deletion, 7 re-expressed files, 10 new files) encoding every testable locked decision from the v4.0.0/producer orchestrator brainstorm report (T1-T16, C1-C9, C8e), following the approved consolidated plan with no redirects. One test-authoring defect (a fixture visible only within one other file) was found and corrected during the post-design run, not carried into the disposition ledger as a product issue.

    The post-design run (316 collected, 150 passed, 143 failed, 6 collection errors, 17 skipped) is the expected and designed-for outcome: /test design ran deliberately before /implement, so every new or re-expressed assertion encodes the FUTURE (post-implementation) contract against the CURRENT (pre-implementation) code. All 143 failures and 6 collection errors trace to exactly 13 root causes, enumerated above with disposition product-bug and a target_mode="implement" action — none required weakening, removing, or routing around an assertion. assertions_preserved_or_strengthened: true. bugs_routed_to_implement: 13 (root-cause categories; 149 individual test instances).

    Per the Single-Pass Discipline, this report does not trigger a second design-&gt;run_suite loop within this invocation even though the post-design suite still shows failures; that is expected, and /implement is the correct next step.
  </summary>

  <action_items>
    <item priority="P0" target_mode="implement" description="Build orchestrator_utils.py/orchestrate_first_level.py changes for all 13 root-cause categories in failing_test_dispositions (post_design) above, each cross-referenced to its locked decision (T1, T2, T4, T5, T6, T7, T8, T9, T10, T11, T12, C1-REV, C1-GATE, C4, C5, C5-MASK, C6, C7, C8d, C8e-NBACK, C9), per the orchestrator brainstorm report's own action items." />
    <item priority="P0" target_mode="implement" description="Generate orch_config_production.yaml and proc_config_production.yaml exactly per C8e (including the 29-contrast table) and add both to .gitignore; this unblocks all 17 currently-skipped tests in test_production_configs.py." />
    <item priority="P1" target_mode="test" description="Re-run the full suite (run_suite only, not a second design pass) once /implement lands, per T15 action item (4); this report's post-design counts are the baseline it should supersede." />
    <item priority="P2" target_mode="none" description="tests/golden_config_refactored.yaml and tests/golden_config_baseline.yaml are now orphaned (superseded by tests/golden_config_v4.yaml, which self-creates on first successful run once build_first_level_config is implemented); harmless but noted for cleanup. Deleting either requires the same session-bound destructive-Bash grant procedure used for tests/test_legacy_routing_migration.py in this session." />
  </action_items>

  <next_steps>/implement plan and build from the orchestrator brainstorm report, using this report's 13-category punch list as the authoritative test-contract checklist. Upstream fmri-first-level-proc work (U1-U5) proceeds independently from the companion upstream brainstorm report; the orchestrator's final version pin/tag waits for the upstream v4.0.0 tag per T1/C8a.</next_steps>
</test_report>
