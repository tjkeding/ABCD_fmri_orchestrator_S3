<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-10-26T18:51:49Z" />

  <environment_preflight>
    <item name="fingerprint-gated upstream reinstall">
      The installed fmri_first_level_proc (conda env ABCD_fmri_orchestrator_S3) was stale relative to the local v4.0.0-build workspace: SHA-256 over the sorted relative paths plus bytes of the fmri_first_level_proc/ package subtree (23 files, excluding __pycache__/.pyc/.DS_Store) mismatched between the workspace and site-packages (7 files differed: events.py, names.py, nuisance.py, qc.py, rest_conn_first_level.py, task_act_first_level.py, task_conn_first_level.py). User approved a non-editable reinstall from the workspace. Post-reinstall fingerprint: both sides 820586cd96ab4efe65d001e051b36193bc829aa0fc328151e8b9c60f5a12d1a5 (match). The full suite below ran only after this match.
    </item>
  </environment_preflight>

  <pre_design_run>
    <total>419</total>
    <passed>418</passed>
    <failed>1</failed>
    <errors>0</errors>
    <coverage_pct>null</coverage_pct>
    <failures>
      <failure test="tests/test_simulated_pipeline.py::TestSkipPartialFailure::test_missing_events_excludes_run" file="tests/test_simulated_pipeline.py" line="531">
        <error_type>OrchestratorError</error_type>
        <message>ses-00A: _complete.json lists file(s) absent from the listing: ['sub-SKIP001_ses-00A_task-nback_run-02_events.tsv']</message>
        <traceback>orchestrate_first_level.py:441: in _process_session -> discover_subject_inputs(...) -> orchestrator_utils.py:899: raise OrchestratorError("ses-00A: _complete.json lists file(s) absent from the listing: [...]")</traceback>
      </failure>
    </failures>
  </pre_design_run>

  <failing_test_dispositions>
    <disposition test="tests/test_simulated_pipeline.py::TestSkipPartialFailure::test_missing_events_excludes_run" file="tests/test_simulated_pipeline.py" classification="obsolete-test">
      <intended_contract>If a configured-task run's events file is missing, that single run is excluded (reason events_missing) and the rest of the session still processes. This is the run-level exclusion logic in orchestrate_first_level.py (the "Pre-exclude non-rest runs without events" block), unchanged by this harmonization round.</intended_contract>
      <current_test_claim>The test deletes a run's events.tsv from disk, then asserts the session still produces a preproc directory and logs mention "events" (expecting the run-level exclusion path to fire).</current_test_claim>
      <evidence>The brainstorm report's locked H7 decision, item (4) (ABCD_fmri_orchestrator_S3_brainstorm_20261022_174139.md): "valid manifest, every listed file for configured-task runs plus the CSF/WM masks listed in session_files must be discovered, else raise OrchestratorError." This gate was implemented faithfully in discover_subject_inputs (orchestrator_utils.py, Phase 4). The test's fixture leaves the deleted events file listed in the session's _complete.json manifest, so discovery now (correctly) raises a session-level integrity error before the run-level exclusion logic is ever reached. The run-level exclusion block itself (orchestrate_first_level.py, "Pre-exclude non-rest runs without events") is unmodified and still reachable for a run whose manifest never listed an events file in the first place.</evidence>
      <action>re-express: the fixture now also removes the deleted file's basename from the manifest's run-2 entry (and decrements file_count) so the manifest and the on-disk listing stay consistent, exercising the still-live run-level exclusion path. No assertion was changed, added, or weakened.</action>
    </disposition>
  </failing_test_dispositions>

  <design_phase>
    <tests_created>20</tests_created>
    <tests_modified>1</tests_modified>
    <files_created>
      <file path="tests/test_run_orchestrator.py" test_count="17" coverage_target="run_orchestrator.py (previously zero test coverage): _run_one's four status branches (success, failed_known_cause, failed_unexplained from a nonzero non-2 code, failed_unexplained from a raised exception) plus the cancelled-before-start path and the running-set bookkeeping; main()'s three-way exit-code precedence (0/1/2, including unexplained-over-known-cause) via a mocked subprocess; the summary CSV's per-subject status column; parse_subject_list's blank/comment/duplicate handling." />
    </files_created>
    <files_modified>
      <file path="tests/test_simulated_pipeline.py" test_count="1 (re-expressed)" coverage_target="obsolete-test disposition fix for test_missing_events_excludes_run (see failing_test_dispositions)." />
      <file path="tests/test_coverage_gaps.py" test_count="2 (new)" coverage_target="GE DV26 timing fixture (16 non-steady-state frames; a run-initial dummy spanning 0-12.0s and a first cue at 12.01s both drop, the first cue counted as straddling; the first real stimulus trial survives with its onset shifted by 12.8s), per the brainstorm's H3 action item. A single-run cue-condition trial-count characterization at the 2-vs-1 survivor boundary, scoped to format_task_timing's own output only (no assertion on fmri_first_level_proc's contrast-floor behavior, per the project's standing rule against mixing orchestrator and upstream testing in one suite; user confirmed this narrowing)." />
      <file path="tests/test_exit_classification.py" test_count="1 (new)" coverage_target="End-to-end test that a confounds TSV whose framewise_displacement disagrees with the recomputed Power FD excludes that run with reason producer_fd_inconsistent, yields session exit category EXIT_FAILED_UNEXPLAINED, and records motion_units_check on the excluded run." />
    </files_modified>
    <design_rationale>
      Covers the four open items the implement build report routed to test design: the GE DV26 timing fixture, a single-run trial-count boundary case (narrowed to orchestrator-owned behavior per user decision), an end-to-end producer-FD-inconsistency exit path, and batch-runner exit-code mapping (previously entirely untested). All new assertions are exact-value or exact-set comparisons, consistent with the existing suite's style; no tolerance-widening or truthiness-only assertions were introduced.
    </design_rationale>
  </design_phase>

  <post_design_run>
    <total>439</total>
    <passed>439</passed>
    <failed>0</failed>
    <errors>0</errors>
    <coverage_pct>null</coverage_pct>
    <failures></failures>
  </post_design_run>

  <receipt_verification>
    Both suite runs were dispatched per the test:run_suite contract (nonce + tee'd receipt file + test_receipt_wrapper.py) and independently verified via verify_receipts.py. Pre-design: the agent's self-reported receipt field omitted the wrapper's leading NONCE=/TIMESTAMP= header (a transcription gap, not a substance mismatch); the authoritative receipt-file checks (mtime within the dispatch window, nonce present, summary/partition/collect counts all matching) passed in full, and the file was read directly as independent confirmation. Post-design: the agent's return and the tee'd file matched on every check (ok=true), including the full header this time.
  </receipt_verification>

  <summary>
    <assertions_preserved_or_strengthened>true</assertions_preserved_or_strengthened>
    <bugs_routed_to_implement>0</bugs_routed_to_implement>
    <recommendation>proceed_to_document</recommendation>
  </summary>

  <action_items>
    <item priority="P2" target_mode="test" description="One-time run of the producer-geometry tissue-mask integration test (tests/test_tissue_mask_regressors.py::TestProducerGeometryIntegration) under AFNI 26.3.02 in the sandbox, per the brainstorm's H8 decision; compare the tie-direction result with the local AFNI 26.0.09 run recorded in this suite (both cases passed under 26.0.09: the tie-invariant case at exactly 1331 voxels, the tie-diagnostic case within {1210, 1331})." />
    <item priority="P2" target_mode="brainstorm" description="Separately scope an upstream fmri-first-level-proc test for the single-run GE trial-floor case (a cue condition at 2 surviving trials kept, at 1 dropped, without losing a contrast): this exercises fmri_first_level_proc's own contrast-dropping logic and was deliberately excluded from this orchestrator suite per the project's standing rule against mixing orchestrator and upstream testing." />
  </action_items>
</test_report>
