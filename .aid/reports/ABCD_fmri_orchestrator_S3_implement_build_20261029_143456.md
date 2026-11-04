<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build" timestamp="2026-10-29T14:34:56Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20261028_200017.md</spec_ref>
  <changes_applied>
    <change id="C2" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="210 (removed plus added, all changes in this build combined)" />
        <file path="orchestrate_first_level.py" lines_changed="145 (removed plus added, all changes in this build combined)" />
      </files_modified>
      <notes>The orchestrator is now S3-only. load_orchestrator_config requires the s3 section and s3.enabled true, runs the S3 field checks unconditionally, and no longer defaults a disabled s3 section. Completion manifests are read only from S3, discovery always uses the single S3 listing, and the session loop always routes, wipes the session output directory, downloads, uploads and cleans up. One small addition beyond the spec: the retired-key check now reads `config["s3"] or {}`, so an empty `s3:` section reaches the new "s3.enabled must be true" error instead of raising a raw TypeError; its comment was updated to match.</notes>
    </change>
    <change id="C3" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="included in the total above" />
        <file path="orchestrate_first_level.py" lines_changed="included in the total above" />
      </files_modified>
      <notes>The reuse-if-exists branches and force_recompute parameters are removed from remove_initial_trs_bold, extract_motion_regressors, compute_tissue_mask_regressor, fix_nback_cue_labels and format_task_timing, with their docstrings updated. The call sites no longer pass force_recompute, and the tissue voxel counts are recorded unconditionally. study.force_recompute now affects only sentinel routing.</notes>
    </change>
    <change id="C1" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="included in the total above" />
        <file path="orchestrate_first_level.py" lines_changed="included in the total above" />
      </files_modified>
      <notes>discover_subject_inputs takes an optional session_filter, logs the sessions that have input files, and drops all other sessions before the completion-manifest gate, so their manifests are never read. process_participant passes --session through, reports a missing filtered session with a specific error, and runs the input-completeness gate on the discovery result directly. The per-session fallback discovery in _process_session passes its own session as the filter.</notes>
    </change>
    <change id="C4" status="done" user_decision="n/a">
      <files_modified>
        <file path="README.md" lines_changed="16" />
        <file path="INPUT_SPECIFICATION.md" lines_changed="45" />
        <file path="example_orchestrator_config.yaml" lines_changed="4" />
      </files_modified>
      <notes>All listed documentation edits are applied. One addition beyond the spec: the edge-case row for upload_max_workers no longer says "also when S3 is disabled". A sweep of the five edited files finds no remaining local-mode wording, em-dashes, producer or bucket names, or step labels. The example config parses as YAML.</notes>
    </change>
  </changes_applied>
  <summary>
    <total_changes>4</total_changes>
    <completed>4</completed>
    <skipped>0</skipped>
    <blocked>0</blocked>
  </summary>
  <next_steps>Recommended: run /test to validate all changes. Both modules parse and import in the project environment, and a static check of the edited functions found no undefined names; no tests were run. Expected test work: re-express test_s3_enabled_with_v7_templates to expect bucket <bucket>; move the seven test files that use s3.enabled false (conftest.py, golden_config_v4.yaml, test_cr_implementation.py, test_cr_implementation_extended.py, test_discovery_v4.py, test_exit_classification.py, test_preprocessing.py) onto the existing S3 client mocks; remove force_recompute from test calls to the five preprocessing functions; update tests that assert the old --session not-found message or the old required-field message; add tests for session-scoped manifest validation (an invalid manifest in an unselected session is not read) and for the s3.enabled-must-be-true and empty-s3-section errors. A pre-build snapshot of the five edited files is kept in the session scratchpad for exact rollback.</next_steps>
</implement_report>
