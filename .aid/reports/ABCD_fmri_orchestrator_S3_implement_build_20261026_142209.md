<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build" timestamp="2026-10-26T18:22:09Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20261026_133402.md</spec_ref>
  <changes_applied>
    <change id="C1" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="137" />
      </files_modified>
      <notes>Completed in the earlier, halted build pass (before the plan was revised); not redone. Removed the tarball-era decompression function, the unused tabular trim function and their four imports; added the math import.</notes>
    </change>
    <change id="C2" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="8" />
        <file path="environment.yaml" lines_changed="1" />
      </files_modified>
      <notes>Completed in the earlier pass; not redone. The production voxel count uses 3dBrickStat -count -non-zero (output format verified against the local AFNI); nibabel remains in environment.yaml as a test-only dependency.</notes>
    </change>
    <change id="C3" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="14" />
      </files_modified>
      <notes>Completed in the earlier pass; not redone.</notes>
    </change>
    <change id="C4" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="16" />
      </files_modified>
      <notes>Completed in the earlier pass; not redone.</notes>
    </change>
    <change id="C5" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="88" />
      </files_modified>
      <notes>Manifest reader helper and the completion gate added as specified; discovery docstring updated.</notes>
    </change>
    <change id="C6" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="12" />
        <file path="orchestrate_first_level.py" lines_changed="2" />
      </files_modified>
      <notes>As specified.</notes>
    </change>
    <change id="C7" status="done" user_decision="n/a">
      <files_modified>
        <file path="orch_config_production.yaml" lines_changed="15" />
      </files_modified>
      <notes>As specified; the bucket placeholder is unchanged.</notes>
    </change>
    <change id="C8" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="40" />
      </files_modified>
      <notes>As specified.</notes>
    </change>
    <change id="C9" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="3" />
        <file path="orchestrator_utils.py" lines_changed="10" />
      </files_modified>
      <notes>As specified.</notes>
    </change>
    <change id="C10" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="95" />
      </files_modified>
      <notes>As specified.</notes>
    </change>
    <change id="C11" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="4" />
        <file path="orchestrator_utils.py" lines_changed="10" />
      </files_modified>
      <notes>As specified, including both docstrings.</notes>
    </change>
    <change id="C12" status="done" user_decision="n/a">
      <files_modified>
        <file path="run_orchestrator.py" lines_changed="51" />
      </files_modified>
      <notes>As specified. A grep for the old two-way counter and status strings returns no matches.</notes>
    </change>
    <change id="C13" status="done" user_decision="n/a">
      <files_modified>
        <file path="tests/mock_data_utils.py" lines_changed="173" />
      </files_modified>
      <notes>As specified. One addition: the former random-FD draw is kept as a discarded draw, so the mock motion values for each seed are identical to before and downstream numeric fixtures do not shift. The same discarded draw is used in the conftest, non-finite-motion and force-recompute fixtures in the obsolete-test change.</notes>
    </change>
    <change id="C14" status="done" user_decision="n/a">
      <files_modified>
        <file path="tests/test_discovery_v4.py" lines_changed="524" />
        <file path="tests/test_production_configs.py" lines_changed="5" />
      </files_modified>
      <notes>Rewritten as specified. Correction to the plan's wording: a real session holds 34 data files plus _complete.json (the manifest's file_count is 34). The inventory test therefore asserts 34 data keys and a manifest file_count of 34, not a 34-key listing.</notes>
    </change>
    <change id="C15" status="done" user_decision="n/a">
      <files_modified>
        <file path="tests/test_coverage_gaps.py" lines_changed="168" />
        <file path="tests/test_coverage_gaps_v2.py" lines_changed="124" />
        <file path="tests/test_exit_classification.py" lines_changed="4" />
        <file path="tests/test_preprocessing.py" lines_changed="8" />
      </files_modified>
      <notes>As specified. The bz2, gzip and tarfile imports in the coverage-gap test files were used only by the removed classes, so they were removed too. The layout grep returns no matches. The line counts include the timing-test edits in the same two coverage-gap files, which belong to the obsolete-test change.</notes>
    </change>
    <change id="C16" status="done" user_decision="n/a">
      <files_modified>
        <file path="tests/mock_subprocess.py" lines_changed="7" />
        <file path="tests/test_tissue_mask_regressors.py" lines_changed="4" />
      </files_modified>
      <notes>As specified.</notes>
    </change>
    <change id="C17" status="done" user_decision="n/a">
      <files_modified>
        <file path="tests/conftest.py" lines_changed="18" />
        <file path="tests/test_cr_implementation.py" lines_changed="164" />
        <file path="tests/test_cr_implementation_extended.py" lines_changed="76" />
        <file path="tests/test_motion_non_finite.py" lines_changed="6" />
        <file path="tests/test_motion_and_qc.py" lines_changed="3" />
        <file path="tests/test_simulated_pipeline.py" lines_changed="29" />
        <file path="tests/test_qc_schema_v4.py" lines_changed="44" />
        <file path="tests/test_events_validation.py" lines_changed="2" />
        <file path="tests/test_exit_classification.py" lines_changed="14" />
      </files_modified>
      <notes>As specified. The stale-name grep returns no matches. Two fixtures were checked and left unchanged because they never reach the motion extractor: the NSS-detection fixtures in test_coverage_gaps.py and the compute_preproc_qc producer-FD fixtures in test_motion_and_qc.py.</notes>
    </change>
    <change id="C18" status="done" user_decision="n/a">
      <files_modified>
        <file path="tests/test_tissue_mask_regressors.py" lines_changed="89" />
      </files_modified>
      <notes>As specified, gated by the file's existing AFNI-availability skip.</notes>
    </change>
  </changes_applied>
  <summary>
    <total_changes>18</total_changes>
    <completed>18</completed>
    <skipped>0</skipped>
    <blocked>0</blocked>
  </summary>
  <verification_performed>
    No tests were run (implement mode). Every modified Python file compiles (py_compile). Grep checks found no remaining references to the removed functions, the rotation-unit heuristic, the clipping fields, the session flag, the old two-way runner statuses, or the old path layout. Lines added in this build contain no em-dashes and no change-ID shorthand. A snapshot of every target file from before the build is in the sandbox at prebuild_backup_20261026/.
  </verification_performed>
  <open_items_for_test_design>
    Not built here; these belong to /test design per the brainstorm action items: a GE DV26 timing fixture (16 NSS frames; the first cue is dropped); a single-run trial-floor case; an end-to-end pipeline run where producer_fd_inconsistent yields exit 1 and per-run QC motion_units_check = "producer_fd_inconsistent"; and batch-runner exit-code mapping tests.
  </open_items_for_test_design>
  <next_steps>Recommended: run /test (fingerprint-gated upstream reinstall, the full suite on local AFNI 26.0.09, then the one-time AFNI 26.3.02 run of the producer-geometry tissue-mask test in the sandbox).</next_steps>
</implement_report>
