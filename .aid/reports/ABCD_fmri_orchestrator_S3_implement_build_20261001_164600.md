<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build" timestamp="2026-10-01T16:46:00Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20261001_111519.md</spec_ref>
  <changes_applied>
    <change id="C1" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="12" />
      </files_modified>
      <notes>Introduced a packaging.version-based comparison for the upstream minimum-version check, with MIN_UPSTREAM_VERSION set to 3.2.0.</notes>
    </change>
    <change id="C2" status="done" user_decision="n/a">
      <files_modified>
        <file path="environment.yaml" lines_changed="2" />
      </files_modified>
      <notes>Added the packaging dependency and pinned fmri_first_level_proc to >= 3.2.0.</notes>
    </change>
    <change id="C3" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="8" />
        <file path="orchestrate_first_level.py" lines_changed="4" />
      </files_modified>
      <notes>Added an upstream import guard and refreshed header metadata to reference the new minimum upstream version.</notes>
    </change>
    <change id="C4" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="40" />
      </files_modified>
      <notes>Introduced PRIMARY_OUTPUT_SUFFIX and _primary_output_ok; completion is now grounded in the frozen primary upstream output file rather than an arbitrary NIfTI count. Renamed the nested per-analysis QC key to upstream_qc_summary.</notes>
    </change>
    <change id="C5" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="20" />
      </files_modified>
      <notes>consolidate_session_qc now reads the outcome-level "qc" key (replacing "fl_qc") and serializes it under "qc" per analysis entry; added session-level n_analyses_flagged and any_analysis_flagged aggregates and per-analysis qc_flagged passthrough.</notes>
    </change>
    <change id="C6" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="35" />
      </files_modified>
      <notes>Hardened the dispatch step (Step 12) to capture upstream status/warnings and derive qc_flagged from FirstLevelResult.status == "warning".</notes>
    </change>
    <change id="C8" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="60" />
      </files_modified>
      <notes>Replaced orchestrator-side concatenation with per-run file collection under a commit-on-success invariant; per-run outputs are written to the session preproc directory.</notes>
    </change>
    <change id="C7" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="150" />
      </files_modified>
      <notes>Retired concatenate_bolds, concatenate_tabular_files, concatenate_task_timing, and compute_mask_intersection; concatenation across runs is now performed entirely upstream.</notes>
    </change>
    <change id="C9" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="30" />
      </files_modified>
      <notes>build_first_level_config now constructs list-form paths (scan_paths/motion_paths/task_timing_paths for task analyses) and forces remove_previous to True on every analysis block.</notes>
    </change>
    <change id="C10" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="10" />
      </files_modified>
      <notes>Added rejection of the legacy concatenate_runs task-definition key during config loading.</notes>
    </change>
    <change id="C11" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="10" />
      </files_modified>
      <notes>Added rejection of legacy extraction-block keys superseded by the upstream >= 3.2.0 extraction contract.</notes>
    </change>
    <change id="C12" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="15" />
        <file path="orchestrate_first_level.py" lines_changed="10" />
      </files_modified>
      <notes>Updated docstrings and inline comments referencing the retired concatenation/legacy timing-directory model to describe the per-run preproc-directory model.</notes>
    </change>
    <change id="C13" status="done" user_decision="n/a">
      <files_modified>
        <file path="example_proc_config.yaml" lines_changed="20" />
      </files_modified>
      <notes>Aligned the example proc config with upstream >= 3.2.0 field names and the list-form paths contract.</notes>
    </change>
    <change id="C14" status="done" user_decision="n/a">
      <files_modified>
        <file path="example_orchestrator_config.yaml" lines_changed="10" />
      </files_modified>
      <notes>Removed concatenate_runs from the example orchestrator config task definitions.</notes>
    </change>
    <change id="C15" status="done" user_decision="n/a">
      <files_modified>
        <file path="proc_config_final.yaml" lines_changed="20" />
      </files_modified>
      <notes>Aligned the local real-world-testing proc config with the same upstream >= 3.2.0 contract as the example config.</notes>
    </change>
    <change id="C16" status="done" user_decision="n/a">
      <files_modified>
        <file path="README.md" lines_changed="40" />
      </files_modified>
      <notes>Updated narrative and examples describing the pipeline's concatenation behavior, processed_files structure, and QC schema.</notes>
    </change>
    <change id="C17" status="done" user_decision="n/a">
      <files_modified>
        <file path="INPUT_SPECIFICATION.md" lines_changed="60" />
      </files_modified>
      <notes>Updated Section 10.3 and related sections to document the list-form processed_files structure and the upstream-delegated concatenation model.</notes>
    </change>
    <change id="C18" status="done" user_decision="n/a">
      <files_modified>
        <file path="tests/test_simulated_pipeline.py" lines_changed="20" />
        <file path="tests/test_coverage_gaps.py" lines_changed="15" />
        <file path="tests/test_coverage_gaps_v2.py" lines_changed="10" />
        <file path="tests/test_cr_implementation.py" lines_changed="10" />
        <file path="tests/test_cr_implementation_extended.py" lines_changed="10" />
      </files_modified>
      <notes>Removed all remaining test coverage for the four retired concatenation/mask-intersection helpers, verified via cross-file grep.</notes>
    </change>
    <change id="C19" status="done" user_decision="n/a">
      <files_modified>
        <file path="tests/conftest.py" lines_changed="15" />
        <file path="tests/test_simulated_pipeline.py" lines_changed="120" />
        <file path="tests/test_coverage_gaps.py" lines_changed="90" />
        <file path="tests/test_coverage_gaps_v2.py" lines_changed="3" />
        <file path="tests/test_cr_implementation.py" lines_changed="25" />
        <file path="tests/test_cr_implementation_extended.py" lines_changed="20" />
        <file path="tests/test_motion_and_qc.py" lines_changed="15" />
        <file path="tests/test_preprocessing.py" lines_changed="25" />
      </files_modified>
      <notes>Re-expressed fixtures and assertions across all eight test files to the new contracts: list-form processed_files and paths keys, the two-level QC schema rename ("fl_qc"/"upstream_qc" outcome-entry keys to "qc"; nested "upstream_qc" to "upstream_qc_summary"), the real upstream status vocabulary ("success"/"warning"), the primary-output-file completion criterion, and removal of concatenate_runs from every task definition fixture. Every signature and key-name change was verified against the actual orchestrator_utils.py/orchestrate_first_level.py implementation before the corresponding test edit. No assertion was weakened; the test_zero_nss_trs edit additionally removed a silent-skip conditional that could mask a missing timing file. Final cross-file grep confirmed zero residual references to concatenate_runs, singular scan_path/motion_path/task_timing_path keys, "fl_qc", or the bare "upstream_qc" entry key across all eight files.</notes>
    </change>
  </changes_applied>
  <summary>
    <total_changes>19</total_changes>
    <completed>19</completed>
    <skipped>0</skipped>
    <blocked>0</blocked>
  </summary>
  <next_steps>Recommended: run /test to validate all changes.</next_steps>
</implement_report>
