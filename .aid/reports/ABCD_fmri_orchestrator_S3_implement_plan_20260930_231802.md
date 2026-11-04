<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-09-30T23:18:02Z" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_test_20260930_194232.md" mode="test" key_items="1" />
  </input_reports>
  <scope_resolutions>
    <resolution id="S1">User approved widening the single action item to both mocks in the mock_first_level fixture: the mock dispatch run functions return None, whereas the fmri_first_level_proc 3.1.0 run() contract returns FirstLevelResult; sim_orchestrator_config enables first-level QC (tests/conftest.py:348), so build_first_level_qc_from_result (orchestrator_utils.py:2957) would raise AttributeError on result=None once the tuple defect is fixed.</resolution>
    <resolution id="S2">User approved re-expressing the two residual _process_session bare-list call sites (tests/test_simulated_pipeline.py:142-150 and :543-550) to the dict return shape (orchestrate_first_level.py:974-979), adding an assertion on routing == "full"; this extends the already-approved return-shape obsolete-test cluster disposition to call sites that the tuple defect had masked.</resolution>
    <resolution id="S3">User approved re-expressing test_nback_cue_relabeling_produces_13_conditions (tests/test_simulated_pipeline.py:242-246) to the BIDS TSV contract. This test runs with skip_first_level=True and never reaches the fixture; its true failure is assert 0 == 1 at :243 (glob for _timing.csv against production's _concat_timing.tsv at orchestrate_first_level.py:750). The input test report misattributed its failure to the fixture defect.</resolution>
    <resolution id="S4">User approved removing the `if aqc.get("upstream_qc") is not None:` guard at tests/test_simulated_pipeline.py:516 so the pct_censored == 5.2 assertion is unconditional (strengthening; upstream_qc is always populated on the success path per orchestrator_utils.py:2972).</resolution>
  </scope_resolutions>
  <changes>
    <change id="C1" priority="P1" source_item="test report action_items[0]">
      <file path="tests/test_simulated_pipeline.py" action="modify" />
      <description>Re-express _make_mock_load_and_validate (lines 80-99) to the fmri_first_level_proc 3.1.0 load_and_validate contract: return list[ResolvedConfig-shaped object] rather than (type, namespace, name) tuples.</description>
      <spec>Signature of the inner function becomes `def mock_load_and_validate(config_path, logger=None, **overrides):` (matches upstream `load_and_validate(config_path, logger=None, **overrides)`; overrides are accepted and ignored). For each block in `config.get("analyses", [])`, build `ns = types.SimpleNamespace(**{k: v for k, v in block.items() if k not in ("type", "name")})`, then set `ns.analysis_type = block["type"]` and `ns.analysis_name = block["name"]`, and append `ns` to the result list. Return the list. Update the outer docstring to: "Create a mock load_and_validate that reads the generated YAML and returns one namespace per analysis block, mirroring fmri_first_level_proc's ResolvedConfig: analysis_type and analysis_name are taken from the block's type/name keys, and all other block keys become attributes." The outer function signature `_make_mock_load_and_validate(proc_template)` and both existing call sites (line 111 and line 530) are unchanged.</spec>
      <dependencies>none</dependencies>
      <risk>low - test-fixture-only; out_dir and out_file_pre are top-level block keys in build_first_level_config output (orchestrator_utils.py:3168-3170), so attribute access in production code and in the mock dispatch resolves.</risk>
      <rollback>git checkout -- tests/test_simulated_pipeline.py (or revert the hunk at lines 80-99).</rollback>
    </change>
    <change id="C2" priority="P1" source_item="S1">
      <file path="tests/test_simulated_pipeline.py" action="modify" />
      <description>Make the mock dispatch run function in _make_mock_dispatch (lines 37-77) return a real upstream FirstLevelResult, matching the fmri_first_level_proc 3.1.0 run() contract.</description>
      <spec>Add module-level imports alongside the existing imports: `from pathlib import Path` (with the stdlib imports) and `from fmri_first_level_proc.first_level_utils import FirstLevelResult` (as a third-party import after `import yaml`). In `_mock_run_fn(ns, logger, analysis_type="unknown")`, keep every existing side effect (makedirs, dummy NIfTI at out_nifti, qc_summary JSON at qc_path, call_log.append) in the existing order, then add as the final statement: `return FirstLevelResult(status="completed", output_dir=Path(ns.out_dir), warnings=[], qc_summary=dict(qc_summary), output_files=[out_nifti, qc_path])`. The inner `fn(ns, logger)` wrapper in `_make_typed_fn` already returns `_mock_run_fn(...)` and needs no change. Extend the `_make_mock_dispatch` docstring's numbered list with: "5. Returns a FirstLevelResult (status='completed') carrying the same qc_summary dict and the two written files as output_files". Do not change the dispatch dict keys or the call_log tuple format.</spec>
      <dependencies>none</dependencies>
      <risk>low - FirstLevelResult is a plain dataclass with five required fields (status, output_dir, warnings, qc_summary, output_files; verified in the installed fmri_first_level_proc 3.1.0 first_level_utils.py:70-91). No currently-passing test reaches the dispatch path (all such tests failed earlier at the tuple defect), so there is no regression surface.</risk>
      <rollback>Revert the added return statement, docstring line, and the two imports.</rollback>
    </change>
    <change id="C3" priority="P1" source_item="S2">
      <file path="tests/test_simulated_pipeline.py" action="modify" />
      <description>Re-express the two residual _process_session bare-list call sites to the dict return shape.</description>
      <spec>(a) test_full_session_nback_and_rest (lines 142-150): replace `outcomes = _process_session(` with `session_result = _process_session(` (arguments unchanged). Replace the comment and `assert isinstance(outcomes, list)` block with: `# Verify _process_session returns the session result dict` / `assert session_result["routing"] == "full"` / `outcomes = session_result["analysis_outcomes"]` / `assert isinstance(outcomes, list)`. Keep `assert len(outcomes) == 3`, the dispatched_types list comprehension, and all three `in dispatched_types` assertions verbatim. Update the docstring's first body sentence from "_process_session returns analysis_outcomes list." to "_process_session returns a session result dict whose analysis_outcomes entry lists per-analysis outcomes.". (b) test_consolidated_qc_failure_detection (lines 543-550): replace `outcomes = _process_session(` with `session_result = _process_session(` (arguments unchanged); immediately after the call add `assert session_result["routing"] == "full"` and `outcomes = session_result["analysis_outcomes"]`; keep both `any(o["status"] == ...)` assertions verbatim. Do not modify line 493 (`outcomes = _process_session(` in test_consolidated_qc_with_first_level), whose return value is unused.</spec>
      <dependencies>none</dependencies>
      <risk>low - assertion re-expression that preserves every prior postcondition and adds the routing assertion.</risk>
      <rollback>Revert the two hunks.</rollback>
    </change>
    <change id="C4" priority="P1" source_item="S3">
      <file path="tests/test_simulated_pipeline.py" action="modify" />
      <description>Re-express test_nback_cue_relabeling_produces_13_conditions to the BIDS TSV timing contract.</description>
      <spec>Line 238 comment: "Find the concatenated timing CSV" becomes "Find the concatenated timing TSV". Line 242: `f.endswith("_timing.csv")` becomes `f.endswith("_timing.tsv")`. Line 245: `pd.read_csv(os.path.join(concat_dir, timing_files[0]))` becomes `pd.read_csv(os.path.join(concat_dir, timing_files[0]), sep="\t")`. Line 246: `timing_df["CONDITION"]` becomes `timing_df["trial_type"]`. Keep `assert len(timing_files) == 1`, the three exclusion assertions, the `"instruction" in conditions` assertion, the explanatory comment block, and `assert len(conditions) == 13, ...` verbatim.</spec>
      <dependencies>none</dependencies>
      <risk>low - mirrors the already-applied re-expression in test_concatenated_timing_onset_offsets (lines 283-287) in the same file.</risk>
      <rollback>Revert the hunk at lines 238-246.</rollback>
    </change>
    <change id="C5" priority="P1" source_item="S4">
      <file path="tests/test_simulated_pipeline.py" action="modify" />
      <description>Make the upstream_qc pct_censored assertion in test_consolidated_qc_with_first_level unconditional.</description>
      <spec>Replace lines 515-517 (`# upstream_qc should contain the mock QC summary` / `if aqc.get("upstream_qc") is not None:` / `    assert aqc["upstream_qc"]["pct_censored"] == 5.2`) with two lines at the loop-body indentation: `# upstream_qc must carry the mock FirstLevelResult.qc_summary` / `assert aqc["upstream_qc"]["pct_censored"] == 5.2`. No other lines in the test change.</spec>
      <dependencies>C2</dependencies>
      <risk>low - strictly strengthens the postcondition; depends on C2 so that upstream_qc is populated from the mock result.</risk>
      <rollback>Restore the conditional guard.</rollback>
    </change>
  </changes>
  <execution_order>C1, C2, C3, C4, C5</execution_order>
  <notes>All five changes touch a single file (tests/test_simulated_pipeline.py) with non-overlapping hunks; they are dispatched as one change group to one execution agent to avoid concurrent-edit conflicts on the same file. Production code (orchestrate_first_level.py, orchestrator_utils.py) is not modified. Per the implement skill's no-testing constraint, validation of these edits is deferred to a subsequent /test run.</notes>
</implement_plan>
