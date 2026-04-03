<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-03-14T07:49:46" />
  <input_reports>
    <report path="inline_user_specification" mode="direct" key_items="1" />
  </input_reports>
  <changes>
    <change id="C1" priority="P0" source_item="inline_user_specification">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Convert the rotation unit check in extract_motion_regressors() from a
        fatal OrchestratorError to a WARNING that proceeds. When max(abs(rotation))
        > 1.0: log at DEBUG that data is definitively in degrees, proceed as before
        (no change to existing branch). When max(abs(rotation)) <= 1.0: log at
        WARNING that rotation units are ambiguous, set a local boolean flag
        rotation_unit_ambiguous=True. Change function return signature from str to
        Tuple[str, bool] — (out_path, rotation_unit_ambiguous). The boolean is
        False when data is definitively in degrees, True when ambiguous.
      </description>
      <spec>
        Lines ~1137-1152: Replace the else branch that raises OrchestratorError
        with a WARNING log and flag. Existing DEBUG branch for > 1.0 remains
        unchanged except that it sets rotation_unit_ambiguous=False.

        Before the if/else block (line ~1136), initialize:
            rotation_unit_ambiguous = False

        New else branch (replacing raise OrchestratorError):
            else:
                logger.warning(
                    "Rotation unit check AMBIGUOUS for %s: "
                    "max(abs(rotation)) = %.6f <= 1.0 across all TRs and axes. "
                    "Units cannot be definitively determined (genuinely low-motion "
                    "subject or data already in radians). Proceeding with deg2rad "
                    "conversion. Run flagged as rotation_unit_ambiguous=True for "
                    "QC review.",
                    motion_tsv_path, max_rot
                )
                rotation_unit_ambiguous = True

        Update the > 1.0 branch to set rotation_unit_ambiguous=False explicitly
        (for clarity, adjacent to the else branch).

        Change return statement from:
            return out_path
        to:
            return out_path, rotation_unit_ambiguous

        Update docstring Returns section to document the new tuple return.
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - changes function return type; all call sites and tests must be updated</risk>
      <rollback>Revert the else branch to raise OrchestratorError and change return back to out_path</rollback>
    </change>

    <change id="C2" priority="P0" source_item="inline_user_specification">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Update the call site at line ~474 to unpack the new tuple return value.
        Inject rotation_unit_ambiguous into preproc_qc_by_run[run_label] if QC
        is enabled and the run has an existing entry; otherwise store it in a
        side dict to ensure the flag is always recorded in the session QC JSON
        regardless of whether preproc QC is enabled.

        Simplest correct approach: always update preproc_qc_by_run[run_label]
        with the flag (creating the entry if absent). The consolidated QC JSON
        writer already handles partial entries.
      </description>
      <spec>
        Line ~474-480:
        Old:
            motion_path = extract_motion_regressors(
                rd["motion_tsv_path"], n_remove,
                study.get("calc_n_motion_derivs", 1),
                motion_out, logger,
                force_recompute=force_recompute
            )
            per_run_motions.append(motion_path)

        New:
            motion_path, rot_unit_ambiguous = extract_motion_regressors(
                rd["motion_tsv_path"], n_remove,
                study.get("calc_n_motion_derivs", 1),
                motion_out, logger,
                force_recompute=force_recompute
            )
            per_run_motions.append(motion_path)
            # Inject rotation unit flag into preproc QC for this run
            preproc_qc_by_run.setdefault(run_label, {})["rotation_unit_ambiguous"] = rot_unit_ambiguous
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - additive change; setdefault is safe whether QC is enabled or not</risk>
      <rollback>Revert unpacking to motion_path = ... and remove the setdefault line</rollback>
    </change>

    <change id="C3" priority="P1" source_item="inline_user_specification">
      <file path="tests/test_cr_implementation.py" action="modify" />
      <description>
        Update all calls to extract_motion_regressors that use the return value
        as a plain string, and update the test that expects OrchestratorError for
        the low-rotation (ambiguous) case to instead expect a WARNING and a
        rotation_unit_ambiguous=True flag in the returned tuple.
      </description>
      <spec>
        1. Any test that does `result = extract_motion_regressors(...)` and
           then asserts `result == out_path` must be updated to unpack:
           `result_path, rot_ambiguous = extract_motion_regressors(...)`
           and assert `result_path == out_path`.

        2. Test at ~line 117 that asserts OrchestratorError for low-rotation
           data must be converted: instead of `pytest.raises(OrchestratorError)`,
           call the function, unpack the tuple, assert rot_ambiguous=True,
           assert the output file was written (function proceeded), and assert
           a WARNING was logged via mock_logger.warning.

        3. Similar pattern for any other test that expects the raise for the
           ambiguous case.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - test-only changes</risk>
      <rollback>Revert test changes</rollback>
    </change>

    <change id="C4" priority="P1" source_item="inline_user_specification">
      <file path="tests/test_phase2_refactored.py" action="modify" />
      <description>
        Update all calls to extract_motion_regressors in this test file to
        unpack the new tuple return value. No OrchestratorError tests for
        ambiguous rotation expected here, but all return-value assertions
        must be updated.
      </description>
      <spec>
        Pattern: `result = extract_motion_regressors(...)` → unpack to
        `result_path, rot_ambiguous = extract_motion_regressors(...)`.
        Update any assertions on result to use result_path.
        Where rot_ambiguous is unused in the test, assign to _ for clarity.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - test-only changes</risk>
      <rollback>Revert test changes</rollback>
    </change>

    <change id="C5" priority="P1" source_item="inline_user_specification">
      <file path="tests/test_phase2_baseline.py" action="modify" />
      <description>
        Same pattern as C4: unpack tuple return values in all calls to
        extract_motion_regressors.
      </description>
      <spec>
        Pattern: `result = extract_motion_regressors(...)` → unpack.
        Update path assertions to use unpacked path variable.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - test-only changes</risk>
      <rollback>Revert test changes</rollback>
    </change>

    <change id="C6" priority="P1" source_item="inline_user_specification">
      <file path="tests/test_cr_implementation_extended.py" action="modify" />
      <description>
        Same pattern: unpack tuple returns. Check for any OrchestratorError
        tests related to rotation units and convert to WARNING + flag assertions.
      </description>
      <spec>
        Pattern: unpack all extract_motion_regressors returns.
        Remove or convert any pytest.raises(OrchestratorError) blocks that
        test the ambiguous rotation case.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - test-only changes</risk>
      <rollback>Revert test changes</rollback>
    </change>

    <change id="C7" priority="P1" source_item="inline_user_specification">
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <description>
        Same pattern: unpack tuple returns in extract_motion_regressors calls.
      </description>
      <spec>
        Pattern: unpack all extract_motion_regressors returns.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - test-only changes</risk>
      <rollback>Revert test changes</rollback>
    </change>
  </changes>
  <execution_order>C1, C2, C3, C4, C5, C6, C7</execution_order>
</implement_plan>
