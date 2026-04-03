<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-03-16T10:46:18-04:00" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_clean_20260316_120000.md" mode="clean" key_items="6" />
  </input_reports>

  <changes>

    <!-- ============================================================ -->
    <!-- C1: Remove internal markers from production code comments    -->
    <!-- P0 | Findings F1, F2, F8                                    -->
    <!-- ============================================================ -->
    <change id="C1" priority="P0" source_item="F1,F2,F8">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Remove internal CR finding references and debug-prefix markers from
        production code comments. Three edits:
        (a) orchestrator_utils.py line 1192: Remove "(F10)" from NaN policy comment.
        (b) orchestrate_first_level.py line 598: Remove "(F9)" from intersection mask comment.
        (c) orchestrator_utils.py line 1686: Remove "Debug:" prefix from log comment.
      </description>
      <spec>
        (a) orchestrator_utils.py ~line 1192:
            OLD: # NaN handling: "unknown = censor" policy (F10)
            NEW: # NaN handling: "unknown = censor" policy — impute 999.0 to guarantee censoring

        (b) orchestrate_first_level.py ~line 598:
            OLD: # Compute intersection mask across all surviving runs (F9)
            NEW: # Compute intersection mask across all surviving runs

        (c) orchestrator_utils.py ~line 1686:
            OLD: # Debug: log reference mask grid dimensions to confirm inputs are grid-matched.
            NEW: # Log reference mask grid dimensions to confirm inputs are grid-matched.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - comment-only edits, no logic change</risk>
      <rollback>Restore original comment text</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C2: Remove internal markers from test file __init__ and      -->
    <!--     test_cr_implementation.py section headers                -->
    <!-- P0 | Findings F3, F4                                        -->
    <!-- ============================================================ -->
    <change id="C2" priority="P0" source_item="F3,F4">
      <file path="tests/__init__.py" action="modify" />
      <file path="tests/test_cr_implementation.py" action="modify" />
      <description>
        (a) tests/__init__.py: Replace "Phase 1 refactor test suite" with generic label.
        (b) tests/test_cr_implementation.py:
            - Module docstring: Replace C/F references (C1-C10, F1-F12 codes)
              with descriptive feature names.
            - Section headers: Replace "C5: Two-tier rotation unit check (F4)",
              "C7: NaN motion handling — 'unknown = censor' policy (F10)",
              "C6: force_recompute config flag (F8)",
              "C8: Strict task label whitelist (F11)",
              "C10: Mask intersection for concatenated tasks (F9)",
              "C3: consolidate_session_qc tests",
              "C1: Per-analysis outcome tracking (F1) — session status derivation",
              "C4: Structured run-loss warning (F3) — log message format"
              with descriptive names without C/F codes.
            - Class docstring for TestNaNMotionHandling: Remove "Before C7" / "After C7" references.
      </description>
      <spec>
        tests/__init__.py line 1:
            OLD: # Phase 1 refactor test suite for ABCD_fmri_orchestrator_S3
            NEW: # Test suite for ABCD_fmri_orchestrator_S3

        tests/test_cr_implementation.py module docstring (lines 1-15):
            OLD:
              """
              Tests for CR implementation changes C1-C10 (F1-F19 findings).

              Validates:
                C1: Per-analysis outcome tracking + qualified session reporting (F1)
                C2: Phase 1 (non-motion) QC — covered in test_phase2_refactored.py
                C3: Consolidated session-level QC JSON — covered in test_simulated_pipeline.py
                C4: Structured run-loss warning (F3) — log-level test
                C5: Two-tier rotation unit check (F4)
                C6: force_recompute config flag (F8)
                C7: NaN motion handling, "unknown = censor" policy (F10)
                C8: Strict task label whitelist (F11)
                C9: S3 run discovery probes all 9 indices (F12)
                C10: Mask intersection for concatenated tasks (F9)
              """
            NEW:
              """
              Tests for orchestrator feature implementations.

              Validates:
                - Per-analysis outcome tracking + qualified session reporting
                - Pre-analysis (non-motion) QC — covered in test_preprocessing.py
                - Consolidated session-level QC JSON — covered in test_simulated_pipeline.py
                - Structured run-loss warning (log-level test)
                - Two-tier rotation unit check
                - force_recompute config flag
                - NaN motion handling, "unknown = censor" policy
                - Strict task label whitelist
                - S3 run discovery probes all 9 indices
                - Mask intersection for concatenated tasks
              """

        Section header lines (exact replacements):
            # C5: Two-tier rotation unit check (F4)
            → # Test: Two-tier rotation unit check

            # C7: NaN motion handling — "unknown = censor" policy (F10)
            → # Test: NaN motion handling — "unknown = censor" policy

            # C6: force_recompute config flag (F8)
            → # Test: force_recompute config flag

            # C8: Strict task label whitelist (F11)
            → # Test: Strict task label whitelist

            # C10: Mask intersection for concatenated tasks (F9)
            → # Test: Mask intersection for concatenated tasks

            # C3: consolidate_session_qc tests
            → # Test: consolidate_session_qc

            # C1: Per-analysis outcome tracking (F1) — session status derivation
            → # Test: Per-analysis outcome tracking — session status derivation

            # C4: Structured run-loss warning (F3) — log message format
            → # Test: Structured run-loss warning — log message format

        TestNaNMotionHandling docstring:
            OLD: Before C7: NaN -> 0.0 ...
                 After C7: NaN -> 999.0 ...
            NEW: Before fix: NaN -> 0.0 (appears as stillest frame, never censored).
                 After fix: NaN -> 999.0 (guarantees censoring by any reasonable threshold).
      </spec>
      <dependencies>none</dependencies>
      <risk>low - docstring and comment-only edits in test files</risk>
      <rollback>Restore original docstring text</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C3: Delete archived baseline test files                      -->
    <!-- P0 | Finding F5                                             -->
    <!-- ============================================================ -->
    <change id="C3" priority="P0" source_item="F5">
      <file path="tests/test_phase1_baseline.py" action="delete" />
      <file path="tests/test_phase2_baseline.py" action="delete" />
      <description>
        Remove the two fully-skipped archived baseline test modules. They contribute
        zero test coverage at runtime and contain internal phase labels that are
        confusing in production. Historical content is preserved in git history.
      </description>
      <spec>
        Delete tests/test_phase1_baseline.py entirely.
        Delete tests/test_phase2_baseline.py entirely.
        No other files reference these modules (verified: they are not imported
        anywhere except at their own module-level pytest.skip).
      </spec>
      <dependencies>none</dependencies>
      <risk>low - fully-skipped files, zero test coverage impact</risk>
      <rollback>git checkout tests/test_phase1_baseline.py tests/test_phase2_baseline.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C4: Rename test modules from phase labels to feature names   -->
    <!-- P0 | Finding F6                                             -->
    <!-- ============================================================ -->
    <change id="C4" priority="P0" source_item="F6">
      <file path="tests/test_phase1_refactored.py" action="delete" />
      <file path="tests/test_preprocessing.py" action="create" />
      <file path="tests/test_phase2_refactored.py" action="delete" />
      <file path="tests/test_motion_and_qc.py" action="create" />
      <description>
        Rename test_phase1_refactored.py -> test_preprocessing.py and
        test_phase2_refactored.py -> test_motion_and_qc.py. Update module
        docstrings to remove "Phase N" labels. The rename is accomplished
        by copying content with updated docstrings to the new file and
        deleting the old file (git mv equivalent via write+delete).

        Also update the module docstring in the new test_motion_and_qc.py
        to remove "Phase 2" language and any inline "Phase N" comments in
        test method docstrings.
      </description>
      <spec>
        test_preprocessing.py docstring:
            OLD: """
                 Phase 1 Post-Refactor Tests

                 These tests validate the refactored behavior after Phase 1 changes:
                 - generate_censor_file removed
                 ...
                 """
            NEW: """
                 Preprocessing Tests

                 These tests validate the orchestrator's preprocessing behavior:
                 - generate_censor_file removed (handled upstream)
                 - build_first_level_config injects global.tr, fd_threshold, censor_prev_tr; no censor paths
                 - validate_proc_template requires global block; rejects censor_path/censor_paths
                 - compute_first_level_qc reads upstream QC JSON
                 - load_orchestrator_config validates censor_prev_tr
                 """

        test_motion_and_qc.py docstring:
            OLD: """
                 Phase 2 Post-Refactor Tests

                 Validates that motion parameters and FD are now sourced from raw motion.tsv
                 files (not fMRIPrep confounds). FD is recomputed using the Power et al. (2012)
                 formula. DVARS and tissue signals remain on confounds.tsv.
                 """
            NEW: """
                 Motion and QC Tests

                 Validates motion parameter extraction and preprocessing QC:
                 - Motion parameters and FD sourced from raw motion.tsv (not fMRIPrep confounds)
                 - FD computed using the Power et al. (2012) formula
                 - DVARS and tissue signals sourced from confounds.tsv
                 """

        NOTE: The FD-related test class (TestComputeFramewiseDisplacement) will be
        REMOVED from test_motion_and_qc.py as part of C5 (dead function removal).
        The import of compute_framewise_displacement will also be removed in C5.

        Inline "Phase N" references in test_motion_and_qc.py class/method docstrings
        must also be cleaned:
          - TestComputePreprocQCRefactored docstring: remove "Phase 1 (non-motion) QC",
            "Phase 2" language → replace with descriptive terms
          - All test method docstrings with "Phase 1 QC" → "preprocessing QC"
          - All test method docstrings with "Phase 2" → "per-analysis" or "upstream"
          - All "C2" references → remove
      </spec>
      <dependencies>C5 (must complete before C4 to know final content of test_motion_and_qc.py)</dependencies>
      <risk>medium - file rename requires careful content transfer; verify import paths unchanged</risk>
      <rollback>git mv / restore original filenames</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C5: Remove dead compute_framewise_displacement function       -->
    <!-- P0 | Finding F7                                             -->
    <!-- ============================================================ -->
    <change id="C5" priority="P0" source_item="F7">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="tests/test_phase2_refactored.py" action="modify" />
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <file path="INPUT_SPECIFICATION.md" action="modify" />
      <description>
        Remove the dead compute_framewise_displacement() function from
        orchestrator_utils.py (lines 1045-1072). It has no production caller.
        Remove TestComputeFramewiseDisplacement class (8 tests) from what will
        become test_motion_and_qc.py, and remove TestComputeFDAdditional class
        (4 tests) from test_coverage_gaps.py, along with the TestRealDataMotion
        test_fd_computation_on_real_data test method (or the class if it ONLY
        tests FD). Remove the import of compute_framewise_displacement from both
        test files.
        Update INPUT_SPECIFICATION.md to remove the "Framewise Displacement (FD)
        Computation" section (which references compute_framewise_displacement) and
        update the "Degree-to-Radian Conversion" section to remove the dead
        reference.
      </description>
      <spec>
        orchestrator_utils.py:
          Remove lines 1045-1072 (def compute_framewise_displacement through return np.concatenate([[0.0], fd_vals])).
          Remove the blank line separator before it if it leaves a double-blank.

        tests/test_phase2_refactored.py (before rename to test_motion_and_qc.py):
          - Remove "compute_framewise_displacement" from the import list (line 23).
          - Remove the entire TestComputeFramewiseDisplacement class (lines ~37-95).
          - Remove the section header "# compute_framewise_displacement tests (8 tests)"

        tests/test_coverage_gaps.py:
          - Remove "compute_framewise_displacement" from the import list (line 74).
          - Remove the entire TestComputeFDAdditional class (~lines 1032-1064) and
            its section header comment.
          - In TestRealDataMotion.test_fd_computation_on_real_data (~line 1104-1119):
            Remove this single test method (the class has other tests that are valid).
          - Update the file-level docstring to remove
            "- format_task_timing (conditions_include, ...)" → update as part of C6.

        INPUT_SPECIFICATION.md:
          Section "### Framewise Displacement (FD) Computation" (~lines 413-428):
            Replace this entire section with a brief note that FD is computed
            exclusively by fmri_first_level_proc (not the orchestrator), and
            link to the upstream documentation.

          Section "### Degree-to-Radian Conversion" (~lines 430-434):
            Replace:
              "1. **`compute_framewise_displacement()`** — internally, before computing arc-length"
              "2. **`extract_motion_regressors()`** — before writing the output `.1D` file (AFNI convention)"
            With:
              "**`extract_motion_regressors()`** — converts rotations to radians before writing the output `.1D` file (AFNI convention)"

          Note at line 443 referencing "Phase 1 QC" / "Phase 2":
            Replace "Phase 1 QC" → "pre-analysis preprocessing QC"
            Replace "Phase 2" → "per-analysis"
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - removes a tested function; confirm no hidden callers via grep</risk>
      <rollback>Restore function from git history; restore test classes</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C6: Remove vestigial conditions_include parameter            -->
    <!-- P1 | Finding F10                                            -->
    <!-- ============================================================ -->
    <change id="C6" priority="P1" source_item="F10">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <description>
        Remove the conditions_include parameter from format_task_timing() signature
        and its filtering logic (lines 1396-1397). Update the only caller in
        orchestrate_first_level.py to not pass None. Update the test
        test_conditions_include_filter in test_coverage_gaps.py (the test tests
        the removed feature — it must be removed along with the docstring update).
      </description>
      <spec>
        orchestrator_utils.py ~line 1362:
            OLD: def format_task_timing(events_path, condition_column, conditions_include, conditions_exclude, n_remove, TR, out_path, logger, force_recompute=False):
            NEW: def format_task_timing(events_path, condition_column, conditions_exclude, n_remove, TR, out_path, logger, force_recompute=False):

        Remove lines 1396-1397:
            # Filter conditions
            if conditions_include is not None:
                events_df = events_df[events_df[condition_column].isin(conditions_include)]
        Keep the conditions_exclude block unchanged (lines 1399-1400).
        NOTE: The "# Filter conditions" comment may be shared — retain it for conditions_exclude.

        orchestrate_first_level.py ~lines 529-536:
            OLD:
                timing_path, n_dropped = format_task_timing(
                    events_for_timing,
                    condition_col,
                    None,  # conditions_include removed
                    task_def.get("conditions_exclude"),
                    n_remove, TR, timing_out, logger,
                    force_recompute=force_recompute
                )
            NEW:
                timing_path, n_dropped = format_task_timing(
                    events_for_timing,
                    condition_col,
                    task_def.get("conditions_exclude"),
                    n_remove, TR, timing_out, logger,
                    force_recompute=force_recompute
                )

        tests/test_coverage_gaps.py:
          - Remove test_conditions_include_filter method (~lines 406-421) from
            TestFormatTaskTiming class.
          - Update all remaining calls to format_task_timing in test_coverage_gaps.py
            that pass conditions_include as a positional arg (3 remaining calls after
            removing the include test):
              test_conditions_exclude_filter: format_task_timing(events, "trial_type", None, ["dummy", "C"], ...)
                → format_task_timing(events, "trial_type", ["dummy", "C"], ...)
              test_missing_onset_column_raises: format_task_timing(path, "trial_type", None, None, ...)
                → format_task_timing(path, "trial_type", None, ...)
              test_empty_after_filter_raises: format_task_timing(events, "trial_type", None, ["dummy"], ...)
                → format_task_timing(events, "trial_type", ["dummy"], ...)
          - Update file docstring line 12:
              OLD: "- format_task_timing (conditions_include, conditions_exclude, missing columns)"
              NEW: "- format_task_timing (conditions_exclude, missing columns, onset adjustment)"
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - signature change; must update all callers; test removals reduce coverage of removed feature</risk>
      <rollback>Restore original signature and remove caller update</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C7: Promote rotation unit PASSED to logger.info              -->
    <!-- P1 | Finding F13                                            -->
    <!-- ============================================================ -->
    <change id="C7" priority="P1" source_item="F13">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Promote the rotation unit check PASSED log message from logger.debug to
        logger.info for symmetry with the WARNING-level AMBIGUOUS case. Uses the
        proposed phrasing from the clean report.
      </description>
      <spec>
        orchestrator_utils.py ~lines 1144-1148:
            OLD:
                logger.debug(
                    "Rotation unit check PASSED (max abs rotation = %.4f > 1.0): "
                    "data is definitively in degrees. Proceeding with deg2rad.",
                    max_rot
                )
            NEW:
                logger.info(
                    "Rotation unit check: PASSED (max abs rotation = %.4f > 1.0, definitively in degrees).",
                    max_rot
                )
      </spec>
      <dependencies>none</dependencies>
      <risk>low - log level promotion only, no logic change</risk>
      <rollback>Revert logger.info to logger.debug</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C8: Remove unnecessary dir() check in compute_preproc_qc     -->
    <!-- P1 | Finding F15                                            -->
    <!-- ============================================================ -->
    <change id="C8" priority="P1" source_item="F15">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Replace unconventional dir() variable existence check with direct variable
        reference. At this point in compute_preproc_qc, anat_dir is always defined
        (set in both branches of the preceding if ses_part: conditional).
      </description>
      <spec>
        orchestrator_utils.py ~line 2064:
            OLD: anat_dir_str = anat_dir if 'anat_dir' in dir() else "(unknown)"
            NEW: anat_dir_str = anat_dir
      </spec>
      <dependencies>none</dependencies>
      <risk>low - anat_dir is always defined at this point; verified by code flow analysis</risk>
      <rollback>Restore original one-liner</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C9: Remove internal phase labels from docstrings             -->
    <!-- P0 | Finding F9                                             -->
    <!-- ============================================================ -->
    <change id="C9" priority="P0" source_item="F9">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Remove "Phase 1" and "Phase 2" labels from three function docstrings:
        (a) generate_carpet_plot docstring (~line 1784): Replace "Phase 1 QC" and
            "Phase 2" with descriptive parentheticals.
        (b) compute_preproc_qc docstring (~lines 1932-1938): Replace "Phase 1
            (pre-analysis)" and "Phase 2 (consolidated session QC)" labels.
        (c) consolidate_session_qc docstring (~line 2203): Replace "Phase 1
            (preprocessing) and Phase 2 (per-analysis)" labels.
      </description>
      <spec>
        (a) generate_carpet_plot docstring:
            OLD: "FD is no longer computed or displayed by the orchestrator (Phase 1 QC).
                 Motion metrics are deferred to Phase 2 (consolidated QC from upstream
                 enorm.1D/censor.1D produced by fmri_first_level_proc)."
            NEW: "FD is no longer computed or displayed by the orchestrator (pre-analysis
                 preprocessing QC). Motion metrics are deferred to per-analysis QC
                 (consolidated from upstream enorm.1D/censor.1D produced by
                 fmri_first_level_proc)."

        (b) compute_preproc_qc docstring:
            OLD: "Compute Phase 1 (pre-analysis) preprocessing QC metrics for a single run.

                 Phase 1 QC includes non-motion metrics only: tSNR, brain mask coverage,
                 registration Dice, DVARS (from fMRIPrep confounds), and carpet plots.
                 Motion metrics (FD, censor stats) are deferred to Phase 2 (consolidated
                 session QC) where they are sourced exclusively from upstream enorm.1D and
                 censor.1D files produced by fmri_first_level_proc."
            NEW: "Compute pre-analysis preprocessing QC metrics for a single run.

                 Pre-analysis QC includes non-motion metrics only: tSNR, brain mask coverage,
                 registration Dice, DVARS (from fMRIPrep confounds), and carpet plots.
                 Motion metrics (FD, censor stats) are deferred to per-analysis QC
                 (consolidated session QC) where they are sourced exclusively from upstream
                 enorm.1D and censor.1D files produced by fmri_first_level_proc."

        (c) consolidate_session_qc docstring:
            OLD: "Combines Phase 1 (preprocessing) and Phase 2 (per-analysis) QC into a
                 single file per session, replacing the previous pattern of separate
                 per-run and per-analysis JSON files."
            NEW: "Combines pre-analysis preprocessing QC and per-analysis QC into a
                 single file per session, replacing the previous pattern of separate
                 per-run and per-analysis JSON files."
      </spec>
      <dependencies>none</dependencies>
      <risk>low - docstring-only changes</risk>
      <rollback>Restore original docstring text</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C10: Remove BUG-003 reference from test_coverage_gaps.py     -->
    <!-- P0 | Finding F12                                            -->
    <!-- ============================================================ -->
    <change id="C10" priority="P0" source_item="F12">
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <description>
        Replace internal BUG-003 tracker label in test comment with a
        descriptive phrase.
      </description>
      <spec>
        tests/test_coverage_gaps.py ~line 1267:
            OLD: Note: As of v2.3.0, the trailing backslash bug (BUG-003) is fixed.
            NEW: Note: As of v2.3.0, the trailing backslash bug in contrast strings is fixed.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - comment-only edit in test file</risk>
      <rollback>Restore original comment</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C11: Update version and date headers                         -->
    <!-- P2 | Finding F11                                            -->
    <!-- ============================================================ -->
    <change id="C11" priority="P2" source_item="F11">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Update the manual Version and Last Updated headers in both files to
        reflect today's date (2026-03-16) for the production cleanup release.
        Remove the Version header from orchestrator_utils.py entirely
        (it has no __version__ constant; version is authoritative in
        orchestrate_first_level.py line 41). Retain the "Last updated" line
        with today's date.
      </description>
      <spec>
        orchestrator_utils.py lines 9-10:
            OLD:
              # Version: 3.1
              # Last updated: 03/13/26
            NEW:
              # Last updated: 03/16/26

        orchestrate_first_level.py lines 23-24:
            OLD:
              # Version: 3.1
              # Last updated: 03/13/26
            NEW:
              # Version: 3.1
              # Last updated: 03/16/26
      </spec>
      <dependencies>none</dependencies>
      <risk>low - header comment update only</risk>
      <rollback>Restore original header lines</rollback>
    </change>

  </changes>

  <execution_order>
    C1, C2, C3, C5, C6, C7, C8, C9, C10, C11, C4
  </execution_order>

  <!-- Note: C4 (rename test files) must come LAST because C5 modifies the content
       of test_phase2_refactored.py, and both changes must be applied to that file
       before renaming. Executing C3 before C4 ensures baseline files are deleted
       before processing. -->

</implement_plan>
