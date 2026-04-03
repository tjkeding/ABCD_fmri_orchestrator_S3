# ABCD_fmri_orchestrator_S3 — Clean Report

```xml
<clean_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="clean" timestamp="2026-03-16T12:00:00-04:00" />
  <scope>
    orchestrator_utils.py (2927 LOC)
    orchestrate_first_level.py (905 LOC)
    tests/__init__.py
    tests/test_cr_implementation.py
    tests/test_cr_implementation_extended.py
    tests/test_phase1_baseline.py
    tests/test_phase1_refactored.py
    tests/test_phase2_baseline.py
    tests/test_phase2_refactored.py
    tests/test_simulated_pipeline.py
    tests/test_coverage_gaps.py
    orch_config_final.yaml
    README.md, INPUT_SPECIFICATION.md
  </scope>
  <metrics>
    <loc>3832 (production), ~4500 (tests)</loc>
    <files>2 production, 9 test, 2 config, 2 docs</files>
    <avg_complexity>Low to moderate — most functions are linear pipelines with early-exit caching</avg_complexity>
  </metrics>
  <findings>

    <!-- ============================================================ -->
    <!-- F1: Internal development markers in production code           -->
    <!-- ============================================================ -->
    <finding id="F1" severity="major" category="maintainability">
      <location file="orchestrator_utils.py" lines="1192" />
      <description>
        Internal critical review finding reference "(F10)" embedded in a production
        code comment: `# NaN handling: "unknown = censor" policy (F10)`. This is a
        development artifact from the CR disposition process and has no meaning to
        external readers or peer reviewers.
      </description>
      <current># NaN handling: "unknown = censor" policy (F10)</current>
      <proposed># NaN handling: "unknown = censor" policy — impute 999.0 to guarantee censoring</proposed>
      <impact>Confuses external collaborators and peer reviewers; signals incomplete cleanup.</impact>
    </finding>

    <finding id="F2" severity="major" category="maintainability">
      <location file="orchestrate_first_level.py" lines="598" />
      <description>
        Internal CR finding reference "(F9)" in a production code comment:
        `# Compute intersection mask across all surviving runs (F9)`. Same issue
        as F1 — opaque internal label.
      </description>
      <current># Compute intersection mask across all surviving runs (F9)</current>
      <proposed># Compute intersection mask across all surviving runs</proposed>
      <impact>Same as F1.</impact>
    </finding>

    <finding id="F3" severity="major" category="maintainability">
      <location file="tests/__init__.py" lines="1" />
      <description>
        Test package docstring reads "Phase 1 refactor test suite for
        ABCD_fmri_orchestrator_S3". This is a development-phase label that should
        describe the test suite generically for production.
      </description>
      <current># Phase 1 refactor test suite for ABCD_fmri_orchestrator_S3</current>
      <proposed># Test suite for ABCD_fmri_orchestrator_S3</proposed>
      <impact>Misleading — the test suite now covers far more than Phase 1.</impact>
    </finding>

    <finding id="F4" severity="major" category="maintainability">
      <location file="tests/test_cr_implementation.py" lines="5-14, 73, 171, 281, 380, 407, 589, 647" />
      <description>
        Extensive internal development references throughout test file headers and
        section markers: "C1: Per-analysis outcome tracking + qualified session
        reporting (F1)", "C5: Two-tier rotation unit check (F4)", "C7: NaN motion
        handling — 'unknown = censor' policy (F10)", etc. These C/F codes are
        internal tracking IDs from the CR disposition and implementation plan.
      </description>
      <current>
        # C5: Two-tier rotation unit check (F4)
        # C7: NaN motion handling — "unknown = censor" policy (F10)
        # C10: Mask intersection for concatenated tasks (F9)
      </current>
      <proposed>
        # Test: Two-tier rotation unit check
        # Test: NaN motion handling — "unknown = censor" policy
        # Test: Mask intersection for concatenated tasks
      </proposed>
      <impact>
        External contributors and reviewers will not understand C/F codes.
        Test descriptions should be self-explanatory.
      </impact>
    </finding>

    <finding id="F5" severity="major" category="maintainability">
      <location file="tests/test_phase1_baseline.py" lines="2, 5, 24-27" />
      <description>
        Archived test modules (test_phase1_baseline.py, test_phase2_baseline.py)
        still reference internal "Phase 1 refactor" and "Phase 2 refactor" labels.
        These modules are entirely skipped at runtime. For production, they should
        either be removed entirely or have their skip messages cleaned up to
        reference the feature rather than the internal phase label.
      </description>
      <current>
        "Baseline tests are archived — generate_censor_file was removed in Phase 1 refactor."
      </current>
      <proposed>
        Remove archived baseline test files entirely (test_phase1_baseline.py,
        test_phase2_baseline.py) — they serve no purpose at runtime and add
        confusion. Their historical value is preserved in git history.
      </proposed>
      <impact>
        Two fully-skipped test files contribute 0 test coverage while adding
        maintenance burden and confusion about which tests are active.
      </impact>
    </finding>

    <finding id="F6" severity="major" category="maintainability">
      <location file="tests/test_phase1_refactored.py" lines="2-4" />
      <description>
        Test module names encode internal development phase labels:
        test_phase1_refactored.py, test_phase2_refactored.py. These names are
        meaningless to external contributors. Renaming to feature-descriptive
        names would improve navigability.
      </description>
      <current>test_phase1_refactored.py, test_phase2_refactored.py</current>
      <proposed>
        test_preprocessing.py (for Phase 1: confounds, masking, NSS, timing)
        test_motion_and_qc.py (for Phase 2: motion extraction, tissue signals, QC)
      </proposed>
      <impact>
        External collaborators cannot determine which tests to run or modify
        without reading internal development history.
      </impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F7: Dead code                                                 -->
    <!-- ============================================================ -->
    <finding id="F7" severity="major" category="redundancy">
      <location file="orchestrator_utils.py" lines="1045-1072" />
      <description>
        `compute_framewise_displacement()` is dead code in the production pipeline.
        It is defined in orchestrator_utils.py but never imported or called by
        orchestrate_first_level.py. It was used prior to the Phase 2 refactor
        (which moved FD computation to upstream fmri_first_level_proc). It is still
        imported by test files but only for testing the function in isolation —
        the function has no production caller.

        Additionally, it is still documented in INPUT_SPECIFICATION.md (lines 428,
        433) as if it is active in the pipeline. This creates a false impression
        that the orchestrator computes its own FD, which contradicts the actual
        architecture where FD is computed exclusively by upstream.
      </description>
      <current>Function exists, is tested, is documented, but has no production caller.</current>
      <proposed>
        Remove the function from orchestrator_utils.py. Remove its tests from
        test_phase2_refactored.py and test_coverage_gaps.py. Update
        INPUT_SPECIFICATION.md to clarify that FD is computed exclusively by
        upstream fmri_first_level_proc, not by the orchestrator.
      </proposed>
      <impact>
        Dead code misleads readers about the pipeline's actual behavior. The
        INPUT_SPECIFICATION.md documentation actively contradicts the implemented
        architecture, which is a peer-review liability.
      </impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F8: Debug-level log comment                                   -->
    <!-- ============================================================ -->
    <finding id="F8" severity="minor" category="maintainability">
      <location file="orchestrator_utils.py" lines="1686" />
      <description>
        Comment reads "# Debug: log reference mask grid dimensions to confirm
        inputs are grid-matched." The "Debug:" prefix is a development artifact.
        The comment should describe the purpose without the debug label.
      </description>
      <current># Debug: log reference mask grid dimensions to confirm inputs are grid-matched.</current>
      <proposed># Log reference mask grid dimensions to confirm inputs are grid-matched.</proposed>
      <impact>Minor clarity improvement.</impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F9: Phase labels in docstrings                                -->
    <!-- ============================================================ -->
    <finding id="F9" severity="minor" category="maintainability">
      <location file="orchestrator_utils.py" lines="1784-1786, 1932-1937, 2203" />
      <description>
        Docstrings reference internal "Phase 1" and "Phase 2" labels:
        - generate_carpet_plot: "FD is no longer computed or displayed by the
          orchestrator (Phase 1 QC)"
        - compute_preproc_qc: "Phase 1 QC includes non-motion metrics only"
        - consolidate_session_qc: "Combines Phase 1 (preprocessing) and Phase 2
          (per-analysis) QC"

        While "Phase 1 (preprocessing)" and "Phase 2 (per-analysis)" do carry
        descriptive parentheticals, the "Phase N" prefix is an internal label
        from the development process. For production, the parenthetical
        descriptions alone are sufficient and clearer.
      </description>
      <current>
        "Phase 1 (pre-analysis) preprocessing QC"
        "Phase 2 (consolidated QC from upstream)"
      </current>
      <proposed>
        "Pre-analysis preprocessing QC (non-motion metrics)"
        "Per-analysis QC (motion and status, from upstream)"
      </proposed>
      <impact>Clearer for external readers; removes internal development jargon.</impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F10: conditions_include vestige                               -->
    <!-- ============================================================ -->
    <finding id="F10" severity="minor" category="redundancy">
      <location file="orchestrator_utils.py" lines="1362, 1396-1397" />
      <description>
        `format_task_timing()` accepts a `conditions_include` parameter that is
        always passed as `None` from its only caller (orchestrate_first_level.py
        line 532). The parameter, its docstring, and its filtering logic are
        vestigial — the feature was apparently removed from the config but the
        function signature was not cleaned up.
      </description>
      <current>
        def format_task_timing(events_path, condition_column, conditions_include, conditions_exclude, ...)
        ...
        if conditions_include is not None:
            events_df = events_df[events_df[condition_column].isin(conditions_include)]
      </current>
      <proposed>
        Remove `conditions_include` parameter and its filtering logic. The caller
        already passes `None` with a comment "# conditions_include removed".
      </proposed>
      <impact>Reduces cognitive load and eliminates dead parameter.</impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F11: Version and date headers                                 -->
    <!-- ============================================================ -->
    <finding id="F11" severity="minor" category="maintainability">
      <location file="orchestrator_utils.py" lines="9-10" />
      <location file="orchestrate_first_level.py" lines="23-24" />
      <description>
        Both files have manual "Version: 3.1" and "Last updated: 03/13/26"
        headers. These must be updated before any release. Additionally, manual
        version tracking in file headers is error-prone — the authoritative
        version is `__version__ = "3.1"` in orchestrate_first_level.py (line 41).
        The file headers should either reference that constant or be removed to
        avoid drift.
      </description>
      <current># Version: 3.1 / # Last updated: 03/13/26</current>
      <proposed>
        Update version and date at release time. Consider removing the manual
        version/date from orchestrator_utils.py header (it has no __version__
        constant) to avoid double-maintenance.
      </proposed>
      <impact>Stale version info erodes trust in documentation accuracy.</impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F12: BUG-003 reference in test                                -->
    <!-- ============================================================ -->
    <finding id="F12" severity="minor" category="maintainability">
      <location file="tests/test_coverage_gaps.py" lines="1267" />
      <description>
        Comment references internal bug tracker ID: "Note: As of v2.3.0, the
        trailing backslash bug (BUG-003) is fixed." BUG-003 is an internal
        tracking label from development, not a public issue tracker reference.
      </description>
      <current>Note: As of v2.3.0, the trailing backslash bug (BUG-003) is fixed.</current>
      <proposed>Note: As of v2.3.0, the trailing backslash bug in contrast strings is fixed.</proposed>
      <impact>Minor — internal label in test code, but should still be cleaned for production.</impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F13: [DEBUG] messages in log files                            -->
    <!-- ============================================================ -->
    <finding id="F13" severity="minor" category="maintainability">
      <location file="orchestrator_utils.py" lines="100, 107, 239, 298, 390, 403, 894, 981, 1144, 1452, 1610, 1684, 1692, 2918" />
      <description>
        14 `logger.debug()` calls produce `[DEBUG]` entries in per-subject log
        files. This is by design (upstream setup_logging sets file handler to
        DEBUG level, console to INFO), so these do NOT appear in terminal output.
        However, the user has flagged this as a concern for production readiness.

        The file-level DEBUG logging is useful for diagnostics but verbose for
        production at scale (~11,000 subjects). Two options:
        (A) Accept as-is — file-level DEBUG is standard practice and aids
            post-hoc troubleshooting. No change needed.
        (B) Promote the rotation unit check PASSED message (line 1144) from
            debug to info, since the AMBIGUOUS case is already a warning and
            having no corresponding success message at the same level is
            asymmetric.

        Recommendation: Option A with exception of Option B for the rotation
        check symmetry.
      </description>
      <current>logger.debug("Rotation unit check PASSED ...")</current>
      <proposed>logger.info("Rotation unit check: PASSED (max abs rotation = %.4f > 1.0, definitively in degrees).", max_rot)</proposed>
      <impact>
        Log file readability: the rotation unit check is a substantive QC
        decision that should be visible at INFO level alongside its WARNING
        counterpart for auditability.
      </impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F14: Bare except clause                                       -->
    <!-- ============================================================ -->
    <finding id="F14" severity="minor" category="correctness">
      <location file="orchestrator_utils.py" lines="1696" />
      <description>
        Bare `except Exception:` used to catch failure of a diagnostic 3dinfo
        call during mask intersection. This is acceptable here (the info is
        purely diagnostic and the fallback logger.debug is appropriate), but
        the pattern should be noted for awareness.
      </description>
      <current>except Exception:</current>
      <proposed>No change needed — acceptable for a non-critical diagnostic.</proposed>
      <impact>None — defensive logging of a non-critical diagnostic.</impact>
    </finding>

    <!-- ============================================================ -->
    <!-- F15: anat_dir_str fallback pattern                            -->
    <!-- ============================================================ -->
    <finding id="F15" severity="style" category="correctness">
      <location file="orchestrator_utils.py" lines="2064" />
      <description>
        `anat_dir_str = anat_dir if 'anat_dir' in dir() else "(unknown)"` uses
        `dir()` to check local variable existence. This is a defensive pattern
        that is technically correct but unconventional. At this point in the code,
        `anat_dir` is always defined (it's set in both branches of the
        `if ses_part:` conditional above), so the `dir()` check is unnecessary.
      </description>
      <current>anat_dir_str = anat_dir if 'anat_dir' in dir() else "(unknown)"</current>
      <proposed>anat_dir_str = anat_dir</proposed>
      <impact>Marginal clarity improvement; removes a confusing defensive pattern.</impact>
    </finding>

  </findings>

  <summary>
    <critical_count>0</critical_count>
    <major_count>7</major_count>
    <minor_count>7</minor_count>
    <style_count>1</style_count>
    <total_findings>15</total_findings>
    <overall_assessment>needs_minor_work</overall_assessment>
    <narrative>
      The production code (orchestrator_utils.py and orchestrate_first_level.py)
      is well-structured, well-documented, and functionally correct. No critical
      bugs, no security issues, no performance bottlenecks, and no algorithmic
      errors were found.

      The primary concern for production readiness is the pervasive presence of
      internal development markers — CR finding IDs (F1-F19), implementation
      change IDs (C1-C10), BUG tracker labels (BUG-003), and internal phase
      labels (Phase 1, Phase 2) — throughout code comments and test files. These
      are artifacts of a rigorous development process but would be confusing to
      external collaborators and peer reviewers.

      Secondary concerns include one dead function
      (compute_framewise_displacement) that contradicts the documented
      architecture, a vestigial function parameter (conditions_include), and
      version/date headers that need updating before release.

      All findings are addressable with straightforward text edits — no
      architectural or algorithmic changes are required.
    </narrative>
  </summary>

  <action_items>
    <item priority="P0" target_mode="implement" finding_ref="F1,F2,F3,F4,F5,F6,F8,F9,F12" description="Remove all internal development markers (F-codes, C-codes, BUG-codes, Phase labels) from production code, test files, and test module names. Remove archived baseline test files." />
    <item priority="P0" target_mode="implement" finding_ref="F7" description="Remove dead compute_framewise_displacement() function, its tests, and update INPUT_SPECIFICATION.md to reflect that FD is computed exclusively by upstream." />
    <item priority="P1" target_mode="implement" finding_ref="F10" description="Remove vestigial conditions_include parameter from format_task_timing()." />
    <item priority="P1" target_mode="implement" finding_ref="F13" description="Promote rotation unit check PASSED message from logger.debug to logger.info for symmetry with the WARNING-level AMBIGUOUS case." />
    <item priority="P1" target_mode="implement" finding_ref="F15" description="Remove unnecessary dir() check in compute_preproc_qc anat_dir_str fallback." />
    <item priority="P2" target_mode="implement" finding_ref="F11" description="Update version headers and dates before release; consider removing manual version from orchestrator_utils.py header." />
  </action_items>
</clean_report>
```
