# Critical Review: C1-C10 Implementation (CR Findings F1-F19)

```xml
<cr_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="cr" timestamp="2026-03-13T19:20:00Z" />
  <scope>
    Review of C1-C10 implementation changes from the CR findings disposition
    (F1-F19). Scope restricted to orchestrator_utils.py and
    orchestrate_first_level.py; upstream fmri_first_level_proc and
    proc_config changes are out of scope (confirmed manually applied).

    Files reviewed:
      - orchestrate_first_level.py (880 lines, version 3.1)
      - orchestrator_utils.py (~2650 lines, version 3.1)
      - tests/test_cr_implementation.py (710 lines, 30 tests)
      - ABCD_fmri_orchestrator_S3_implement_plan_20260313_165151.md
      - ABCD_fmri_orchestrator_S3_implement_build_20260313_181403.md

    Changes reviewed:
      C1: Per-analysis outcome tracking + qualified session reporting (F1)
      C2: Remove orchestrator FD from QC; Phase 1/Phase 2 split (F2)
      C3: Consolidated session-level QC JSON (F2 enrichment)
      C4: Structured run-loss warning (F3)
      C5: Two-tier rotation unit check (F4)
      C6: force_recompute config flag (F8)
      C7: NaN motion handling — "unknown = censor" policy (F10)
      C8: Strict task label whitelist (F11)
      C9: S3 run discovery — probe all 9 indices (F12)
      C10: Mask intersection for concatenated tasks (F9)
  </scope>

  <findings>

    <!-- ================================================================ -->
    <!-- F1: session_wall_time hardcoded to 0.0 in consolidated QC JSON   -->
    <!-- ================================================================ -->
    <finding id="F1" severity="critical" category="validity">
      <location file="orchestrate_first_level.py" lines="751" />
      <description>
        The call to consolidate_session_qc() at line 751 passes
        session_wall_time=0.0 as a hardcoded literal. The actual session
        elapsed time is computed in process_participant() at line 213 (elapsed
        = time.time() - session_start), but this value exists only in the
        outer scope and is never passed into _process_session().

        As a result, every consolidated QC JSON file will record
        "wall_time_seconds": 0.0 for the session-level block, regardless of
        actual runtime.
      </description>
      <evidence>
        orchestrate_first_level.py:751:
            consolidate_session_qc(
                sub_id, ses_label, qc_session_status, 0.0,  # &lt;-- hardcoded
                preproc_qc_by_run, analysis_outcomes, qc_json_path, logger
            )

        The session_start timestamp is tracked at line 165 within
        process_participant(), but _process_session() does not have access
        to it. The consolidated QC is written inside _process_session()
        (Step 12b), before control returns to process_participant().
      </evidence>
      <impact>
        Session-level wall time in the QC JSON is always 0.0, rendering
        this field useless for runtime auditing, cost estimation, and
        performance regression detection across ~11,000 subjects. The
        per-analysis wall_time_seconds fields ARE correct (timed inside
        the analysis loop at lines 682-694), so the total can be
        approximated by summing analysis times, but preprocessing time
        and overhead are lost.
      </impact>
      <recommendation>
        Track session start time at the top of _process_session() (e.g.,
        session_start = time.time()) and compute elapsed time at the end
        (session_elapsed = time.time() - session_start). Pass
        session_elapsed to consolidate_session_qc() instead of 0.0.
        Alternatively, move the consolidation call to process_participant()
        where elapsed is already available, but this requires passing
        preproc_qc_by_run out of _process_session() alongside
        analysis_outcomes.
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F2: force_recompute broken for all AFNI-based cache functions    -->
    <!-- ================================================================ -->
    <finding id="F2" severity="critical" category="robustness">
      <location file="orchestrator_utils.py" lines="871-905, 933-995, 1409-1441, 1555-1613, 1615-1663" />
      <description>
        When force_recompute=True, the early-exit cache check is correctly
        bypassed (e.g., "if os.path.isfile(out_path) and not
        force_recompute"), but the function then calls an AFNI tool (3dcalc,
        3dTcat, 3dmerge, 3dBlurToFWHM, 3dmask_tool) with -prefix pointing
        to a file that ALREADY EXISTS on disk. AFNI programs refuse to
        overwrite existing output files by default — they raise an error
        such as "** FATAL ERROR: Output dataset already exists."

        This means force_recompute is functionally broken for all
        AFNI-backed caching functions. Affected functions:
          - apply_brain_mask (3dcalc)
          - remove_initial_trs_bold (3dTcat)
          - concatenate_bolds (3dTcat)
          - apply_smoothing (3dmerge / 3dBlurToFWHM)
          - compute_mask_intersection (3dmask_tool)

        Non-AFNI functions (extract_motion_regressors, extract_tissue_signals,
        fix_nback_cue_labels, format_task_timing, concatenate_tabular_files,
        concatenate_task_timing) use np.savetxt / pd.to_csv / shutil.copy2,
        all of which overwrite silently. These work correctly with
        force_recompute=True.

        The unit tests for force_recompute (test_cr_implementation.py) only
        exercise extract_motion_regressors (np.savetxt — works) and
        compute_mask_intersection (mock_subprocess — always succeeds). No
        test exercises force_recompute with real AFNI tools on an existing
        output file.
      </description>
      <evidence>
        orchestrator_utils.py:882-884 (apply_brain_mask):
            if os.path.isfile(out_path) and not force_recompute:
                logger.info("Masked BOLD already exists: %s", out_path)
                return out_path
            # When force_recompute=True, falls through to line 886:
            cmd = ["3dcalc", "-a", bold_path, "-b", mask_path,
                   "-expr", "a*step(b)", "-prefix", out_path]
            # AFNI will refuse: out_path already exists.

        Same pattern in remove_initial_trs_bold (line 957),
        concatenate_bolds (line 1413), apply_smoothing (line 1561),
        compute_mask_intersection (line 1643).
      </evidence>
      <impact>
        Any attempt to use force_recompute=True in an environment with real
        AFNI will crash on the first AFNI-backed function that has a cached
        output. The feature was designed for iterative development, so this
        will be the first thing a developer encounters when toggling the
        flag. The resulting CalledProcessError will propagate as an
        OrchestratorError, halting processing for the entire session.
      </impact>
      <recommendation>
        Two options, either of which is correct:

        Option A (preferred — minimal change): When force_recompute is True
        and the output file exists, delete the existing file before calling
        the AFNI command. Insert before the subprocess.run call:
            if force_recompute and os.path.isfile(out_path):
                os.remove(out_path)
        Apply to all 5 affected functions.

        Option B: Pass AFNI's -overwrite flag when force_recompute is True.
        This is less portable (not all AFNI programs support -overwrite
        identically) and harder to maintain.

        After fixing, add a test that exercises force_recompute on at least
        one AFNI-backed function with an existing output file (even with
        mock subprocess, the test should verify os.remove is called or the
        file is recreated).
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F3: NaN derivative propagation inflates censored TR count        -->
    <!-- ================================================================ -->
    <finding id="F3" severity="major" category="validity">
      <location file="orchestrator_utils.py" lines="1148-1159, 1169-1186" />
      <description>
        When NaN values exist in the base motion parameters, the temporal
        derivative computation (np.diff) propagates NaN to adjacent TRs.
        Specifically, if base motion has NaN at TR t, the first derivative
        will have NaN at both TRs t and t+1 (because diff[t] = base[t+1] -
        base[t], and diff[t-1] = base[t] - base[t-1], both involving NaN).
        For the second derivative, NaN spreads further.

        After derivative expansion, the NaN imputation replaces ALL NaN
        positions with 999.0. This means a single NaN in the raw motion data
        can produce up to 2 * (1 + calc_n_motion_derivs) columns of 999.0
        values across 1-3 TRs (depending on derivative degree), potentially
        censoring TRs whose base motion was measured correctly.

        The WARNING log message reports the total NaN count across ALL
        columns (including derivatives), which overstates the number of
        "real" NaN values in the raw data.
      </description>
      <evidence>
        orchestrator_utils.py:1148-1157 (derivative computation):
            for degree in range(1, calc_n_motion_derivs + 1):
                diff = np.diff(prev_degree_data, axis=0)
                deriv_data = np.vstack([np.zeros((1, ...)), diff])
                arrays.append(deriv_data)
                prev_degree_data = deriv_data
            motion_data = np.hstack(arrays)  # NaN propagated through derivatives

        orchestrator_utils.py:1169-1186 (NaN imputation):
            nan_mask = np.isnan(motion_data)  # includes derivative NaN
            # Warning reports total count including derivative-propagated NaN

        With calc_n_motion_derivs=1 (the production default), a single NaN
        at TR 5 in rot_x produces: 999.0 in col_3 (rot_x) at TR 5, 999.0
        in col_9 (d1_rot_x) at TRs 5 and 6. The warning reports "3
        occurrence(s) across 2 unique TR(s)" — TR 6's base data is clean.
      </evidence>
      <impact>
        The conservative direction of this error (more censoring, not less)
        makes it defensible from a data quality standpoint. The primary
        concern is that a single corrupted TR in one rotation axis could
        cause up to 3 TRs to be censored (with 1 derivative), which could
        impact DOF in aggressive-censoring analyses (rest_conn with low FD
        threshold). In practice, NaN in motion data is rare, and the extra
        1-2 TRs of censoring is unlikely to affect results meaningfully.

        The inflated NaN count in the warning log is an interpretability
        issue, not a data integrity issue.
      </impact>
      <recommendation>
        P2 — acceptable as-is given the conservative direction. If addressed:

        1. Impute NaN in base_data BEFORE derivative computation (i.e.,
           replace NaN with 999.0 in the 6 base columns, then compute
           derivatives normally). This confines the 999.0 imputation to
           only the truly-NaN TRs in the base columns, while derivatives
           of 999.0 will be large (diff of 999.0 vs. normal value) and
           censored naturally.

        2. Log two counts: "N raw NaN(s) in base motion at TRs [...]" and
           "M additional derivative-propagated NaN(s)" for full transparency.
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F4: Session status derivation duplicated in two locations         -->
    <!-- ================================================================ -->
    <finding id="F4" severity="major" category="robustness">
      <location file="orchestrate_first_level.py" lines="175-182, 737-744" />
      <description>
        The session status derivation logic (empty -> "failed", all success
        -> "success", any success -> "partial", else -> "failed") is
        implemented identically in two locations:

        1. _process_session() lines 737-744 (for the consolidated QC JSON)
        2. process_participant() lines 175-182 (for the session_results dict
           and SESSION SUMMARY log)

        This violates DRY and introduces a maintenance risk: if the logic
        changes (e.g., adding a "skipped" status), both locations must be
        updated in lockstep. The test suite (TestSessionStatusDerivation)
        tests a third copy of the logic (_derive_status static method) that
        is not the actual production code.
      </description>
      <evidence>
        orchestrate_first_level.py:175-182:
            if not analysis_outcomes:
                session_status = "failed"
            elif all(o["status"] == "success" for o in analysis_outcomes):
                session_status = "success"
            elif any(o["status"] == "success" for o in analysis_outcomes):
                session_status = "partial"
            else:
                session_status = "failed"

        orchestrate_first_level.py:737-744 (identical):
            if not analysis_outcomes:
                qc_session_status = "failed"
            ...
      </evidence>
      <impact>
        Divergence between the QC JSON session status and the log-reported
        session status would be a data provenance defect. Currently they
        are identical, but the duplication makes future divergence likely
        during maintenance.
      </impact>
      <recommendation>
        Extract a module-level helper function:

            def _derive_session_status(analysis_outcomes):
                if not analysis_outcomes:
                    return "failed"
                elif all(o["status"] == "success" for o in analysis_outcomes):
                    return "success"
                elif any(o["status"] == "success" for o in analysis_outcomes):
                    return "partial"
                else:
                    return "failed"

        Call from both locations. Update the test to exercise the actual
        production function rather than a replicated copy.
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F5: Rotation unit check may halt batch on phantom/sedated data   -->
    <!-- ================================================================ -->
    <finding id="F5" severity="minor" category="generalizability">
      <location file="orchestrator_utils.py" lines="1118-1138" />
      <description>
        The two-tier rotation unit check raises an OrchestratorError when
        max(abs(rotation)) &lt;= 1.0 across all TRs and axes. The physical
        justification is sound for awake human subjects in an MRI head coil.
        However, this check will produce false positives for:

        1. Phantom scans (rotation is nearly zero by construction)
        2. Deeply sedated / anesthetized subjects (pediatric or clinical
           populations where motion may be extremely small)
        3. Synthetic / simulated motion data in test environments

        In the ABCD context (awake children aged 9-10), false positives are
        extremely unlikely. The concern is generalizability if this
        orchestrator is adapted for other datasets.
      </description>
      <evidence>
        orchestrator_utils.py:1122-1138:
            max_rot = float(np.nanmax(np.abs(rot_cols)))
            if max_rot > 1.0:
                logger.debug("Rotation unit check PASSED ...")
            else:
                raise OrchestratorError("Rotation unit check FAILED ...")
      </evidence>
      <impact>
        Low for ABCD. The check is a hard error (raises OrchestratorError),
        which will halt the entire session for the affected subject. In
        automated batch processing of ~11,000 subjects, this produces a
        single-subject failure that is logged and recoverable. The error
        message correctly directs manual inspection.
      </impact>
      <recommendation>
        No change needed for ABCD. If the orchestrator is later
        generalized to other datasets, consider making this a configurable
        warning (log at WARNING and continue) with a config flag
        "strict_rotation_check: true" (default true for ABCD). Document
        the assumption in INPUT_SPECIFICATION.md.
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F6: compute_preproc_qc called before masking in NSS-trimmed path -->
    <!-- ================================================================ -->
    <finding id="F6" severity="minor" category="validity">
      <location file="orchestrate_first_level.py" lines="421-431" />
      <description>
        compute_preproc_qc() is called at Step 6 (line 421) using the
        masked BOLD (bold_path=masked_bold) and n_remove from
        detect_non_steady_state_trs(). Note that n_remove is computed
        TWICE for the same confounds file: once at line 423 (for QC) and
        again at line 435 (for actual trimming). The two calls to
        detect_non_steady_state_trs() are redundant. This is a performance
        issue (reads and parses the confounds TSV twice), not a correctness
        issue, but worth noting.
      </description>
      <evidence>
        orchestrate_first_level.py:423:
            n_remove_for_qc = detect_non_steady_state_trs(rd["confounds_path"], logger)
        orchestrate_first_level.py:435:
            n_remove = detect_non_steady_state_trs(rd["confounds_path"], logger)
      </evidence>
      <impact>
        No correctness impact — both calls return the same value. Minor
        performance impact from redundant file I/O (~negligible per run).
      </impact>
      <recommendation>
        Move the n_remove detection before Step 5 (brain masking) and reuse
        the same value for both QC and trimming. E.g.:

            n_remove = detect_non_steady_state_trs(rd["confounds_path"], logger)
            # Step 5: Brain mask
            masked_bold = apply_brain_mask(...)
            # Step 6: QC
            qc_metrics = compute_preproc_qc(..., n_remove, ...)
            # Step 7: Trim
            trimmed_bold, n_trs = remove_initial_trs_bold(..., n_remove, ...)
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F7: 3dmask_tool -inter may fail on identical-grid assumption     -->
    <!-- ================================================================ -->
    <finding id="F7" severity="minor" category="robustness">
      <location file="orchestrator_utils.py" lines="1647-1650" />
      <description>
        compute_mask_intersection() passes per-run masks to 3dmask_tool
        -inter without verifying that all masks share the same grid
        (dimensions, orientation, voxel size). AFNI's 3dmask_tool typically
        requires grid-matching inputs. In the current pipeline,
        per_run_masks are fMRIPrep BOLD-space brain masks from the same
        session and space, so they are effectively guaranteed to match.
        However, no explicit check is performed.
      </description>
      <evidence>
        orchestrator_utils.py:1647-1650:
            cmd = ["3dmask_tool", "-inter", "-prefix", out_path,
                   "-input"] + list(mask_paths)
            subprocess.run(cmd, capture_output=True, text=True, check=True)
      </evidence>
      <impact>
        Negligible for ABCD (fMRIPrep produces all BOLD masks on the same
        template grid). If per-run masks ever differed in grid (e.g., due to
        partial FOV acquisitions), 3dmask_tool would raise an error that
        would propagate as OrchestratorError.
      </impact>
      <recommendation>
        No change needed — the existing CalledProcessError handling
        provides an adequate safety net. If desired, add a debug-level
        log message confirming grid dimensions match before the AFNI call.
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F8: Hardcoded version "3.1" in consolidate_session_qc            -->
    <!-- ================================================================ -->
    <finding id="F8" severity="note" category="reproducibility">
      <location file="orchestrator_utils.py" lines="2201" />
      <description>
        The orchestrator version is hardcoded as the string literal "3.1"
        inside consolidate_session_qc() (line 2201). The version is also
        specified in the file header of both orchestrate_first_level.py
        (line 23) and orchestrator_utils.py. These three locations must be
        updated in lockstep on version bumps. A module-level constant
        (e.g., __version__ = "3.1") would be more maintainable.
      </description>
      <evidence>
        orchestrator_utils.py:2201:
            "orchestrator_version": "3.1",
        orchestrate_first_level.py:23:
            # Version: 3.1
      </evidence>
      <impact>
        Version drift between the QC JSON provenance and the actual code
        version could cause confusion during batch-level QC auditing.
      </impact>
      <recommendation>
        Define __version__ = "3.1" as a module-level constant in
        orchestrate_first_level.py. Import and reference it in
        consolidate_session_qc(). Update the header comment to reference
        the constant.
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F9: Per-run smoothing uses wrong mask index after run failures    -->
    <!-- ================================================================ -->
    <finding id="F9" severity="minor" category="robustness">
      <location file="orchestrate_first_level.py" lines="611-620" />
      <description>
        In the per-run (non-concatenated, i.e., rest) smoothing path,
        the mask for each run is retrieved as run_dicts[i]["mask_path"]
        (line 615). However, per_run_bolds only contains BOLDs from
        SUCCESSFULLY processed runs (failed runs are skipped and added to
        skipped_runs). The run_dicts list still contains ALL runs including
        failed ones. If run 1 fails and run 2 succeeds, per_run_bolds[0]
        corresponds to run_dicts[1], but run_dicts[0] (the failed run)
        would be indexed.

        This means the per-run smoothing loop applies the wrong mask
        whenever a preceding run fails preprocessing.
      </description>
      <evidence>
        orchestrate_first_level.py:611-620:
            if smoothing_cfg.get("enabled", False):
                smoothed_bolds = []
                for i, bold in enumerate(per_run_bolds):
                    smoothed = apply_smoothing(
                        bold, run_dicts[i]["mask_path"],  # &lt;-- wrong index
                        smoothing_cfg["method"], smoothing_cfg["fwhm"],
                        preproc_dir, f"sub-{sub_id}_{run_dicts[i]['run_label']}",
                        logger, force_recompute=force_recompute
                    )

        per_run_bolds is built by appending only on success (line 513).
        run_dicts retains all runs. Index mismatch occurs when any run
        in the middle of the list fails.
      </evidence>
      <impact>
        Applies the wrong brain mask during smoothing for rest tasks when
        a preceding run fails preprocessing. In fMRIPrep template space,
        per-run masks are typically identical, so the practical effect is
        negligible. However, if masks differ (e.g., due to dropout), the
        wrong spatial coverage would be applied. Additionally, the log
        and output file prefix (run_dicts[i]['run_label']) would reference
        the wrong run.
      </impact>
      <recommendation>
        Maintain a parallel list of mask paths and run labels for
        successfully processed runs, similar to per_run_bolds. For
        example, track per_run_labels alongside per_run_masks (already
        tracked) and use per_run_masks[i] instead of
        run_dicts[i]["mask_path"] in the smoothing loop.
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F10: VALID_TASK_LABELS not enforced in download_session_data      -->
    <!-- ================================================================ -->
    <finding id="F10" severity="note" category="robustness">
      <location file="orchestrate_first_level.py" lines="366-372" />
      <location file="orchestrator_utils.py" lines="194-196, 371-373" />
      <description>
        The VALID_TASK_LABELS whitelist check is enforced in
        _process_session() (line 366-372) during preprocessing. However,
        download_session_data() and discover_local_mmps_files() in
        orchestrator_utils.py do not validate against VALID_TASK_LABELS.
        They proceed to construct S3 keys and file patterns using whatever
        task_label is provided.

        In practice, the download step occurs BEFORE the whitelist check,
        so if an invalid task label is in the config, S3 requests will be
        issued (and fail with 404) before the OrchestratorError is raised
        during preprocessing. This is a wasted-effort issue, not a
        correctness issue.
      </description>
      <evidence>
        Execution order:
          Step 1 (line 310-315): download_session_data() — uses task_defs as-is
          Step 3 (line 341): discover_session_files() — uses task_defs as-is
          Step 4+ (line 366): VALID_TASK_LABELS check — first validation point
      </evidence>
      <impact>
        Negligible — invalid task labels would fail gracefully at download
        (no files found) and then explicitly at the whitelist check. The
        only cost is unnecessary S3 HEAD requests.
      </impact>
      <recommendation>
        Optionally move the VALID_TASK_LABELS check to
        load_orchestrator_config() during config validation, where all
        task_labels are already iterated. This catches invalid labels at
        startup rather than mid-processing.
      </recommendation>
    </finding>

    <!-- ================================================================ -->
    <!-- F11: Test coverage gap — status derivation tested on replica     -->
    <!-- ================================================================ -->
    <finding id="F11" severity="note" category="robustness">
      <location file="tests/test_cr_implementation.py" lines="590-634" />
      <description>
        TestSessionStatusDerivation tests a _derive_status() static method
        that is a hand-written replica of the production logic, not the
        actual production code. The tests verify that the replica works
        correctly, but do not verify that the two production locations
        (lines 175-182 and 737-744) remain consistent with each other or
        with the test replica.
      </description>
      <evidence>
        test_cr_implementation.py:590-600:
            @staticmethod
            def _derive_status(outcomes):
                # Replicate the session status derivation ...
                if not outcomes:
                    return "failed"
                ...
      </evidence>
      <impact>
        If either production location drifts from the tested logic, the
        tests will continue to pass while the production code is wrong.
      </impact>
      <recommendation>
        After extracting _derive_session_status() as a module-level
        function (per F4 recommendation), import and test the actual
        production function instead of the replica.
      </recommendation>
    </finding>

  </findings>

  <summary>
    <critical_count>2</critical_count>
    <major_count>2</major_count>
    <minor_count>4</minor_count>
    <note_count>3</note_count>
    <overall_assessment>conditionally_defensible</overall_assessment>
    <narrative>
      The C1-C10 implementation is well-structured and addresses the
      original CR findings faithfully. The two critical findings (F1:
      session wall time hardcoded to 0.0; F2: force_recompute broken for
      AFNI-based functions) are both implementation defects that do not
      affect the default production path (force_recompute defaults to
      false, and session wall time is a metadata field). However, both
      must be fixed before publication: F1 compromises the QC provenance
      record, and F2 renders an advertised feature non-functional.

      The major findings (F3: NaN derivative propagation; F4: status
      derivation duplication) are defensible in their current form — F3
      errs conservatively, and F4 is a maintenance risk rather than a
      current defect. F9 (per-run smoothing index mismatch) is a
      pre-existing bug exposed during this review that could cause
      incorrect mask application when smoothing is enabled and a run
      fails; it should be fixed alongside the C1-C10 changes.

      Overall: conditionally defensible. Fix F1 and F2 before proceeding
      to real-world testing. Strongly recommend also fixing F4 and F9.
    </narrative>
  </summary>

  <action_items>
    <item priority="P0" target_mode="implement" finding_ref="F1"
          description="Fix session_wall_time — track time at top of _process_session() and pass actual elapsed to consolidate_session_qc()" />
    <item priority="P0" target_mode="implement" finding_ref="F2"
          description="Fix force_recompute for AFNI-backed functions — delete existing output before AFNI call when force_recompute=True (5 functions: apply_brain_mask, remove_initial_trs_bold, concatenate_bolds, apply_smoothing, compute_mask_intersection)" />
    <item priority="P1" target_mode="implement" finding_ref="F4"
          description="Extract _derive_session_status() helper to eliminate status logic duplication" />
    <item priority="P1" target_mode="implement" finding_ref="F9"
          description="Fix per-run smoothing index mismatch — use per_run_masks[i] instead of run_dicts[i] in the rest smoothing loop" />
    <item priority="P2" target_mode="implement" finding_ref="F3"
          description="Optional: impute NaN before derivative computation and separate raw vs. propagated NaN in log message" />
    <item priority="P2" target_mode="implement" finding_ref="F6"
          description="Consolidate detect_non_steady_state_trs() call — compute n_remove once per run" />
    <item priority="P2" target_mode="implement" finding_ref="F8"
          description="Define __version__ module-level constant; reference in consolidate_session_qc()" />
    <item priority="P2" target_mode="test" finding_ref="F11"
          description="Update TestSessionStatusDerivation to test the actual production function after F4 refactor" />
    <item priority="P2" target_mode="implement" finding_ref="F10"
          description="Optional: validate task labels in load_orchestrator_config() for fail-fast at startup" />
  </action_items>

</cr_report>
```
