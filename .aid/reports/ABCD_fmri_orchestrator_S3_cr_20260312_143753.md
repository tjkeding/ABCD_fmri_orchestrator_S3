# Critical Review: ABCD_fmri_orchestrator_S3

```xml
<cr_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="cr" timestamp="2026-03-12T14:37:53Z" />

  <scope>
    Comprehensive review of the full orchestrator + upstream pipeline:
    - orchestrate_first_level.py (779 lines) — main entry point, session-centric workflow
    - orchestrator_utils.py (~2600 lines) — preprocessing, QC, config building, S3 I/O
    - fmri_first_level_proc v2.3.0 — upstream first-level analysis engine:
      - task_act_first_level.py — 3dDeconvolve GLM activation analysis
      - task_conn_first_level.py — 3dLSS beta series estimation + connectivity
      - rest_conn_first_level.py — 3dTproject nuisance regression + connectivity
      - first_level_utils.py — shared utilities (FD, censoring, HRF, extraction, connectivity)
      - first_level_config.py — config validation and Namespace construction
    - orch_config_final.yaml — orchestrator config for ABCD real-world testing
    - proc_config_final.yaml — fmri_first_level_proc analysis template
    - Real-world test results: 1 participant, 4 sessions, 11 analyses (v2.3.0)
  </scope>

  <findings>

    <!-- ================================================================== -->
    <!-- CRITICAL FINDINGS -->
    <!-- ================================================================== -->

    <finding id="F1" severity="critical" category="robustness">
      <location file="orchestrate_first_level.py" lines="605-633" />
      <description>
        Session-level success reporting does not propagate per-analysis failures.
        When an individual analysis (e.g., nback_act, rest_conn) raises an
        exception or exits non-zero, the error is caught and logged at lines
        617-623 and 629-633, but `_process_session()` does not raise or return
        an error indicator. The calling function `process_participant()` at line
        170 unconditionally records `session_results[session] = "success"`.

        In the v2.3.0 real-world test (ses-00A), rest_conn's 3dTproject failed
        for runs 3 and 4 (DOF exceeded), yet the session was reported as
        SUCCESS. While upstream handled this gracefully (skipped those runs,
        proceeded with runs 1-2), the orchestrator's summary provides no
        indication that 2 of 4 rest runs failed. At the group level across
        ~11,000 subjects, this would silently mask systematically degraded
        rest_conn data quality.
      </description>
      <evidence>
        Run log: ses-00A rest_conn runs 3-4 failed with "total number of fixed
        regressors (342) is too many for [259|165] retained time points", yet
        session summary reads "ses-00A: success". The `analysis_error` variable
        at line 614 is set but only used for QC JSON annotation — it does not
        affect session-level outcome.
      </evidence>
      <impact>
        Group-level analyses would include subjects with partially failed
        rest_conn pipelines without any flag in the session summary. Users
        relying on the session summary for quality gating (the intended use
        case for ~11,000 subjects) would unknowingly include degraded data.
        The QC JSON does capture the failure, but the summary log — the primary
        monitoring tool during batch processing — does not.
      </impact>
      <recommendation>
        Implement per-analysis outcome tracking within `_process_session()`:
        (1) Accumulate analysis results as a list of {name, status, error} dicts.
        (2) Propagate to `process_participant()` for inclusion in session_results.
        (3) Report per-analysis outcomes in the SESSION SUMMARY log block.
        (4) Optionally define a session-level success policy (e.g., "all analyses
        must pass" vs. "at least one analysis must pass") via config.

        The per-analysis QC JSON already captures `completed_successfully` and
        `error` fields — this finding concerns only the session-level summary
        log, which is the primary monitoring interface during batch runs.
      </recommendation>
    </finding>

    <finding id="F2" severity="critical" category="validity">
      <location file="orchestrator_utils.py" lines="1031-1058" />
      <description>
        Framewise displacement (FD) computation in the orchestrator's
        `compute_framewise_displacement()` uses Power et al. (2012) formula
        with a 50mm head radius, while the upstream `fmri_first_level_proc`
        generates censor files via AFNI's `1d_tool.py -censor_motion`, which
        uses AFNI's default 80mm (57.3mm effective arc-length conversion).
        These are two different FD calculations applied to the same data for
        different purposes:

        - Orchestrator FD (radius=50mm): used for QC metrics (mean/median/max
          FD, carpet plots, pct_censored in preproc QC JSON)
        - Upstream FD (AFNI 1d_tool.py, radius~80mm): used for actual censoring
          decisions that govern which TRs enter the GLM

        The FD values in the preproc QC JSON do not correspond to the censoring
        applied during analysis. A subject might show 25% censored in QC (at
        r=50mm) but have 15% actually censored (at AFNI's threshold), or vice
        versa. Group-level exclusion decisions based on QC metrics would be
        inconsistent with the censoring actually applied.
      </description>
      <evidence>
        orchestrator_utils.py line 1057: `radius_mm=50.0` (Power et al. default).
        AFNI 1d_tool.py source: uses `enorm` (Euclidean norm) with angular
        displacement scaled by 80mm/pi factor. These yield different FD values
        for identical motion parameters, especially for rotational components.
      </evidence>
      <impact>
        QC-based subject exclusion (e.g., "exclude if >30% censored") would
        use FD values computed with a different formula and radius than the
        censoring actually applied. This creates a systematic disconnect
        between quality gating and analysis.
      </impact>
      <recommendation>
        Harmonize the FD computation:
        Option A (preferred): Compute orchestrator QC FD using the same method
        as upstream (AFNI's enorm via 1d_tool.py), so QC metrics match actual
        censoring. This requires calling 1d_tool.py for QC as well.
        Option B: Parameterize `radius_mm` in the orchestrator config and set
        it to match AFNI's effective radius (approximately 57.3mm for the
        enorm metric). Document the discrepancy.
        Option C: Use upstream's censor file to derive QC censoring stats
        directly (n_censored = count of 0s in censor.1D), bypassing the
        orchestrator's independent FD computation for censoring statistics.
      </recommendation>
    </finding>

    <!-- ================================================================== -->
    <!-- MAJOR FINDINGS -->
    <!-- ================================================================== -->

    <finding id="F3" severity="major" category="robustness">
      <location file="orchestrate_first_level.py" lines="353-476" />
      <description>
        Per-run preprocessing failures are caught by a broad `except Exception`
        at line 470 and silently downgraded to warnings. Any unexpected error
        during Steps 4-10 (decompress, mask, QC, NSS removal, motion extraction,
        tissue extraction, timing formatting) results in the run being skipped
        with a warning. If 1 of 2 task runs fails, the pipeline proceeds with
        a single run — no concatenation is needed, and the analysis runs on
        half the expected data. No explicit warning is issued about reduced
        statistical power from run loss.
      </description>
      <evidence>
        Lines 470-476: `except Exception as e: logger.warning("Skipping run '%s'
        for task '%s' — unexpected error: %s", ...); skipped_runs.append(...);
        continue`. The warning at line 478-484 reports skipped runs but does
        not quantify the impact on statistical power.
      </evidence>
      <impact>
        For the ABCD n-back task (2 runs), losing 1 run halves the available
        data. The GLM proceeds but with substantially reduced power and
        potentially biased estimates if the surviving run is not representative.
        At scale (~11,000 subjects), some subjects may have systematically
        fewer runs than others, creating heterogeneous data quality that
        complicates group-level inference.
      </impact>
      <recommendation>
        (1) Log a structured warning when runs are lost (e.g., "Task 'nback'
        proceeding with 1 of 2 runs — statistical power is substantially
        reduced").
        (2) Record run survival count in the QC JSON for post-hoc filtering.
        (3) Consider a configurable minimum-run policy (e.g., "require at least
        N runs for task X") that raises OrchestratorError if violated.
      </recommendation>
    </finding>

    <finding id="F4" severity="major" category="assumptions">
      <location file="orchestrator_utils.py" lines="1118-1121" />
      <description>
        Motion parameter rotation-to-radian conversion assumes input rotations
        are in degrees. Line 1120: `motion_array[:, 3:] = np.deg2rad(...)`.
        This assumption is not validated against the actual motion.tsv file
        header or content. If the mmps_mproc pipeline changes its output
        convention (e.g., to radians), all downstream motion regressors and
        FD computations would be silently corrupted by a factor of pi/180.
      </description>
      <evidence>
        Line 1120: unconditional deg2rad conversion. No header inspection of
        the motion.tsv to confirm unit convention. The mmps_mproc documentation
        specifies degrees, but this is not programmatically enforced.
      </evidence>
      <impact>
        If rotations are already in radians, the conversion would produce
        values ~57x too small, making rotational FD contributions negligible.
        Censoring would become too permissive for rotational motion. This is
        a latent vulnerability — currently correct but fragile.
      </impact>
      <recommendation>
        Add a plausibility check: if max(abs(rot_columns)) < 0.1, log a
        warning that rotations may already be in radians (typical head rotation
        during fMRI is 0.5-5 degrees; values < 0.1 degrees across all TRs
        would be unusually low and suggest radian input). Alternatively,
        inspect the mmps_mproc metadata/header for unit specification.
      </recommendation>
    </finding>

    <finding id="F5" severity="major" category="validity">
      <location file="proc_config_final.yaml" lines="65" />
      <description>
        The n-back activation analysis uses `hrf_model: "dmBLOCK"` (duration-
        modulated block model), which requires married timing (onset*duration
        pairs in AFNI format). The orchestrator's `format_task_timing()`
        (orchestrator_utils.py:1293) produces a CSV with CONDITION, ONSET,
        DURATION columns, and upstream's `get_stim_data()` converts these to
        AFNI onset files.

        The dmBLOCK model estimates a separate amplitude for each trial's
        duration, which is appropriate for variable-duration events but
        introduces additional parameters into the design matrix. For the ABCD
        n-back task, where block durations are relatively fixed within
        condition, dmBLOCK may be overparameterized relative to a simpler
        BLOCK model.

        More critically, the n-back connectivity analysis (line 172) uses
        `hrf_model: "GAM"`. Using different HRF models for activation and
        connectivity analyses of the same task creates an interpretive
        inconsistency: the activation maps reflect dmBLOCK-convolved
        responses while the beta series reflect GAM-convolved responses.
        Contrasts defined identically across both analyses will capture
        different aspects of the hemodynamic response.
      </description>
      <evidence>
        proc_config_final.yaml line 65: nback_act uses `hrf_model: "dmBLOCK"`.
        Line 172: nback_conn uses `hrf_model: "GAM"`. These produce different
        design matrices for the same stimulus events.
      </evidence>
      <impact>
        Activation and connectivity results for the same task are not directly
        comparable due to different HRF assumptions. This is a defensible
        design choice (dmBLOCK is standard for activation, GAM is standard
        for beta series), but it must be explicitly documented and justified
        in any publication. A reviewer could argue that the HRF inconsistency
        undermines the coherence of the analysis pipeline.
      </impact>
      <recommendation>
        Document the HRF model choice rationale in the methods section:
        "Activation analyses used dmBLOCK to accommodate variable block
        durations; beta series estimation used GAM (canonical HRF) following
        Rissman et al. (2004) and Mumford et al. (2012) conventions."
        This is a known and accepted practice in the literature but requires
        explicit justification.
      </recommendation>
    </finding>

    <finding id="F6" severity="major" category="robustness">
      <location file="rest_conn_first_level.py" lines="180-200" />
      <description>
        The rest_conn 3dTproject nuisance regression uses `-bandpass [low] [high]`
        with `-polort 2` simultaneously. AFNI's 3dTproject documentation warns
        that combining bandpass filtering with polynomial detrending can create
        ill-conditioned design matrices because the bandpass regressors
        (sinusoidal basis set) overlap with the polynomial trend regressors.

        In the real-world test, ses-00A rest runs 3-4 failed with "total number
        of fixed regressors (342) is too many for [259|165] retained time
        points." The 342 regressors include 324 stopband regressors (from
        bandpass) + 3 polynomial + 15 nuisance orts. The stopband regressor
        count (324 for 377 TRs) is driven by the narrow passband (0.01-0.1 Hz
        at TR=0.8s, Nyquist=0.625 Hz). This means ~86% of the frequency
        spectrum is being regressed out, requiring nearly as many regressors
        as timepoints.

        When combined with aggressive censoring (31-56% of TRs), the retained
        timepoints drop below the regressor count, causing the DOF failure.
        This is not a bug but a mathematical constraint that will affect a
        non-trivial proportion of ABCD subjects.
      </description>
      <evidence>
        Run log: "Block #0: 377 time points -- 324 stopband regressors" +
        "3 polort regressors" + "15 other fixed ort regressors" = 342 total.
        For run 3: 377-118=259 retained, 342>259 → failure.
        For run 4: 379-214=165 retained, 342>165 → failure.
        Stopband count formula: n_stopband ≈ 2 × (Nyquist - high_freq) ×
        n_TRs × TR ≈ 2 × (0.625-0.1) × 377 × 0.8 ≈ 316 (close to 324).
      </evidence>
      <impact>
        Any rest run with >~10-15% censoring at TR=0.8s and bandpass=[0.01,0.1]
        will approach or exceed the DOF limit. For the ABCD dataset, where
        rest-run censoring rates of 20-50% are common in children, a substantial
        fraction of runs will fail this constraint. The upstream skip-low-DOF
        logic handles this gracefully, but the resulting connectivity matrices
        are computed from a subset of available runs, introducing heterogeneous
        data quality.
      </impact>
      <recommendation>
        (1) Document the expected run-failure rate for the ABCD sample and
        the implications for group-level connectivity analyses.
        (2) Consider using `-polort -1` (no polynomial detrending) when
        bandpass is active, as the bandpass filter implicitly removes low-
        frequency drift. AFNI documentation suggests this is acceptable.
        (3) Record the number of surviving runs per subject in the QC summary
        for post-hoc quality gating (e.g., "exclude subjects with fewer
        than 2 surviving rest runs").
        (4) Consider raising the bandpass lower bound (e.g., 0.008 Hz) or
        widening the passband to reduce stopband regressor count.
      </recommendation>
    </finding>

    <finding id="F7" severity="major" category="assumptions">
      <location file="orchestrator_utils.py" lines="1246-1278" />
      <description>
        The n-back cue relabeling logic (`fix_nback_cue_labels()`) makes a
        critical assumption about the ABCD n-back task structure: that
        0-back cues are "passive viewing" events (relabeled to bare condition
        names like "posface"), while 2-back cues are "instruction" screens.

        This relabeling transforms the cue events from nuisance into task-
        relevant conditions, fundamentally altering the design matrix. If the
        assumption about the psychological nature of cues is incorrect (e.g.,
        if 0-back cues are attentional cues rather than passive viewing), the
        resulting condition estimates will conflate two distinct cognitive
        processes.
      </description>
      <evidence>
        Lines 1273-1278: `if level == "0": events_df.at[i, condition_column] =
        condition` (bare condition name) `else: events_df.at[i,
        condition_column] = "instruction"`. This converts generic "cue" rows
        into either stimulus conditions or "instruction" based on the n-back
        level of the subsequent trial block.
      </evidence>
      <impact>
        If the relabeling assumption is incorrect, condition-level activation
        estimates will be biased. The "posface" activation map, for example,
        would include both the 0-back cue presentation and the 0-back recall
        trials, mixing two potentially distinct neural responses.
      </impact>
      <recommendation>
        (1) Cite the specific ABCD study protocol document that defines the
        0-back cue as passive viewing of the target stimulus.
        (2) Consider running a sensitivity analysis with cues excluded entirely
        (labeled as nuisance) to verify that the relabeling does not materially
        alter the activation pattern.
        (3) Document the relabeling decision and its justification in any
        publication methods section.
      </recommendation>
    </finding>

    <finding id="F8" severity="major" category="reproducibility">
      <location file="orchestrator_utils.py" lines="870-905" />
      <description>
        Multiple preprocessing functions include early-exit caching that
        returns a previously-computed result if the output file already exists
        on disk. Examples include: `apply_brain_mask()` (line 882),
        `extract_motion_regressors()` (line 1090), `extract_tissue_signals()`
        (line 1168), `format_task_timing()` (line 1306), `concatenate_bolds()`
        (line 1377), and others. While caching avoids redundant computation,
        it creates a reproducibility hazard: if input data or parameters change
        between runs (e.g., a re-extraction of fMRIPrep data, a change in
        `calc_n_motion_derivs`), stale cached outputs will be silently reused.

        No invalidation mechanism (e.g., hash-based cache keys, parameter
        fingerprinting) exists to detect parameter-data mismatch.
      </description>
      <evidence>
        apply_brain_mask() line 882: `if os.path.isfile(out_path):
        logger.info("Masked BOLD already exists: %s", out_path); return
        out_path`. No check that `bold_path` or `mask_path` match the cached
        result. Same pattern in 10+ functions.
      </evidence>
      <impact>
        Re-running the pipeline with different parameters (e.g., changing
        `calc_n_motion_derivs` from 1 to 2) would silently use motion files
        computed with the old parameter. This is a correctness risk during
        iterative development and testing. The current mitigation (manually
        deleting output directories between runs) is fragile.
      </impact>
      <recommendation>
        (1) Document the caching behavior and the requirement to delete output
        directories when parameters change.
        (2) Add a `--force-recompute` flag that bypasses all file-existence
        checks.
        (3) Consider encoding key parameters into the output filename (e.g.,
        `_motion_deriv1.1D`) to prevent parameter-mismatch reuse.
      </recommendation>
    </finding>

    <!-- ================================================================== -->
    <!-- MINOR FINDINGS -->
    <!-- ================================================================== -->

    <finding id="F9" severity="minor" category="validity">
      <location file="orchestrator_utils.py" lines="512-513" />
      <description>
        The concatenation mask selection uses the mask from the first
        successfully processed run (`concat_mask = per_run_masks[0]`) for
        smoothing operations. If run-specific brain masks differ (e.g., due
        to different head positions across runs), the first run's mask may
        not be representative of the full concatenated dataset. Mask coverage
        could exclude or include voxels that are brain in some runs but not
        others.
      </description>
      <evidence>
        Line 513: `concat_mask = per_run_masks[0]`. No intersection or union
        operation across per-run masks.
      </evidence>
      <impact>
        Low impact in practice: fMRIPrep masks are computed in template space
        and should be consistent across runs. However, edge cases (e.g.,
        susceptibility-induced signal dropout varying across runs) could
        introduce small mask inconsistencies.
      </impact>
      <recommendation>
        Use the intersection of per-run masks for smoothing to ensure only
        voxels consistently within the brain are included. This is conservative
        but safe. Alternatively, document the current behavior.
      </recommendation>
    </finding>

    <finding id="F10" severity="minor" category="assumptions">
      <location file="orchestrator_utils.py" lines="1143-1144" />
      <description>
        NaN values in motion parameters are replaced with 0.0 (`np.nan_to_num
        (motion_data, nan=0.0)`). While this prevents downstream failures,
        it introduces artificial zero-motion TRs that could affect motion
        derivative computation and FD-based censoring. A NaN in the motion
        data likely indicates a tracking failure, which should trigger
        censoring rather than imputation.
      </description>
      <evidence>
        Line 1144: `motion_data = np.nan_to_num(motion_data, nan=0.0)`.
        No check for NaN prevalence or logging of how many values were
        imputed.
      </evidence>
      <impact>
        TRs with tracking failures would appear as zero-motion frames,
        potentially surviving censoring when they should be removed.
      </impact>
      <recommendation>
        (1) Log the count and location of NaN-imputed values.
        (2) Consider flagging NaN-containing TRs for censoring rather than
        imputing with zero.
        (3) Raise an error if NaN prevalence exceeds a threshold (e.g., >5%).
      </recommendation>
    </finding>

    <finding id="F11" severity="minor" category="generalizability">
      <location file="orchestrate_first_level.py" lines="325" />
      <description>
        Task type detection uses a string prefix check: `is_rest =
        task_label.lower().startswith("rest")`. This is fragile and would
        misclassify tasks named "resting_state", "restingstate", or
        "RestEyes" unless they start with "rest". Conversely, a hypothetical
        task named "restoration" would be misclassified as resting-state.
      </description>
      <evidence>
        Line 325: `is_rest = task_label.lower().startswith("rest")`.
      </evidence>
      <impact>
        Low impact for ABCD (task labels are "nback" and "rest"), but limits
        portability to other datasets.
      </impact>
      <recommendation>
        Use an explicit `is_rest: true/false` field in the task definition
        YAML rather than inferring from the task label string. This is more
        robust and self-documenting.
      </recommendation>
    </finding>

    <finding id="F12" severity="minor" category="robustness">
      <location file="orchestrator_utils.py" lines="201-241" />
      <description>
        S3 events/motion file probing uses a sequential scan from run 1-9,
        stopping at the first missing run (`break` at line 225). If a subject
        has runs 1 and 3 but not run 2 (e.g., due to data quality exclusion
        at the source), run 3 would never be discovered. The "stop at first
        gap" assumption may not hold for all data organizations.
      </description>
      <evidence>
        Lines 224-225: `if code in ("404", "NoSuchKey"): break`.
      </evidence>
      <impact>
        Low impact if ABCD run numbering is always contiguous, but would
        silently miss data in datasets with non-contiguous run indices.
      </impact>
      <recommendation>
        Continue probing all 9 runs regardless of gaps, collecting all that
        exist. Alternatively, use S3 list_objects to discover runs by prefix
        rather than probing fixed indices.
      </recommendation>
    </finding>

    <finding id="F13" severity="minor" category="validity">
      <location file="proc_config_final.yaml" lines="121" />
      <description>
        The contrast label "place_tback-z-back" (line 121) contains a typo:
        "z-back" should be "zback" (hyphen vs. no hyphen). The contrast
        function at line 90 is correctly specified as
        `1*2_back_place-1*0_back_place`, so the label is cosmetic only and
        does not affect computation. However, inconsistent labeling could
        cause confusion during extraction or group-level analysis.
      </description>
      <evidence>
        Line 121: `"place_tback-z-back"` vs. line 117 pattern: `"ave_tback-ave_zback"`.
      </evidence>
      <impact>
        Cosmetic; does not affect computation but could confuse downstream
        label matching if extraction labels reference this contrast by name.
      </impact>
      <recommendation>
        Correct to `"place_tback-zback"` for consistency with other labels.
      </recommendation>
    </finding>

    <finding id="F14" severity="minor" category="assumptions">
      <location file="first_level_utils.py" lines="529-600" />
      <description>
        The upstream FD-based censoring via AFNI's 1d_tool.py uses only the
        base 6 motion parameters (translations + rotations) for censor file
        generation. The motion derivatives computed by the orchestrator
        (calc_n_motion_derivs=1, yielding 12 columns) are included as
        nuisance regressors in the GLM but are NOT used for censoring
        decisions. This is standard practice (Power et al. 2012 defines FD
        on raw displacements), but it means that rapid acceleration/jerk
        patterns visible only in derivatives do not trigger censoring.
      </description>
      <evidence>
        first_level_utils.py line 531: `prepare_motion_file(motion_path, 6, ...)`
        extracts only the first 6 columns for censor computation.
      </evidence>
      <impact>
        Low practical impact — FD on raw displacements is the established
        standard. Noted for completeness and reviewer anticipation.
      </impact>
      <recommendation>
        Document in methods: "Censoring was based on framewise displacement
        computed from raw displacement parameters (Power et al. 2012);
        temporal derivatives were included as nuisance regressors but did not
        contribute to censoring decisions."
      </recommendation>
    </finding>

    <finding id="F15" severity="minor" category="validity">
      <location file="proc_config_final.yaml" lines="73-86" />
      <description>
        Several n-back activation contrasts use coefficient 0.3333 (lines
        74-86), which is 1/3 rounded to 4 decimal places. The sum of three
        0.3333 coefficients is 0.9999, not 1.0000. While the deviation is
        negligible for practical purposes, it means these contrasts are not
        exactly unit-weighted averages. AFNI may internally renormalize, but
        this is not guaranteed for -gltsym specifications.
      </description>
      <evidence>
        Line 74: `"0.3333*negface+0.3333*0_back_negface+0.3333*2_back_negface"`
        → sum = 0.9999. The mathematically exact coefficient would be
        0.333333... (repeating).
      </evidence>
      <impact>
        Negligible — the 0.01% deviation from unit weighting is well below
        any practical significance threshold. Noted for mathematical precision.
      </impact>
      <recommendation>
        Use higher precision (e.g., 0.333333) or normalize post-hoc.
        Alternatively, document the rounding convention.
      </recommendation>
    </finding>

    <finding id="F16" severity="minor" category="reproducibility">
      <location file="orchestrate_first_level.py" lines="597-599" />
      <description>
        The pipeline changes the working directory to `session_out` (line 599:
        `os.chdir(session_out)`) before running first-level analyses to prevent
        AFNI's `3dDeconvolve.err` files from colliding across analyses. This
        is restored in a `finally` block (line 653). However, if the Python
        process is killed (SIGKILL, OOM) between `os.chdir` and the `finally`
        block, subsequent pipeline invocations would run from an unexpected
        working directory.

        More importantly, `os.chdir()` is process-global and not thread-safe.
        If parallelism is ever added within a single process (e.g., concurrent
        session processing), this would cause race conditions.
      </description>
      <evidence>
        Lines 598-599: `original_dir = os.getcwd(); os.chdir(session_out)`.
        Line 653: `os.chdir(original_dir)` in finally block.
      </evidence>
      <impact>
        Low impact in the current single-threaded, single-session-at-a-time
        architecture. Would become a correctness issue if the architecture
        changes to multi-threaded processing.
      </impact>
      <recommendation>
        Use AFNI's `-overwrite` flag or explicitly specify output paths for
        3dDeconvolve.err (e.g., via stderr redirection) to avoid the need
        for working directory manipulation. Alternatively, document the
        single-threaded constraint.
      </recommendation>
    </finding>

    <!-- ================================================================== -->
    <!-- NOTES -->
    <!-- ================================================================== -->

    <finding id="F17" severity="note" category="validity">
      <location file="proc_config_final.yaml" lines="26" />
      <description>
        `force_diff_atlas: true` bypasses the atlas space validation between
        the parcellation template and the functional data. While this is
        necessary when the template and fMRIPrep outputs use slightly different
        space naming conventions (e.g., "MNI" vs "MNI152NLin2009cAsym"), it
        disables a safety check that would catch genuine space mismatches.
      </description>
      <evidence>
        Line 26: `force_diff_atlas: true`. The template is a Schaefer+Tian
        atlas in MNI152NLin2009cAsym space, matching the fMRIPrep output
        space — the override is needed only because of naming convention
        differences in 3dinfo.
      </evidence>
      <impact>
        Low risk given that both template and data are in the same actual
        space. The flag appropriately handles the known AFNI/fMRIPrep naming
        discrepancy.
      </impact>
      <recommendation>
        Document why `force_diff_atlas` is enabled and which spaces are being
        matched. Consider adding a logging message in the upstream validation
        that reports the actual space strings being compared.
      </recommendation>
    </finding>

    <finding id="F18" severity="note" category="generalizability">
      <location file="orchestrate_first_level.py" lines="157" />
      <description>
        Session label construction hard-codes the "A" suffix: `ses_label =
        f"ses-{session}A"`. This is ABCD-specific (sessions are labeled
        "ses-00A", "ses-02A", etc.). Other datasets using this orchestrator
        would need to modify this logic.
      </description>
      <evidence>
        Line 157: `ses_label = f"ses-{session}A"`. Also present in
        orchestrator_utils.py at multiple locations.
      </evidence>
      <impact>
        None for ABCD usage. Limits portability to other studies.
      </impact>
      <recommendation>
        Consider making the session label format configurable (e.g., a
        `session_label_template: "ses-{code}A"` field in the config) for
        future generalizability.
      </recommendation>
    </finding>

    <finding id="F19" severity="note" category="assumptions">
      <location file="rest_conn_first_level.py" lines="265-267" />
      <description>
        The `notch_filter_band: [0.31, 0.43]` in proc_config_final.yaml
        targets respiratory artifact removal (Fair et al. 2020). This
        frequency range assumes a respiratory rate of ~18.6-25.8 breaths/min,
        which is appropriate for the general adolescent population in ABCD.
        However, subjects with atypical respiratory rates (e.g., anxious
        subjects with rapid breathing >30 bpm, or athletes with slow
        breathing <15 bpm) may not have their respiratory artifacts fully
        removed.
      </description>
      <evidence>
        proc_config_final.yaml line 266: `notch_filter_band: [0.31, 0.43]`.
        0.31 Hz = 18.6 bpm, 0.43 Hz = 25.8 bpm.
      </evidence>
      <impact>
        Low — the chosen band covers the modal respiratory rate for
        adolescents. Subjects outside this range would retain respiratory
        artifacts, which would manifest as increased noise but not
        systematic bias.
      </impact>
      <recommendation>
        Document the respiratory rate assumption and note that the notch
        filter band may need adjustment for populations with different
        respiratory characteristics.
      </recommendation>
    </finding>

  </findings>

  <summary>
    <critical_count>2</critical_count>
    <major_count>6</major_count>
    <minor_count>8</minor_count>
    <overall_assessment>conditionally_defensible</overall_assessment>

    The pipeline is methodologically sound in design, implementing established
    approaches (Power et al. FD censoring, 3dDeconvolve GLM, 3dLSS beta series,
    3dTproject nuisance regression, 3dNetCorr connectivity). The architecture is
    well-structured with comprehensive config validation, input checking, and
    error handling.

    The two critical findings require remediation before production use:
    (1) Session-level success reporting must propagate per-analysis outcomes to
    prevent silent quality degradation at scale; (2) FD metric inconsistency
    between QC reporting and actual censoring must be harmonized to ensure
    group-level quality gating is internally consistent.

    The major findings are primarily documentation requirements and robustness
    improvements that would strengthen the pipeline's defensibility under peer
    review but do not represent fundamental methodological errors.

    After addressing the critical findings and documenting the major findings,
    the pipeline would be defensible for publication in a high-impact journal.
  </summary>

  <action_items>
    <item priority="P0" target_mode="implement" finding_ref="F1" description="Implement per-analysis outcome tracking in session summary: accumulate analysis results, propagate to session_results, report in SESSION SUMMARY log block" />
    <item priority="P0" target_mode="implement" finding_ref="F2" description="Harmonize FD computation between orchestrator QC and upstream censoring (prefer Option C: derive QC censoring stats from upstream censor file)" />
    <item priority="P1" target_mode="implement" finding_ref="F3" description="Add structured run-survival warnings and record run count in QC JSON" />
    <item priority="P1" target_mode="implement" finding_ref="F8" description="Add --force-recompute flag to bypass file-existence caching" />
    <item priority="P1" target_mode="implement" finding_ref="F13" description="Fix typo: 'place_tback-z-back' -> 'place_tback-zback' in proc_config_final.yaml" />
    <item priority="P1" target_mode="implement" finding_ref="F10" description="Log NaN count in motion parameters and consider flagging NaN TRs for censoring" />
    <item priority="P1" target_mode="document" finding_ref="F5" description="Document HRF model choice rationale (dmBLOCK for activation, GAM for beta series)" />
    <item priority="P1" target_mode="document" finding_ref="F6" description="Document expected rest_conn run-failure rate and DOF constraint at TR=0.8s" />
    <item priority="P1" target_mode="document" finding_ref="F7" description="Document n-back cue relabeling assumption with ABCD protocol citation" />
    <item priority="P2" target_mode="implement" finding_ref="F4" description="Add rotation-unit plausibility check in extract_motion_regressors()" />
    <item priority="P2" target_mode="implement" finding_ref="F9" description="Use intersection of per-run masks for concatenated-task smoothing" />
    <item priority="P2" target_mode="implement" finding_ref="F11" description="Add explicit is_rest field to task definition YAML" />
    <item priority="P2" target_mode="implement" finding_ref="F12" description="Continue S3 probing past gaps instead of stopping at first missing run" />
    <item priority="P2" target_mode="document" finding_ref="F14" description="Document that FD censoring uses raw displacements only (not derivatives)" />
    <item priority="P2" target_mode="document" finding_ref="F17" description="Document force_diff_atlas rationale" />
    <item priority="P2" target_mode="document" finding_ref="F19" description="Document respiratory rate assumption for notch filter band" />
  </action_items>

</cr_report>
```
