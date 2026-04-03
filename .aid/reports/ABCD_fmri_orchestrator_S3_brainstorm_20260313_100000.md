# Brainstorm: Critical Review Findings F3–F19 Disposition

```xml
<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-03-13T10:00:00Z" />

  <context_files>
    <file path="cr_history/ABCD_fmri_orchestrator_S3_cr_20260312_143753.md" relevance="Source CR report with 19 findings (2 critical, 6 major, 8 minor, 3 notes)" />
    <file path="brainstorm_history/ABCD_fmri_orchestrator_S3_brainstorm_20260312_150000.md" relevance="Prior brainstorm session — F1-F2 decided, F3 partially discussed, QC enrichment decided" />
    <file path="orchestrate_first_level.py" relevance="Main orchestrator — session success reporting, per-run preprocessing, analysis dispatch, os.chdir, is_rest check" />
    <file path="orchestrator_utils.py" relevance="Core utilities — FD computation, motion extraction (deg2rad, NaN imputation), mask selection, file-existence caching, S3 probing, cue relabeling, task timing" />
    <file path="proc_config_final.yaml" relevance="Analysis config — HRF models (dmBLOCK/GAM), polort, notch_filter_band, force_diff_atlas, contrast labels/coefficients" />
    <file path="orch_config_final.yaml" relevance="Orchestrator config — task definitions, session labels, cleanup settings" />
    <file path="fmri_first_level_proc/rest_conn_first_level.py" relevance="Upstream rest_conn — 3dTproject bandpass + polort interaction, DOF constraint, per-run processing architecture" />
    <file path="fmri_first_level_proc/first_level_utils.py" relevance="Upstream utilities — 1d_tool.py censoring (base 6 params only), motion file preparation" />
  </context_files>

  <topics>

    <!-- ================================================================== -->
    <!-- F3: BROAD EXCEPTION CATCH / MINIMUM-RUN GATE -->
    <!-- ================================================================== -->

    <topic id="T1" title="F3: Broad exception catch and minimum-run policy">
      <summary>
        Per-run preprocessing failures are caught by a broad `except Exception`
        at orchestrate_first_level.py:470 and silently downgraded to warnings.
        The run is skipped and processing continues with surviving runs. The CR
        raised concern about (1) lack of structured run-loss warnings with
        power implications, (2) run survival counts in QC, and (3) whether a
        configurable minimum-run policy should gate analysis execution.

        Sub-question (2) is already addressed by the QC enrichment decision
        from the prior brainstorm session (consolidated QC JSON includes
        runs_attempted, runs_succeeded, per_run_status).
      </summary>
      <approaches>
        <approach id="A1" label="No minimum-run gate" feasibility="high" risk="low">
          <description>
            Process whatever runs survive preprocessing. Record run survival
            counts in the consolidated QC JSON. Group-level exclusion criteria
            (e.g., "require >= 2 runs for n-back") are applied post-hoc during
            group-level analysis setup, where they can be tuned, documented,
            and applied consistently across all ~11,000 subjects.

            Add a structured run-loss warning to the existing skipped-runs log
            message (lines 478-484) that explicitly states the surviving vs.
            total run count for the task.
          </description>
          <pros>
            Maximizes data collection; consistent with pipeline philosophy
            (process everything, screen post-hoc); avoids discarding partial
            data that may still contribute at group level; exclusion criteria
            remain tunable rather than baked into processing
          </pros>
          <cons>
            Single-run GLM estimates have lower reliability; included in
            downstream analyses unless explicitly filtered by QC
          </cons>
        </approach>
        <approach id="A2" label="Configurable minimum-run gate" feasibility="high" risk="med">
          <description>
            Add a per-task `min_runs` config parameter. If surviving runs fall
            below the threshold, skip the analysis entirely and record the
            reason in QC.
          </description>
          <pros>Prevents low-power analyses from entering the pipeline</pros>
          <cons>
            Discards data at processing time; exclusion criteria become
            immutable once processing is complete; harder to re-run with
            different thresholds without full reprocessing
          </cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">
        No minimum-run gate. Process whatever survives; run survival counts
        recorded in QC JSON; group-level exclusion handles filtering. Add a
        structured run-loss warning to the existing skipped-runs log message.
        This is consistent with the pipeline's design philosophy: the
        orchestrator is agnostic to data quality — all screening happens
        post-hoc at group level using QC logs.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F4: ROTATION UNIT ASSUMPTION -->
    <!-- ================================================================== -->

    <topic id="T2" title="F4: Rotation unit validation in extract_motion_regressors()">
      <summary>
        orchestrator_utils.py:1120 unconditionally applies np.deg2rad() to
        rotation columns from motion.tsv. No programmatic validation that
        rotations are in degrees. If the upstream mmps_mproc pipeline changed
        output convention to radians, all rotational motion regressors and FD
        contributions would be silently corrupted by a factor of pi/180.

        Discussion focused on finding a definitive diagnostic rather than a
        heuristic plausibility threshold. Physical constraints of MRI head
        coils provide hard boundaries that unambiguously distinguish degree
        and radian conventions.
      </summary>
      <approaches>
        <approach id="A3" label="Plausibility heuristic (threshold-based warning)" feasibility="high" risk="med">
          <description>
            If max(abs(rot_columns)) &lt; 0.1, log a warning that rotations
            may already be in radians. Threshold chosen because typical head
            rotation is 0.5-5 degrees, making max &lt; 0.1 degrees implausible.
          </description>
          <pros>Simple; low implementation cost</pros>
          <cons>
            Heuristic, not definitive; threshold is somewhat arbitrary;
            "plausible" vs. "implausible" is a judgment call, not a physical
            impossibility
          </cons>
        </approach>
        <approach id="A4" label="Two-tier definitive check using physical constraints" feasibility="high" risk="low">
          <description>
            Exploit the physical impossibility of large rotations inside an
            MRI head coil. The head coil restricts rotation to approximately
            +/- 15-20 degrees maximum.

            Tier 1 — Definitive pass:
              if max(abs(rot_columns)) > 1.0: data MUST be in degrees.
              Rationale: 1.0 radian = 57.3 degrees, physically impossible
              inside a head coil. 1.0 degree is trivially common in any
              real fMRI scan.

            Tier 2 — Ambiguous, hard error:
              if max(abs(rot_columns)) &lt;= 1.0: raise OrchestratorError.
              Rationale: across hundreds of TRs, never exceeding 1 degree
              of rotation in any axis is extraordinarily unlikely for real
              data. If values are this small, they are likely already in
              radians (max 0.35 rad = 20 degrees is plausible). The check
              cannot definitively determine the unit, so it halts and
              requires manual inspection rather than guessing.

            The 1.0 threshold has zero ambiguity in the definitive-pass case:
            no subject can rotate 57.3 degrees in a head coil, yet virtually
            every scan will show > 1.0 degree of rotation somewhere.
          </description>
          <pros>
            Definitive rather than heuristic; uses physical impossibility,
            not statistical plausibility; zero false positives in normal
            operation; hard error on ambiguity prevents silent corruption
          </pros>
          <cons>
            Hard error in the ambiguous case halts processing; requires
            manual inspection for edge cases (though such cases are
            vanishingly unlikely with real data)
          </cons>
          <statistical_considerations>
            The probability of max(abs(rotation)) &lt;= 1.0 degrees across
            an entire fMRI scan (200-400+ TRs) is negligible for real data.
            Even highly compliant adult subjects exhibit rotational
            displacements exceeding 1 degree at some point during a scan.
            For the ABCD pediatric/adolescent sample, this probability is
            effectively zero.
          </statistical_considerations>
        </approach>
      </approaches>
      <decision status="decided" chosen="A4">
        Two-tier definitive check. If max(abs(rot)) > 1.0, data is
        definitively in degrees — proceed with deg2rad. If max(abs(rot))
        &lt;= 1.0, raise OrchestratorError requiring manual inspection.
        Implementation: ~10 lines in extract_motion_regressors(), inserted
        immediately after base column extraction and before deg2rad
        conversion.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F5: HRF MODEL INCONSISTENCY DOCUMENTATION -->
    <!-- ================================================================== -->

    <topic id="T3" title="F5: HRF model choice documentation (dmBLOCK vs. GAM)">
      <summary>
        proc_config_final.yaml specifies dmBLOCK for n-back activation
        (3dDeconvolve) and GAM for n-back connectivity (3dLSS beta series).
        Different HRF models for the same task events produce different
        design matrices, making activation and connectivity results not
        directly comparable. The CR flagged this as a potential
        interpretive inconsistency requiring justification.

        Discussion confirmed this is standard practice, not a deficiency:
        dmBLOCK is the established model for activation analyses with
        variable-duration events (accommodates block duration), while GAM
        (canonical gamma HRF) is the standard basis for least-squares-
        separate beta series estimation (Rissman et al. 2004, Mumford
        et al. 2012). Duration modulation in beta series estimation is
        non-standard and would introduce degrees of freedom the LSS
        estimator is not designed to handle. The two analyses answer
        fundamentally different questions (mean activation vs. trial-by-
        trial covariance), so different HRF models are methodologically
        coherent.
      </summary>
      <approaches>
        <approach id="A5" label="Documentation only" feasibility="high" risk="low">
          <description>
            Document the HRF model choice rationale in methods documentation
            and README. Cite Rissman et al. (2004) and Mumford et al. (2012)
            for beta series GAM convention; cite AFNI documentation and
            standard practice for dmBLOCK in activation analyses with
            married timing. One to two sentences in a methods section.
          </description>
          <pros>No code change; addresses reviewer concerns preemptively</pros>
          <cons>None</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A5">
        Documentation only. Cite Rissman et al. (2004) and Mumford et al.
        (2012) for GAM in beta series; AFNI conventions for dmBLOCK in
        activation. Note that activation and connectivity analyses answer
        different questions and the HRF model choice is standard for each.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F6: REST_CONN DOF CONSTRAINT -->
    <!-- ================================================================== -->

    <topic id="T4" title="F6: rest_conn DOF constraint at TR=0.8s (polort + bandpass)">
      <summary>
        3dTproject bandpass [0.01, 0.1] at TR=0.8s generates ~324 stopband
        regressors for ~377 TRs via regression-based filtering. Combined
        with polort 2 (3 regressors) and 15 nuisance orts, total fixed
        regressor count (~342) leaves only ~35 DOF. Any run with >10%
        censoring exceeds DOF and fails.

        Detailed mathematical analysis of the bandpass implementation:
        3dTproject constructs sin/cos basis functions spanning the stopband.
        The stopband fraction is fixed at 1 - 2*(high-low)*TR = 0.856,
        meaning ~85.6% of TRs become stopband regressors regardless of
        total scan length. This ratio is invariant to concatenation.

        Polort redundancy: when bandpass is active, stopband regressors
        already capture frequencies below 0.01 Hz, making polynomial
        detrending (polort) redundant. The DC component (0 Hz) is not
        explicitly modeled by stopband sin/cos regressors, but downstream
        connectivity analysis (3dNetCorr Pearson correlation) is mean-
        invariant, making polort -1 (no polynomials) safe.

        Concatenation analysis: the user asked whether concatenating rest
        runs before 3dTproject would alleviate the DOF issue. Mathematical
        analysis showed:
        - Stopband fraction is constant (85.6%) regardless of N_TRs
        - Censoring tolerance as a percentage is ~10% whether per-run or
          concatenated
        - Concatenation introduces a "bad apple" problem: high-censoring
          runs drag down DOF for the entire concatenated dataset
        - Example: ses-00A with runs at 5%, 5%, 31%, 56% censoring —
          per-run: 2 survive; concatenated: 24% overall censoring causes
          total failure, losing the 2 good runs
        - The per-run approach insulates good runs from bad runs and is
          strictly more robust when censoring is heterogeneous across runs

        Changing polort from 2 to -1 saves exactly 3 regressors (342 to
        339), shifting the failure threshold from ~10.0% to ~10.8%
        censoring. This is negligible but methodologically correct
        (removes redundancy).
      </summary>
      <approaches>
        <approach id="A6" label="Config change: polort -1" feasibility="high" risk="low">
          <description>
            Set polort to -1 in proc_config_final.yaml for rest_conn
            analyses. Removes the 3 redundant polynomial regressors when
            bandpass is active. Methodologically cleaner; negligible DOF
            impact but eliminates redundancy. Safe because downstream
            correlation is mean-invariant.
          </description>
          <pros>
            Methodologically correct; removes redundancy; trivial config
            change; no code modification needed
          </pros>
          <cons>
            Negligible DOF improvement (3 regressors out of ~342)
          </cons>
        </approach>
        <approach id="A7" label="Concatenate rest runs before 3dTproject" feasibility="med" risk="high">
          <description>
            Concatenate all rest runs, then run 3dTproject once on the
            concatenated timeseries. Would require handling run-boundary
            discontinuities (per-run nuisance regressors, run-mean
            regressors) and upstream architecture changes.
          </description>
          <pros>Pools DOF across runs when censoring is uniform</pros>
          <cons>
            Does not change the fundamental stopband fraction (85.6%);
            censoring tolerance as a percentage remains ~10%; high-censoring
            runs contaminate DOF for the entire dataset ("bad apple"
            problem); loses the per-run insulation that currently allows
            good runs to survive independently; requires significant
            upstream refactoring
          </cons>
          <statistical_considerations>
            Concatenation is strictly worse when censoring is heterogeneous
            across runs, which is the common case in ABCD pediatric data.
            The per-run architecture is more robust because it allows
            independent survival — good runs are insulated from bad runs.
          </statistical_considerations>
        </approach>
      </approaches>
      <decision status="decided" chosen="A6">
        Config change only: set polort to -1 for rest_conn. Handled by the
        user directly in proc_config_final.yaml. No code changes, no
        documentation needed. The DOF constraint is structural (inherent
        to regression-based bandpass at TR=0.8s with narrow passband) and
        is understood by practitioners running this analysis. The per-run
        processing architecture is confirmed as the correct approach.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F7: N-BACK CUE RELABELING -->
    <!-- ================================================================== -->

    <topic id="T5" title="F7: N-back cue relabeling assumption and task timing">
      <summary>
        fix_nback_cue_labels() relabels generic "cue" trial types based on
        the n-back level of the subsequent trial block. The CR raised concern
        that this fundamentally alters the design matrix and requires
        justification.

        Detailed task structure review confirmed the relabeling is correct:

        0-back blocks:
        - First stimulus ("cue") = participant passively views an image from
          the stimulus category (e.g., a positive face). This is the target
          to remember for subsequent match/no-match decisions.
        - Subsequent trials = participant views other images and decides
          match/no-match against the cued target.
        - Relabeling: cue to bare condition name (e.g., "posface"). This
          creates a SEPARATE regressor from recall trials ("0_back_posface"),
          appropriately modeling the distinct cognitive demands: encoding
          (cue) vs. comparison + decision (recall).

        2-back blocks:
        - First stimulus ("cue") = generic instruction screen reading
          "2-BACK" with no stimulus-category content.
        - Subsequent trials = participant views images and decides if current
          matches the stimulus seen two trials ago.
        - Relabeling: cue to "instruction". Pooled across all 2-back
          conditions because the instruction is identical regardless of
          upcoming stimulus category.

        The relabeling preserves category-specific information for 0-back
        cues (which contain real stimuli) while correctly treating 2-back
        cues as generic instructions (which contain no stimulus content).
        The separation of cue and recall events into distinct regressors
        is methodologically appropriate given their qualitatively different
        cognitive demands.
      </summary>
      <approaches>
        <approach id="A8" label="Documentation only" feasibility="high" risk="low">
          <description>
            Document the cue relabeling rationale in methods documentation.
            Cite Casey et al. (2018) and the ABCD n-back task protocol.
            Justify the separation of cue (encoding/passive viewing) and
            recall (comparison + decision) events into distinct regressors
            based on differential cognitive demands and the presence vs.
            absence of category-specific stimulus content.
          </description>
          <pros>No code change; preemptively addresses reviewer questions</pros>
          <cons>None</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A8">
        Documentation only. The relabeling logic in fix_nback_cue_labels()
        is correct and well-justified by the ABCD n-back task design. Cite
        Casey et al. (2018) and justify the separation of encoding (cue)
        and recall events based on distinct cognitive demands and stimulus
        content.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F8: FILE-EXISTENCE CACHING -->
    <!-- ================================================================== -->

    <topic id="T6" title="F8: File-existence caching and force_recompute flag">
      <summary>
        10+ preprocessing functions use early-exit caching via
        if os.path.isfile(out_path): return out_path with no invalidation
        mechanism. If input data or parameters change between pipeline
        invocations, stale cached outputs are silently reused. The risk is
        bounded: in production (~11,000 subjects, each processed once),
        caching is irrelevant. The vulnerability is during iterative
        development and testing where parameters may change.

        Parameter-encoded filenames (CR option 3) were rejected as overly
        complex for marginal benefit. A simple force_recompute boolean
        flag in the orchestrator config was agreed upon as the lowest-risk,
        most practical mitigation.
      </summary>
      <approaches>
        <approach id="A9" label="force_recompute config option" feasibility="high" risk="low">
          <description>
            Add a boolean force_recompute field to the orchestrator config
            (default: false). When true, bypass all file-existence checks
            in preprocessing functions. Implementation: pass the flag
            through to each function and add `and not force_recompute` to
            each `if os.path.isfile()` check. Document the caching behavior
            and the flag in INPUT_SPECIFICATION.md.
          </description>
          <pros>
            Simple; minimal code change; no behavioral change when false
            (default); gives users explicit control over caching; addresses
            the reproducibility concern during development
          </pros>
          <cons>
            Requires threading the flag through ~10 function signatures
            (or passing via a context/config object)
          </cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A9">
        Implement force_recompute as a boolean orchestrator config option
        (default false). Document caching behavior and the flag. No
        parameter-encoded filenames.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F9: CONCATENATION MASK SELECTION -->
    <!-- ================================================================== -->

    <topic id="T7" title="F9: Mask intersection for concatenated tasks">
      <summary>
        orchestrate_first_level.py uses concat_mask = per_run_masks[0]
        (first run's brain mask) for smoothing concatenated data. If per-run
        masks differ (e.g., due to susceptibility-induced signal dropout
        varying across runs), the first run's mask may not be representative.

        Although fMRIPrep computes masks in template space (making per-run
        masks near-identical in practice), the intersection operation is
        trivially cheap and strictly more conservative.
      </summary>
      <approaches>
        <approach id="A10" label="Mask intersection via 3dmask_tool -inter" feasibility="high" risk="low">
          <description>
            When multiple runs are concatenated, compute the intersection of
            all per-run masks using 3dmask_tool -inter. Fall back to the
            single mask when only one run survives. This ensures only voxels
            consistently within the brain across all runs are included in
            smoothing. Approximately 5 lines replacing the per_run_masks[0]
            assignment.
          </description>
          <pros>
            Conservative; eliminates edge-case mask inconsistency;
            trivial computational cost; no behavioral change when masks
            are identical (intersection = any single mask)
          </pros>
          <cons>None meaningful</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A10">
        Implement mask intersection via 3dmask_tool -inter for concatenated
        tasks. ~5-line code change.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F10: NaN MOTION IMPUTATION -->
    <!-- ================================================================== -->

    <topic id="T8" title="F10: NaN motion parameter handling (unknown = censor)">
      <summary>
        orchestrator_utils.py:1144 replaces NaN motion values with 0.0 via
        np.nan_to_num(). NaN in motion parameters indicates a tracking
        failure — the true motion is unknown, not zero. Imputing zero makes
        these TRs appear as the stillest frames, virtually guaranteeing
        they survive censoring when they are the least trustworthy.

        Discussion established the principle: missingness is ambiguous and
        the conservative approach is to not trust uncertain TRs. This is
        consistent with the pipeline philosophy that all quality screening
        happens post-hoc — but within-run censoring decisions must be
        conservative at the individual level.

        A hard error threshold on NaN prevalence (e.g., >5%) was rejected.
        Subject exclusion for data quality issues (including extensive
        motion tracking failures) is handled post-hoc at group level, not
        during first-level processing. The pipeline should process whatever
        data is available and let QC metrics inform exclusion.
      </summary>
      <approaches>
        <approach id="A11" label="Log and impute with censoring-guarantee value" feasibility="high" risk="low">
          <description>
            1. Count NaN occurrences and log their TR indices and column
               names at WARNING level.
            2. Replace NaN values with a large value (e.g., 999.0) that
               guarantees the TR will exceed any reasonable FD threshold
               and be censored by upstream's 1d_tool.py.
            3. No hard error threshold — let the data flow through
               regardless of NaN prevalence. If a run has 100% NaN motion,
               every TR will be censored, the run will fail DOF checks,
               and it will be recorded in QC. Subject exclusion happens
               post-hoc.

            This implements an explicit "unknown = censor" policy: if the
            motion tracking system could not estimate head position for a
            TR, that TR is treated as high-motion and censored.
          </description>
          <pros>
            Conservative; prevents untrusted TRs from surviving censoring;
            no data loss at processing time; QC captures the NaN count for
            post-hoc evaluation; consistent with pipeline philosophy
          </pros>
          <cons>
            Large imputed values could affect motion derivative computation
            for adjacent TRs (derivative of 999.0 is large), but those
            adjacent TRs would also likely be censored as a consequence,
            which is arguably appropriate for TRs neighboring a tracking
            failure
          </cons>
          <statistical_considerations>
            The imputed value must exceed the maximum plausible FD threshold
            used in any analysis config. A value of 999.0 mm for
            translations (and 999.0 radians for rotations) guarantees
            censoring under any threshold. The derivative spillover to
            adjacent TRs is a conservative side effect — tracking failures
            often co-occur with rapid motion, so censoring neighbors is
            defensible.
          </statistical_considerations>
        </approach>
      </approaches>
      <decision status="decided" chosen="A11">
        Log NaN count and TR indices; impute with large value (999.0)
        guaranteeing censoring. No hard error threshold. Explicit
        "unknown = censor" policy. Consistent with pipeline philosophy
        that subject exclusion happens post-hoc at group level.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F11: is_rest STRING CHECK -->
    <!-- ================================================================== -->

    <topic id="T9" title="F11: Strict task label whitelist">
      <summary>
        orchestrate_first_level.py:325 uses is_rest = task_label.lower()
        .startswith("rest") to infer task type. This is fragile (could
        misclassify "resting_state" or "restoration") but low impact for
        ABCD where only "rest" and "nback" are used.

        Discussion rejected the CR's recommendation of an is_rest config
        field as unnecessary complexity for an ABCD-specific orchestrator.
        Instead, the user proposed a strict whitelist: if task_label is
        not one of the known ABCD first-level task labels ("rest",
        "nback"), the pipeline should error. The orchestrator should
        expect correct task labels and not attempt to decipher user intent
        via substring matching.
      </summary>
      <approaches>
        <approach id="A12" label="Strict task label whitelist with hard error" feasibility="high" risk="low">
          <description>
            Replace the startswith("rest") check with an explicit whitelist
            of recognized ABCD task labels: {"rest", "nback"}. If an
            unrecognized task label is encountered, raise OrchestratorError
            with a clear message listing the valid labels. The is_rest
            determination becomes an exact equality check: is_rest =
            (task_label == "rest").

            Implementation: ~5 lines. Define VALID_TASK_LABELS = {"rest",
            "nback"} as a module-level constant. Validate early in the
            per-task loop. Replace the startswith check with exact equality.
          </description>
          <pros>
            Explicit; no ambiguity; catches config typos immediately;
            appropriate for a dataset-specific orchestrator; simpler than
            substring matching
          </pros>
          <cons>
            Requires code modification if new task types are added (but
            this is intentional — new tasks should be explicitly supported)
          </cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A12">
        Strict whitelist: VALID_TASK_LABELS = {"rest", "nback"}. Hard error
        on unrecognized labels. is_rest determined by exact equality. ~5-line
        implementation.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F12: S3 GAP PROBING -->
    <!-- ================================================================== -->

    <topic id="T10" title="F12: S3 run discovery — probe all indices regardless of gaps">
      <summary>
        orchestrator_utils.py S3 run probing stops at the first missing run
        (break on 404/NoSuchKey at line 225). If runs are non-contiguous
        (e.g., run 1 and run 3 exist but not run 2), run 3 is never
        discovered.

        Initial analysis suggested the contiguous-run assumption was valid
        for ABCD. However, the user clarified that if a participant has
        non-contiguous runs (regardless of the reason), ALL existing runs
        must be discovered and processed. The orchestrator should not make
        assumptions about run numbering contiguity.
      </summary>
      <approaches>
        <approach id="A13" label="Probe all 9 indices unconditionally" feasibility="high" risk="low">
          <description>
            Replace the `break` on 404/NoSuchKey with `continue` in the S3
            run probing loop. Probe all run indices 1-9 regardless of gaps.
            Collect all runs that exist on S3. The downstream preprocessing
            already handles variable numbers of runs (concatenation operates
            on whatever runs survive).

            Implementation: single-line change (break to continue).
          </description>
          <pros>
            Discovers all existing runs regardless of numbering gaps;
            robust to data curation decisions that may remove individual
            runs; trivial implementation
          </pros>
          <cons>
            Slightly more S3 HEAD requests for subjects with fewer runs
            (probes all 9 instead of stopping early). Negligible cost
            (~9 HEAD requests per task per session).
          </cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A13">
        Probe all 9 run indices unconditionally. Replace break with
        continue on 404/NoSuchKey. Single-line change.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F13: CONTRAST LABEL TYPO -->
    <!-- ================================================================== -->

    <topic id="T11" title="F13: Contrast label typo in proc_config_final.yaml">
      <summary>
        proc_config_final.yaml line 121: "place_tback-z-back" should be
        "place_tback-zback" for consistency with other contrast labels.
        Cosmetic issue; does not affect computation.
      </summary>
      <decision status="decided" chosen="none">
        Fixed manually by the user in proc_config_final.yaml. No further
        action needed.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F14: FD CENSORING USES RAW DISPLACEMENTS -->
    <!-- ================================================================== -->

    <topic id="T12" title="F14: FD censoring based on raw displacements only">
      <summary>
        Upstream censoring via 1d_tool.py uses only the base 6 motion
        parameters, not temporal derivatives. This is field-standard
        practice per Power et al. (2012). Derivatives serve as nuisance
        regressors in the GLM but do not contribute to censoring decisions.
      </summary>
      <decision status="decided" chosen="none">
        No action. Field-standard behavior; does not require documentation
        — it is the default assumption for anyone familiar with FD-based
        censoring.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F15: CONTRAST COEFFICIENT ROUNDING -->
    <!-- ================================================================== -->

    <topic id="T13" title="F15: Contrast coefficient precision (0.3333 to 0.333333)">
      <summary>
        Several n-back activation contrasts used 0.3333 (4 decimal places)
        for 1/3 coefficients. Sum of three = 0.9999, not 1.0000.
        Negligible practical impact but mathematically imprecise.
      </summary>
      <decision status="decided" chosen="none">
        Fixed manually by the user in proc_config_final.yaml (updated to
        0.333333, 6 decimal places). No further action needed.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F16: os.chdir THREAD SAFETY -->
    <!-- ================================================================== -->

    <topic id="T14" title="F16: os.chdir thread safety">
      <summary>
        orchestrate_first_level.py:599 uses os.chdir(session_out) to
        prevent AFNI's 3dDeconvolve.err file collisions. Process-global
        and not thread-safe, but the orchestrator is single-threaded by
        design. SLURM-level parallelism (one subject per job) operates
        at the process level where os.chdir is isolated.
      </summary>
      <decision status="decided" chosen="none">
        No action. Single-process sequential architecture is the design
        intent. SLURM parallelism provides process-level isolation.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F17: force_diff_atlas FLAG -->
    <!-- ================================================================== -->

    <topic id="T15" title="F17: force_diff_atlas flag in proc_config">
      <summary>
        force_diff_atlas: true bypasses atlas space validation. Required
        because the specific Schaefer+Tian template in use has an
        inaccurate space label in its NIfTI header, despite being in the
        same stereotaxic space (MNI152NLin2009cAsym) as the fMRIPrep data.
      </summary>
      <decision status="decided" chosen="none">
        No action. Template-specific header issue, not a pipeline concern.
        No documentation needed — the template is genuinely in the correct
        space, just mislabeled in the header.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F18: SESSION LABEL HARD-CODING -->
    <!-- ================================================================== -->

    <topic id="T16" title="F18: Session label 'A' suffix hard-coding">
      <summary>
        Session label construction hard-codes "A" suffix (ses-{session}A).
        ABCD-specific convention. Portability to other datasets is not a
        design goal for this orchestrator.
      </summary>
      <decision status="decided" chosen="none">
        No action. ABCD-specific by design.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F19: NOTCH FILTER RESPIRATORY RATE -->
    <!-- ================================================================== -->

    <topic id="T17" title="F19: Notch filter respiratory rate assumption">
      <summary>
        notch_filter_band: [0.31, 0.43] targets ~18.6-25.8 breaths/min
        per Fair et al. (2020). Standard for the ABCD adolescent population.
        The parameter is visible and self-documenting in proc_config_final
        .yaml.
      </summary>
      <decision status="decided" chosen="none">
        No action. Config parameter is self-documenting; citation
        (Fair et al. 2020) is standard for ABCD resting-state analyses.
      </decision>
    </topic>

  </topics>

  <!-- ==================================================================== -->
  <!-- CONSOLIDATED ACTION ITEMS -->
  <!-- ==================================================================== -->

  <action_items>

    <!-- P0: Critical implementation (from prior brainstorm session) -->
    <item priority="P0" target_mode="implement" finding_ref="F1"
          description="Implement per-analysis outcome tracking: accumulate {name, status, error} dicts in _process_session(); propagate to process_participant() session_results; report success/partial/failed with per-analysis breakdown in SESSION SUMMARY log" />
    <item priority="P0" target_mode="implement" finding_ref="F2"
          description="Remove orchestrator FD computation from QC pipeline; split QC into Phase 1 (pre-analysis: tSNR, mask coverage, registration Dice) and Phase 2 (post-analysis: all motion from upstream enorm.1D/censor.1D — FD summary, censoring stats, carpet plots)" />
    <item priority="P0" target_mode="implement" finding_ref="F2"
          description="Build consolidated session-level QC JSON (sub-{ID}_ses-{session}_orchestrator_qc.json): provenance (AFNI/proc/orch versions, timestamp), preprocessing (per task per run: n_total_trs, n_nss_removed, tsnr, dice, mask_voxels), analyses (per analysis: status, error, wall_time, upstream QC embedded, orchestrator augmentation), session status" />

    <!-- P1: Major implementation -->
    <item priority="P1" target_mode="implement" finding_ref="F3"
          description="Add structured run-loss warning to skipped-runs log message (lines 478-484): explicitly state surviving vs. total run count" />
    <item priority="P1" target_mode="implement" finding_ref="F4"
          description="Two-tier rotation unit check in extract_motion_regressors(): if max(abs(rot)) > 1.0 then definitively degrees, proceed; if &lt;= 1.0 then raise OrchestratorError requiring manual inspection. ~10 lines after base column extraction, before deg2rad" />
    <item priority="P1" target_mode="implement" finding_ref="F8"
          description="Add force_recompute boolean to orchestrator config (default false); thread through all file-existence-cached functions; bypass os.path.isfile checks when true. Document caching behavior." />
    <item priority="P1" target_mode="implement" finding_ref="F10"
          description="NaN motion handling: log NaN count and TR indices at WARNING level; impute with 999.0 (guarantees censoring). No hard error threshold. Explicit 'unknown = censor' policy." />
    <item priority="P1" target_mode="implement" finding_ref="F11"
          description="Strict task label whitelist: VALID_TASK_LABELS = {'rest', 'nback'}; validate early in per-task loop; raise OrchestratorError on unrecognized labels; replace startswith('rest') with exact equality" />
    <item priority="P1" target_mode="implement" finding_ref="F12"
          description="S3 run discovery: replace break with continue on 404/NoSuchKey to probe all 9 run indices regardless of gaps. Single-line change." />

    <!-- P2: Minor implementation -->
    <item priority="P2" target_mode="implement" finding_ref="F9"
          description="Mask intersection for concatenated tasks: replace per_run_masks[0] with 3dmask_tool -inter across all per-run masks; fall back to single mask when only one run survives. ~5 lines." />

    <!-- Documentation items -->
    <item priority="P1" target_mode="document" finding_ref="F5"
          description="Document HRF model rationale: dmBLOCK for activation (variable-duration events), GAM for beta series (Rissman et al. 2004, Mumford et al. 2012). Note that activation and connectivity analyses answer different questions." />
    <item priority="P1" target_mode="document" finding_ref="F7"
          description="Document n-back cue relabeling: cite Casey et al. (2018) and ABCD protocol. Justify separation of 0-back cue (encoding/passive viewing to bare condition name) and recall (comparison + decision to 0_back_{condition}) as distinct cognitive events. 2-back cues to 'instruction' (no stimulus content)." />

    <!-- Config changes (handled by user) -->
    <item priority="P0" target_mode="config" finding_ref="F6"
          description="Set polort: -1 for rest_conn in proc_config_final.yaml. COMPLETED by user." />
    <item priority="P2" target_mode="config" finding_ref="F13"
          description="Fix contrast label typo: place_tback-z-back to place_tback-zback. COMPLETED by user." />
    <item priority="P2" target_mode="config" finding_ref="F15"
          description="Update contrast coefficients from 0.3333 to 0.333333. COMPLETED by user." />

  </action_items>

  <!-- ==================================================================== -->
  <!-- NEXT STEPS -->
  <!-- ==================================================================== -->

  <next_steps>
    All 19 CR findings have been dispositioned. The brainstorm phase is
    complete. The recommended next step is /implement mode:

    1. Generate a unified technical specification (tech spec) from the two
       brainstorm reports (Session 13: F1-F2 + QC enrichment; Session 14:
       F3-F19) consolidating all implementation action items.

    2. Implementation priority order:
       - P0 first: F1 (qualified session reporting), F2 (QC architecture
         overhaul + consolidated QC JSON) — these are critical and
         interdependent
       - P1 second: F4 (rotation check), F8 (force_recompute), F10
         (NaN handling), F11 (task whitelist), F12 (S3 gap probing),
         F3 (run-loss warning)
       - P2 last: F9 (mask intersection)

    3. After implementation: /test to update the test suite (161 existing
       tests + new tests for all changes), then /run-local to retest
       a test participant with the updated pipeline.

    4. After successful retest: /document for F5 and F7 documentation
       items, then /publish.
  </next_steps>

</brainstorm_report>
```
