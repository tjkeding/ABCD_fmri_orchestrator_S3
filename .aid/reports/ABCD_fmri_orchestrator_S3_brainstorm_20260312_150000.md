# Brainstorm: Critical Review Response Planning

```xml
<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-03-12T15:00:00Z" />

  <context_files>
    <file path="ABCD_fmri_orchestrator_S3_cr_20260312_143753.md" relevance="Source CR report with 19 findings (2 critical, 6 major, 8 minor, 3 notes)" />
    <file path="orchestrate_first_level.py" relevance="Main orchestrator — session success reporting, QC flow, analysis dispatch" />
    <file path="orchestrator_utils.py" relevance="Core utilities — FD computation, preproc QC, first-level QC, config building" />
    <file path="fmri_first_level_proc/task_act_first_level.py" relevance="Upstream task_act — QC summary JSON contents" />
    <file path="fmri_first_level_proc/task_conn_first_level.py" relevance="Upstream task_conn — QC summary JSON, x1D design matrix" />
    <file path="fmri_first_level_proc/rest_conn_first_level.py" relevance="Upstream rest_conn — per-run DOF, censor stats, run skip logic" />
    <file path="fmri_first_level_proc/first_level_utils.py" relevance="Upstream utils — create_censor_file, enorm output, FD computation" />
  </context_files>

  <topics>

    <!-- ================================================================== -->
    <!-- F1: SESSION SUCCESS REPORTING -->
    <!-- ================================================================== -->

    <topic id="T1" title="F1: Session success masks analysis failures">
      <summary>
        Session-level success reporting does not propagate per-analysis failures.
        A session is recorded as "success" even if individual analyses fail entirely.
        Discussed the appropriate definition of "success" given the user's philosophy:
        process everything available, screen quality post-hoc at group level.
      </summary>
      <approaches>
        <approach id="A1" label="Strict — fail if any analysis fails" feasibility="high" risk="med">
          <description>Session fails if any analysis raises an exception or produces no output.</description>
          <pros>Conservative; no silent failures</pros>
          <cons>May halt processing prematurely; prevents partial data collection</cons>
        </approach>
        <approach id="A2" label="Permissive — succeed if at least one analysis completes" feasibility="high" risk="low">
          <description>Session succeeds if any analysis produces output.</description>
          <pros>Maximizes data collection</pros>
          <cons>Masks failures in non-primary analyses</cons>
        </approach>
        <approach id="A3" label="Qualified — success/partial/failed with per-analysis breakdown" feasibility="high" risk="low">
          <description>
            Track per-analysis outcomes. Session status: "success" (all analyses
            produced output), "partial" (some analyses produced output), "failed"
            (no analyses produced output). SESSION SUMMARY includes per-analysis
            status line. Participant-level exit unchanged (raise only if ALL
            sessions failed).
          </description>
          <pros>Maximizes data collection; transparent reporting; supports post-hoc filtering</pros>
          <cons>Slightly more complex implementation</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A3">
        Qualified reporting with per-analysis breakdown. "Success" means all
        attempted analyses produced output for available data. The orchestrator
        is agnostic to data quality — all motion/DOF/trial-survival screening
        happens post-hoc at group level using QC logs. The session summary log
        must include per-analysis status (OK/FAILED with reason).
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F2: FD METRIC INCONSISTENCY + QC ENRICHMENT -->
    <!-- ================================================================== -->

    <topic id="T2" title="F2: FD metric inconsistency and QC architecture overhaul">
      <summary>
        Orchestrator computes FD with Power et al. (2012) formula (radius=50mm)
        for QC, while upstream uses AFNI's 1d_tool.py (enorm, effective
        radius~80mm) for actual censoring. These produce different values.
        Discussion expanded into a comprehensive QC architecture redesign:
        single source of truth for all motion metrics, exhaustive QC JSONs
        for group-level screening of ~11,000 subjects.
      </summary>
      <approaches>
        <approach id="A4" label="Option A — harmonize radius in orchestrator" feasibility="high" risk="med">
          <description>Change orchestrator FD radius to match AFNI's effective radius.</description>
          <pros>Simple code change</pros>
          <cons>Still two independent FD computations; residual formula differences</cons>
        </approach>
        <approach id="A5" label="Option B — keep orchestrator FD for preproc, upstream for censoring" feasibility="high" risk="med">
          <description>Label orchestrator FD as "Power r=50mm", use upstream for censoring stats only.</description>
          <pros>Preserves preproc QC timing</pros>
          <cons>Two FD metrics in QC JSONs; confusing for downstream users</cons>
        </approach>
        <approach id="A6" label="Option C — single source: all motion from upstream/AFNI" feasibility="high" risk="low">
          <description>
            Eliminate orchestrator's independent FD computation entirely. All
            motion metrics (FD values, censoring stats, carpet plot FD traces)
            come from upstream's enorm.1D and censor.1D files produced by AFNI's
            1d_tool.py. Requires splitting QC into two phases:

            Phase 1 (pre-analysis): Non-motion metrics only — tSNR, brain mask
            coverage, registration Dice.

            Phase 2 (post-analysis): Motion-dependent metrics from upstream files
            — FD summary (mean/median/max from enorm.1D), censoring stats (from
            censor.1D), carpet plots (using enorm FD), per-run status, trial
            survival, DOF. Consolidated into single session-level QC JSON.

            Motion metrics reported at analysis granularity: concatenated for
            task (task_act, task_conn), per-run for rest (rest_conn).
          </description>
          <pros>Single source of truth; no metric inconsistency; uses authoritative AFNI values</pros>
          <cons>Carpet plots deferred to post-analysis; slightly more complex implementation</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A6">
        Option C — all motion metrics from upstream/AFNI. The orchestrator's
        compute_framewise_displacement() will be removed from the QC pipeline
        (retained in orchestrator_utils.py only for unit tests or standalone use).
        QC split into Phase 1 (pre-analysis, non-motion) and Phase 2
        (post-analysis, motion + consolidated report). This ensures the pipeline
        never uses different motion information at different points.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- QC ENRICHMENT: CONSOLIDATED SESSION QC JSON -->
    <!-- ================================================================== -->

    <topic id="T3" title="Consolidated session-level QC JSON design">
      <summary>
        QC logs are the sole post-hoc forensic record for ~11,000 subjects.
        Designed a comprehensive session-level QC JSON that consolidates
        upstream QC summaries with orchestrator-level metadata. Three tiers:
        (1) inclusion/exclusion determinants, (2) quality characterization,
        (3) provenance.
      </summary>
      <approaches>
        <approach id="A7" label="Orchestrator augments upstream QC" feasibility="high" risk="low">
          <description>
            Orchestrator reads upstream QC summary JSONs (already written by
            fmri_first_level_proc) and augments with orchestrator-level data.
            Produces a single sub-{ID}_ses-{session}_orchestrator_qc.json per
            session. Contents:

            PROVENANCE: AFNI version, fmri_first_level_proc version,
            orchestrator version, timestamp.

            PREPROCESSING (per task, per run): n_total_trs, n_nss_removed,
            n_trs_after_trim, tsnr_median, dice_registration, brain_mask_voxels.

            ANALYSES (per analysis): status (success/failed), error message,
            wall_time_seconds, upstream QC summary (embedded verbatim),
            plus orchestrator-derived fields:
            - task_act/task_conn: conditions_dropped list, contrasts_skipped
              list, derived from per_condition_surviving_trials
            - rest_conn: runs_attempted, runs_succeeded, per_run_status with
              failure reason and DOF breakdown
            - All: censoring temporal pattern (max_consecutive_censored,
              n_clean_segments) derived from censor.1D

            SESSION LEVEL: session_status (success/partial/failed),
            session_wall_time_seconds.

            Designed for: pd.json_normalize([json.load(f) for f in glob(...)]).
          </description>
          <pros>Single file per session; machine-readable; uses authoritative upstream data; no upstream changes needed</pros>
          <cons>Some redundancy with upstream QC JSONs (which continue to exist independently)</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A7">
        Orchestrator reads and augments upstream QC. Single consolidated JSON
        per session. Upstream QC JSONs continue to exist for standalone
        fmri_first_level_proc use.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- DESIGN MATRIX DIAGNOSTICS -->
    <!-- ================================================================== -->

    <topic id="T4" title="Design matrix diagnostics (condition number)">
      <summary>
        x1D design matrix files are only available for task_conn (explicitly
        saved via -x1D flag). task_act does not save x1D by default; rest_conn
        uses 3dTproject which has no equivalent. Adding -x1D to task_act
        requires upstream change.
      </summary>
      <approaches>
        <approach id="A8" label="Defer to P2" feasibility="high" risk="low">
          <description>Skip design matrix diagnostics for now. Revisit if collinearity issues emerge during group-level analysis.</description>
          <pros>No implementation needed; low practical risk for well-designed tasks</pros>
          <cons>Misses potential collinearity issues</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A8">
        Deferred to P2. Low priority given well-structured ABCD task designs.
        If pursued later, would require adding -x1D flag to task_act upstream.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- INPUT CHECKSUMS -->
    <!-- ================================================================== -->

    <topic id="T5" title="Input file checksums for provenance">
      <summary>
        Computing MD5/SHA256 of multi-GB fMRIPrep archives adds non-trivial
        I/O overhead. S3 object keys provide sufficient provenance for
        identifying input data versions.
      </summary>
      <decision status="decided" chosen="none">
        Not implementing. S3 object keys are sufficient for input provenance.
        The orchestrator already logs the S3 key paths for all downloaded files.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- F3: BROAD EXCEPTION CATCH (PARTIALLY DISCUSSED) -->
    <!-- ================================================================== -->

    <topic id="T6" title="F3: Broad exception catch silently downgrades per-run failures">
      <summary>
        Per-run preprocessing failures caught by broad except Exception at
        line 470, run silently skipped. Concern: losing 1 of 2 n-back runs
        halves data with no structured record. Partially discussed — the
        consolidated QC JSON (T3) addresses the recording gap. Open question:
        should there be a configurable minimum-run policy?

        Preliminary recommendation: no minimum-run gate; process whatever
        survives, record in QC, let group-level handle exclusion. Consistent
        with user's stated philosophy.
      </summary>
      <decision status="open" chosen="none">
        Leaning toward no minimum-run gate. To be confirmed at next session.
      </decision>
    </topic>

    <!-- ================================================================== -->
    <!-- REMAINING FINDINGS (NOT YET DISCUSSED) -->
    <!-- ================================================================== -->

    <topic id="T7" title="Remaining CR findings to discuss">
      <summary>
        The following findings from the CR report have not yet been discussed:
        F3 (confirm decision), F4 (rotation unit assumption), F5 (HRF model
        inconsistency documentation), F6 (rest_conn DOF constraint), F7
        (n-back cue relabeling), F8 (file-existence caching), F9 (concat mask
        selection), F10 (NaN motion imputation), F11 (is_rest string check),
        F12 (S3 gap probing), F13 (contrast label typo), F14 (FD uses raw
        displacements only), F15 (contrast coefficient rounding), F16
        (os.chdir thread safety), F17 (force_diff_atlas), F18 (session label
        hard-coding), F19 (notch filter respiratory rate).
      </summary>
      <decision status="open" chosen="none">
        To be discussed at next session, starting with F3 confirmation.
      </decision>
    </topic>

  </topics>

  <action_items>
    <item priority="P0" target_mode="implement" description="F1: Implement per-analysis outcome tracking — success/partial/failed session status with per-analysis breakdown in SESSION SUMMARY log" />
    <item priority="P0" target_mode="implement" description="F2: Remove orchestrator FD computation from QC pipeline; split QC into Phase 1 (pre-analysis, non-motion) and Phase 2 (post-analysis, all motion from upstream enorm.1D/censor.1D)" />
    <item priority="P0" target_mode="implement" description="QC Enrichment: Build consolidated session-level QC JSON (sub-{ID}_ses-{session}_orchestrator_qc.json) — provenance, preprocessing, per-analysis upstream QC + orchestrator augmentation, session status" />
    <item priority="P2" target_mode="implement" description="Design matrix diagnostics: deferred; would require upstream -x1D flag for task_act" />
  </action_items>

  <next_steps>
    Resume brainstorm at F3 (confirm no minimum-run gate), then proceed through
    F4-F19. After all findings are dispositioned, generate a unified
    implementation plan for /implement mode covering all decided changes.
  </next_steps>

</brainstorm_report>
```
