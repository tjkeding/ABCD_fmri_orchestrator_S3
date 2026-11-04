<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-10-13T19:17:42Z" report_type="interim" supersedes="ABCD_fmri_orchestrator_S3_brainstorm_20261013_101544.md" />

  <interim_status>
    INTERIM report. The brainstorm is paused between topics and resumes at resume_point. Locked: T1, C1-C7 (plus scope locks and facts listed below). Open: C9 (next), C8, T2, T4-T16. T3 is absorbed into C9. No Pre-Report Audit was run; the final report must pass it. Nothing beyond the locked items may be treated as a specification by /implement.
  </interim_status>

  <resume_point>
    1. Walk C9 (checkpoint/restart safety, absorbs T3) with the upstream-aligned framing below. The user is undecided between Option A (restore ledger + recorded outputs from S3 and let the fmri-first-level-proc ledger decide reuse, remove_previous false) and Option B (recompute incomplete sessions from scratch). Work both up with costs (S3 GET/transfer volume and time per restore, compute saved per pod retry, implementation and test surface, G4 interaction, failure modes) before recommending; recommendation must pass N=3 proposal verification.
    2. Then C8 (cross-repo coupling), then T2, T4-T16 in order, one topic per message, each recommendation through N=3 proposal verification (proposal-verification-agent-sonnet-high).
    3. The fmri-first-level-proc read-only grant is persisted (project_scope, read). AWS reads use /usr/local/bin/aws (v2) with profile <aws_profile>; the FSL-bundled v1 CLI first on PATH has no SSO support. SSO sessions expire; the user runs the login.
  </resume_point>

  <user_directives>
    <directive>Inputs are ONLY s3://<bucket>/mmps_mproc/ (task events) and s3://<bucket>/derivatives/func/ (preprocessed per-run tarballs). <other_prefix>/ is unrelated to this project.</directive>
    <directive>Per-subject batch invocation (producer Argo workflow calls orchestrate_first_level.py --subj_id with the bare 8-character ID); the orchestrator discovers sessions/runs, renders the fmri-first-level-proc config, runs it, compiles QC.</directive>
    <directive>Only volumetric inputs; derivatives/func_surf (CIFTI) is never handled or used.</directive>
    <directive>Changes land in the orchestrator and fmri-first-level-proc GitHub repos; the producer maintainer re-downloads them as vendored copies. This project does not edit producer.</directive>
    <directive>Producer-level defects (MNI BOLD corruption, write-timing, events retention) are producer's responsibility; the user states all producer outputs will be complete and correct before the orchestrator and fmri-first-level-proc are used.</directive>
    <directive>Treat as fact: task timing files live at mmps_mproc/{subj}/{ses}/func/*_events.tsv.</directive>
    <directive>Before proposing any new exception class, confirm no existing fmri-first-level-proc (FmriFirstLevelError hierarchy) or orchestrator class fits (saved to project memory).</directive>
    <directive>Orchestrator and fmri-first-level-proc must be checkpoint/restart safe for <bucket>: never re-run subjects that already have valid outputs.</directive>
  </user_directives>

  <context_files>
    <file path="orchestrator_utils.py" relevance="Discovery/download (710-1395, to be replaced), routing and sentinel (~200-345), extract_motion_regressors (~1731), extract_tissue_signals, detect_non_steady_state_trs, format_task_timing, compute_preproc_qc, build_first_level_config (forces remove_previous True at ~3009)." />
    <file path="orchestrate_first_level.py" relevance="Per-run loop (~571-712, blanket except), active_analyses filter (~799-802), _derive_session_status (93-127), upload + sentinel (~935-955)." />
    <file path="example_orchestrator_config.yaml; example_proc_config.yaml" relevance="Tasks nback+rest; s3 block still <bucket>/fmriprep; qc.preproc flags; rest model fd 0.4, bandpass 0.01-0.1, motion_deriv_degree 2, use_tissue_derivs true." />
    <file path="fmri-first-level-proc v4.0.0-build: engine.py, checkpoint.py, nuisance.py, rest_conn_first_level.py, first_level_config.py, events.py, exceptions.py, qc.py" relevance="Ledger/reuse model, DOF accounting, schema, onset validation, exception hierarchy, excluded_runs QC key." />
    <file path="producer (gh api, main @ 2026-10-13): images/afni/preproc.py, argo/workflows/producer_minproc/*, argo/workflows/fmri_first_level_proc/fmri-first-level-proc-workflow-template.yaml, prefect/flows/first_level_queue_manager.py, images/globus/transfer.py, plans/017-func-reprocess-replace-metrics.md, docs/pipelines.md, ADR 017" relevance="Producer contract, transfer/cleanup, queue completion semantics, reprocess campaign." />
  </context_files>

  <topics>
    <topic id="T1" title="Sequencing and environment">
      <decision status="decided" chosen="A1">Unchanged from prior session: feature branch against the local v4.0.0-build checkout; exact pins python 3.14, numpy 2.4.2, pandas 3.0.1; hyphenated URL at tag v4.0.0 only in the final pre-merge commit after the tag exists and tests re-pass; MIN_UPSTREAM_VERSION "4.0.0", reject >= 5.0.0.</decision>
    </topic>

    <topic id="C1" title="Discovery and S3 contract">
      <research>Bucket census 2026-10-13: derivatives/func = 180,220 objects, all derivatives/func/sub-XXXXXXXX/ses-{00,02,04,06}A/sub-XXXXXXXX_ses-..A_task-{nback,rest}_run-NN_space-MNI152NLin2009cAsym_bold.tar.gz; 11,721 subjects; nback runs 01-02; rest runs 01-06 (297 runs numbered 05/06). Tarball members: {task}_{run}/ root with _desc-boldref.nii.gz, _desc-confounds_timeseries.tsv, _space-MNI152NLin2009cAsym_bold.nii.gz, _space-MNI152NLin2009cAsym_brainmask.nii.gz; the producer archives its whole work dir minus surface files, so membership is not code-fixed. Producer first-level workflow strips sub- and calls --subj_id; queue completion test is any object under derivatives/first_levels/sub-{ID}/.</research>
      <decision status="decided" chosen="A1">Verified 1/3 + 2 concerns, revised. (1) One paginated list_objects_v2 over {func_s3_prefix}/sub-{id}/; strict key regex; non-matching keys WARNING and ignored; configured tasks only; sessions from listing. (2) Per run: require the four named members, extract only those with tarfile filter='data', extras logged INFO and ignored, missing member skips the run with WARNING and a QC reason. (3) mmps_mproc listed once per session (amended by C3: events only). (4) Run identity (session, task, int run) from the tarball key; run_label format unchanged. (5) Config: bucket <bucket>; func_s3_prefix (default derivatives/func) replaces fmriprep_s3_prefix (legacy key raises ConfigError); mmps_mproc_s3_prefix mmps_mproc; upload_prefix derivatives/first_levels; available_sessions and its validator removed. (6) Local mode: study.input_dir mirrors both S3 layouts; sessions/runs found by glob through the same parser. (7) CLI unchanged.</decision>
    </topic>

    <topic id="C2" title="Input integrity gate">
      <decision status="decided" chosen="none">No orchestrator-side handling. MNI BOLD corruption (2026-05-25..10-06; mechanism verified in pre-#629 writer code and in a stratified 12-run sample, integer-code signature in 12/12 corrupt-era runs, 0/5 controls) and partial-write sequencing are producer responsibilities; the user states outputs will be complete and correct before use. No cutoff gate, settle window, registration-marker QC or content QC metrics.</decision>
    </topic>

    <topic id="C3" title="Motion source">
      <research>compute_confounds copies the six base columns from _motion.tsv unchanged. All 29 runs with both sources: equal row counts, max abs difference 0.</research>
      <decision status="decided" chosen="A1">Verified 3/3. Six base parameters (trans_* mm, rot_* deg) from the tarball confounds TSV by name for every run; mmps_mproc *_motion.tsv not downloaded; mmps_mproc listing restricted to *_events.tsv for non-rest tasks; extract_motion_regressors re-pointed to the confounds path with derivative/NSS logic unchanged; precomputed derivative/power columns ignored deliberately; OrchestratorError if a base column is missing or confounds rows differ from BOLD frames.</decision>
    </topic>

    <topic id="C4" title="nback runs without events">
      <decision status="decided" chosen="A1">Verified 3/3 on items 2-5; the shared major concern was an artifact of verifiers reading the pre-C1 single-archive code. Events availability decided from the mmps_mproc listing before download; a non-rest run without events is excluded and its tarball not downloaded; run-level exclusion only, exit status unaffected; per-session preproc_excluded_runs [{run_label, task, reason}] in the session QC JSON (reason events_missing, or exception class + message); a configured analysis with zero usable runs gets an outcome record status 'skipped', reason 'no_usable_runs' (session becomes 'partial'); tasks with some usable runs proceed on survivors. Exceptions: expected per-run data problems raise the orchestrator's existing OrchestratorError extended with an optional context dict (mirroring FmriFirstLevelError) carrying a categorized reason; unexpected exceptions remain run-level skips recorded as reason 'unexpected_error' with class and message and logged with traceback. Field named preproc_excluded_runs to avoid collision with fmri-first-level-proc's excluded_runs QC key (qc.py:64).</decision>
    </topic>

    <topic id="C5" title="Rest tissue regressors">
      <research>R1 (research agent; receipts checked via Europe PMC abstracts): Muschelli 2014 NeuroImage 96:22-35 abstract verbatim "We demonstrate that the use of aCompCor removes motion artifacts more effectively than tissue-mean signal regression" (low-motion pediatric sample). Parkes 2018 NeuroImage 171:415-436 abstract: mean WM/CSF regression "is not sufficient"; "aCompCor pipelines may only be viable in low-motion data"; GSR "improved the performance of nearly all pipelines". Ciric 2017 (arXiv:1608.03616): 193 young adults. fmri-first-level-proc v4 rest_conn requires single-column CSF_paths/WM_paths (DOF dry run counts one column each; derivatives from column 0; 3dTproject -ort regresses all columns).</research>
      <decision status="decided" chosen="A1">Keep the current mean time-course model (mean CSF + mean WM + GS, derivatives as configured, FD censoring, bandpass). Read csf, white_matter, global_signal by name; missing column raises OrchestratorError with context reason 'tissue_signal_missing' (run lands in preproc_excluded_runs). Never pass multi-column aCompCor through CSF_paths/WM_paths. An aCompCor option in fmri-first-level-proc is deferred to some future release, not this cycle. R2 research declined by the user.</decision>
    </topic>

    <topic id="C6" title="Non-steady-state frames and event timing">
      <research>Producer flags NSS frames 0..n-1 with one indicator column each; it does not trim. All 154 events files in the bucket start with a 'dummy' event at 0. Siemens/Philips: dummy = nss_frames*TR exactly. GE DV26 (16 frames, 12.8 s): dummy 0-12.0 s, first nback cue at 12.01-12.52 s (straddles by 0.28-0.79 s); GE DV25 (5 frames): dummy 0-3.2 s, cue at 3.71 s. MID/SST events start >= 1.2 s after the NSS window. fmri-first-level-proc rejects onset < 0.</research>
      <decision status="decided" chosen="A1">Verified 2/3 + 1 concerns (scanner-correlated clipping, acknowledged). Keep trimming NSS frames (n_remove = count of non_steady_state_outlier_* columns) and the onset shift n_remove*TR; boundary rule: drop only if shifted onset + duration &lt;= 0, clip straddling events to onset 0 with the remaining duration; per-run QC n_remove, n_events_dropped_before_start, n_events_clipped_at_start, clipped_seconds (return contract extended); document GE first-cue clipping as a modeling assumption with a scanner-covariate recommendation (T14).</decision>
    </topic>

    <topic id="C7" title="Redundant or obsolete orchestrator preprocessing">
      <decision status="decided" chosen="A1">Verified 3/3. Remove apply_brain_mask (producer already ran the identical 3dcalc a*step(b) with the shipped mask); pass the shipped BOLD to NSS trimming and update call sites. Remove registration Dice QC (compute_registration_quality, anat-mask discovery, all anat_mask_path plumbing, qc.preproc.registration_quality raises ConfigError if present, 'registration' QC field). Keep DVARS, tSNR, mask volume and carpet plots; update all fMRIPrep docstring references. Remove legacy-archive migration (migrate_session_from_archive, LegacyArchiveCorruptError, 'migrate' routing, 'migrated' status; routing full|skip; tests updated); s3://<bucket>/derivatives/first_levels/ verified empty. Example config: drop registration_quality and the auto-migrate comment. User amendment: restart safety is a hard requirement, handled in C9.</decision>
    </topic>

    <topic id="C9" title="Checkpoint/restart safety (absorbs T3)">
      <summary>Next topic. Verified current state: (a) fmri-first-level-proc restart model is a per-analysis ledger in out_dir (stage, validate, promote, completion record); reuse requires a completion record, matching stage hash, outputs on disk, no upstream re-execution, no failed runs; stage hash covers config fields, SHA-256 of derived input files, upstream stage hashes and tool/package versions (engine.py:502-531); G4 refuses unrecorded files in out_dir; remove_previous is ledger-scoped; retirement sweep removes unlisted recorded outputs. (b) Producer first-level pods use a per-pod emptyDir (/scratch, 300Gi), so an Argo retry starts with an empty out_dir; the orchestrator forces remove_previous True (orchestrator_utils.py ~3009), a no-op on an empty dir that rules out reuse elsewhere. (c) The orchestrator's only durable checkpoint is the per-session S3 _COMPLETE sentinel (routing skip); upload_session_to_s3 writes it whenever any task was preprocessed, regardless of analysis outcomes, so a session whose analyses all failed is never retried. (d) The producer queue treats any object under derivatives/first_levels/sub-{ID}/ as subject-complete, so a subject interrupted after some sessions is never resubmitted. (e) Concurrent per-file upload can store outputs without their ledger; interrupted attempts can leave stale S3 objects.</summary>
      <approaches>
        <approach id="A" label="Restore and let the ledger decide" feasibility="med" risk="med">
          <description>For an incomplete session, download that session's ledgers plus the outputs their completion records list from S3 into out_dir, run with remove_previous false, and let the ledger reuse unchanged stages. Requires ledger-aware upload ordering (outputs, then each analysis ledger, then _COMPLETE last) so a restored out_dir never trips G4.</description>
        </approach>
        <approach id="B" label="Recompute incomplete sessions" feasibility="high" risk="low">
          <description>Sessions without a valid _COMPLETE are recomputed from scratch on a fresh out_dir; upstream reuse is unused in production.</description>
        </approach>
      </approaches>
      <decision status="open" chosen="none">User undecided A vs B; work both up with costs. Upstream-aligned framing for the rest of C9: _COMPLETE means every configured analysis returned a FirstLevelResult (success/warning); any terminal-failure classification must use upstream's exception taxonomy (ModelError data quality vs ToolExecutionError tool failure), not a new orchestrator rule; subject-level completion marker for the queue (coordinate in C8); S3-side retirement sweep and upload ordering mirroring upstream's write protocol.</decision>
    </topic>

    <topic id="C8" title="Cross-repo coupling">
      <decision status="open" chosen="none">Not yet discussed. Scope per Q3: orchestrator and fmri-first-level-proc repos change; producer changes are relayed by the user. Known items: queue completion semantics (depends on C9 subject marker); producer vendored configs reference retired keys (concatenate_runs, legacy extraction keys) and will be replaced by re-download; scripts/generate_first_level_subjects.py still lists derivatives/fmriprep/; scripts/compileFirstLevelQCmetrics.py scrapes v3-era log lines; pod limits 2 CPU, 10Gi, 7200 s deadline vs v4 workload; producer AFNI 26.3.02 vs upstream validated 26.0.09; producer environment.yaml unpinned.</decision>
    </topic>

    <topic id="T2" title="Explicit run_labels injection"><decision status="open" chosen="none">Re-present with the producer premise: integer run from the tarball key (C1 item 4); non-contiguous survivors after C4 exclusions; rest runs up to 06.</decision></topic>
    <topic id="T3" title="remove_previous under the ledger"><decision status="open" chosen="none">Absorbed into C9.</decision></topic>
    <topic id="T4" title="Task tissue regressors"><decision status="open" chosen="none">Not discussed. Renderer replaces task paths wholesale, dropping template CSF/WM; reconsider with C5 (mean csf/white_matter columns pending from producer).</decision></topic>
    <topic id="T5" title="Smoothing migration"><decision status="open" chosen="none">Not discussed. Producer output is unsmoothed (verified in docs and code).</decision></topic>
    <topic id="T6" title="chdir removal and absolute paths"><decision status="open" chosen="none">Not discussed.</decision></topic>
    <topic id="T7" title="Strict v4 events validation"><decision status="open" chosen="none">Not discussed. Events format verified: columns onset, duration, response_time, trial_type (quoted strings; 'n/a' response_time); nback conditions 8 face/place x load conditions plus 'cue' and 'dummy'. Interacts with C6 (dummy dropped, GE cue clipped) and upstream G3/G4 findings.</decision></topic>
    <topic id="T8" title="GSR always on"><decision status="open" chosen="none">Not discussed. C5 keeps GS in the rest model; confirm injection policy.</decision></topic>
    <topic id="T9" title="Output collection and QC builder"><decision status="open" chosen="none">Not discussed. Must incorporate preproc_excluded_runs, 'skipped' analysis outcomes (C4), C6 clipping fields.</decision></topic>
    <topic id="T10" title="QC embedding and flagging"><decision status="open" chosen="none">Not discussed. Note upstream excluded_runs vs orchestrator preproc_excluded_runs distinction.</decision></topic>
    <topic id="T11" title="S3 upload scope"><decision status="open" chosen="none">Not discussed. Interacts with C9 upload ordering and S3 retirement sweep.</decision></topic>
    <topic id="T12" title="Censoring metric labeling"><decision status="open" chosen="none">Not discussed. Producer FD is Power FD (50 mm radius); orchestrator/upstream censoring metric to be reconciled.</decision></topic>
    <topic id="T13" title="NaN-to-999 motion imputation x rest notch filter"><decision status="open" chosen="none">Not discussed. Producer derivative columns are n/a in row 0, but C3 recomputes derivatives from base columns (which have no n/a).</decision></topic>
    <topic id="T14" title="Documentation"><decision status="open" chosen="none">Not discussed. Adds: C3 motion-source departure from the documented mmps_mproc contract; C6 GE clipping assumption; all fMRIPrep references; underscore URL in CLAUDE.md/MEMORY.md.</decision></topic>
    <topic id="T15" title="Tests"><decision status="open" chosen="none">Not discussed. Scope now includes producer-shaped fixtures, removal of migration tests, C4/C6 behaviours.</decision></topic>
    <topic id="T16" title="Upstream report-back selection"><decision status="open" chosen="none">Not discussed. Group G1-G6 as recorded in the prior interim report.</decision></topic>
  </topics>

  <verified_facts>
    <bucket>s3://<bucket> top level: backup, config, derivatives (fastsurfer, func, func_surf, registration, subregions), logs, mmps_mproc, quarantine, scratch, tabulated, tools; no derivatives/first_levels objects. Versioning never enabled. mmps_mproc: 1,103 objects, 17 subject folders (11 with events, 16 with motion) because producer's delete-globus-input deletes mmps_mproc/{subj}/ after every successful workflow (producer-long-master-workflow-template.yaml ~1218-1278). Exhaustive key search (all prefixes, FastSurfer non-standard keys included): task timing exists only in mmps_mproc and a few scratch/backup test copies; tabulated behaviour tables hold summaries only. Tarballs contain no task timing (members, confounds columns, NIfTI header fields and AFNI extension checked on two nback runs).</bucket>
    <producer>Plan 017 reprocesses every derivatives/func run (all ~11,721 subjects) after the float16 writer fix (#629, pinned 2026-10-06 19:20Z), read-back guard (#630, 19:36:52Z) and CompCor change (#643, 2026-10-08 01:43:32Z); 794 post-fix tarballs (50-subject pilot) existed on 2026-10-13. Globus transfer deliberately carries events files into mmps_mproc; no step persists them; docs/pipelines.md assumes analysts align with task-timing files but never names a location.</producer>
  </verified_facts>

  <action_items>
    <item priority="P0" target_mode="brainstorm" description="Resume at C9 (A vs B with costs), then C8, then T2, T4-T16; one topic per message with N=3 proposal verification." />
    <item priority="P0" target_mode="none" description="User to relay to the producer maintainer: (1) retain mmps_mproc/{subj}/{ses}/func/*_events.tsv past delete-globus-input (treated as fact here); (2) add per-frame mean 'csf' and 'white_matter' columns to the confounds TSV from the masks compute_confounds already builds (C5 dependency)." />
    <item priority="P1" target_mode="none" description="Superseded facts in the prior interim report (mmps_mproc_noimages prefix; fMRIPrep-era assumptions) must not be reused." />
  </action_items>

  <next_steps>Resume /brainstorm at C9. No /implement until the final report passes the Pre-Report Audit; T1 still gates the merge on upstream tag v4.0.0.</next_steps>
</brainstorm_report>
