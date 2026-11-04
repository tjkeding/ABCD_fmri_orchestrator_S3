<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-10-13T10:15:44Z" report_type="interim" />

  <interim_status>
    This is an INTERIM report. The brainstorm is paused mid-discussion and resumes at the exact point recorded under resume_point. Only T1 is locked. No Pre-Report Audit was run, because this report does not claim completeness; the final report (after all topics lock) supersedes it and must pass the full audit.
  </interim_status>

  <resume_point>
    The last assistant message presented a re-scoped analysis after the user disclosed that producer (not fMRIPrep) is the preprocessing module whose outputs feed the orchestrator. That message proposed Group C (C1-C8) and research candidate R1, and asked three questions (Q1-Q3). The user has NOT yet answered. Next session must:
    1. Re-present Group C, R1 and Q1-Q3 (verbatim content below) and obtain approval/amendment plus answers.
    2. Update the progress tracker to include the approved Group C topics (progress.sh init/update), ordered BEFORE the existing T2-T16.
    3. Dispatch R1 only if approved (research-agent-sonnet-xhigh, per Topic Approval Gate).
    4. Walk topics one per message, each recommendation passing N=3 proposal-verification (PV-1..PV-3) before presentation. T2 is re-presented after C1 locks, with its premise rebased onto producer run keys.
  </resume_point>

  <user_directives>
    <directive>Original scope: full sweep of this orchestrator and the unpublished fmri-first-level-proc v4.0.0-build workspace (read-only grant) to ensure full compatibility; report any upstream implementation issues for a report back to that project.</directive>
    <directive>Investigate code directly. Do NOT verify via reports or take shortcuts; this is the full pass.</directive>
    <directive>producer (<producer_repo_url>, private; read via gh api) is the preprocessing module. Its outputs are the orchestrator's inputs. fMRIPrep is definitively no longer used.</directive>
    <directive>Session-bound read-only grant on the fmri-first-level-proc workspace was issued this session (grants.sh grant --operations read). It clears at end-session and must be re-requested next session.</directive>
  </user_directives>

  <context_files>
    <file path="TODO_ABCD_fmri_orchestrator_S3_20261009_113924.md" relevance="Upstream-authored TODO for v4.0.0 harmonization (untracked in this repo). Used as a pointer only; every claim was re-verified against code." />
    <file path="orchestrator_utils.py" relevance="Discovery (discover_session_files 1211-1395), NSS detection (1563-1579), motion extraction (1735-1797, fMRIPrep column names trans_x/rot_x), tissue signal extraction (1883-1918, expects fMRIPrep csf/white_matter/global_signal columns), smoothing (2140-2197), timing formatting (2037-2134), output collection (2686-2690, 2782-2786), config rendering (build_first_level_config ~2943-3031), validation (3259-3269)." />
    <file path="orchestrate_first_level.py" relevance="Session processing, os.chdir (817-821, 913), smoothing call sites (84,165,375,750-762), S3 upload enumeration." />
    <file path="environment.yaml" relevance="python>=3.8, unpinned numpy/pandas, underscore URL @v3.2.0 (tag never created)." />
    <file path="<local_path>/fmri-first-level-proc (branch v4.0.0-build, uncommitted, untagged)" relevance="Upstream v4.0.0: first_level_config.py, bids.py, runsets.py, names.py, engine.py, events.py, nuisance.py, qc.py, runtime.py, tools.lock.yaml, task_act_first_level.py, rest_conn_first_level.py, pyproject.toml." />
    <file path="producer: images/afni/preproc.py" relevance="Defines per-run outputs and confounds TSV schema (compute_confounds)." />
    <file path="producer: argo/workflows/producer_minproc/functional-preprocessing-workflow-template.yaml" relevance="Driver: inputs from mmps_mproc/{subj}/{ses}/func, tarball construction rooted at {task}_{run}/." />
    <file path="producer: docs-internal/investigations/2026-10-06-mni-bold-float16-corruption-handoff.md" relevance="Corrupt MNI BOLD corpus (2026-05-25 to 2026-10-06)." />
    <file path="producer: prefect/flows/first_level_queue_manager.py; scripts/generate_first_level_subjects.py; scripts/compileFirstLevelQCmetrics.py" relevance="First-level submission, subject selection, log-scraping QC consumer." />
    <file path="producer: images/fmri-first-level-proc/Dockerfile; argo/workflows/fmri_first_level_proc/fmri-first-level-proc-workflow-template.yaml; vendored orch_config_final.yaml, proc_config_final.yaml, environment.yaml" relevance="producer vendors and runs this orchestrator in production." />
    <file path="producer: docs/pipelines.md" relevance="States first-level reads derivatives/func/...tar.gz and writes derivatives/first_levels/{subjID}/." />
    <file path="Session scratchpad copies (may not survive the session): <local_path>/producer" relevance="Re-fetch via gh api if absent. Not yet read in full: openspec confound-estimation spec, images/shared/func_iqm.py. Not yet fetched: scripts/computeNumNSS.ipynb, docs/metrics_data_dictionary.md, docs/architecture.md, ADR 017." />
  </context_files>

  <topics>
    <topic id="T1" title="Sequencing and environment">
      <summary>How to land v4.0.0 compatibility given upstream is implemented but unpublished; dependency pinning; version gate.</summary>
      <research>Codebase-verified: upstream pyproject requires-python >=3.12, numpy>=2.4.2, pandas>=3.0.1; packaged tools.lock.yaml v2.0.0 validated AFNI 26.0.09, python 3.14, numpy 2.4.2, pandas 3.0.1; runtime_check raises ToolVersionError below minimum and emits medium UNVALIDATED_VERSION on any mismatch with validated. Upstream remote is the hyphenated URL.</research>
      <approaches>
        <approach id="A1" label="Option B: feature branch now, merge after tag" feasibility="high" risk="low">
          <description>Implement on a feature branch validated against an editable install of the local v4.0.0-build checkout (install requires explicit per-invocation env approval). Add the @v4.0.0 pip line to environment.yaml only in the final pre-merge commit, after git ls-remote --tags confirms v4.0.0 exists AND the suite re-passes against the tag-installed package. Merge only then.</description>
          <pros>Work proceeds now; nothing references a nonexistent tag on main.</pros>
          <cons>Requires a second validation pass at tag time.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">LOCKED by user ("(b) exact validated pins, and option B confirmed"). Exact pins python=3.14, numpy=2.4.2, pandas=3.0.1, with a conda dry-run solve check (fall back to ranges only with explicit notice). URL https://github.com/tjkeding/fmri-first-level-proc.git. MIN_UPSTREAM_VERSION="4.0.0"; verify_upstream_version rejects >=5.0.0. Verification: PV-3 verified, PV-1/PV-2 concerns addressed. Carry-over: CLAUDE.md and MEMORY.md still use the underscore URL (fold into T14). NEW cross-repo implication from producer discovery: producer vendors its own environment.yaml (python>=3.8, unpinned) and uses AFNI_26.3.02 (not validated 26.0.09); see C8.</decision>
    </topic>

    <topic id="T2" title="Explicit run_labels injection">
      <summary>v4 requires run_labels (unique non-negative ints, length == scan_paths) or infers them in per-run-list mode via RUN_RE (^|_)run-(\d+)(_|\.); orchestrator currently renders none.</summary>
      <research>Presented with 3/3 verification under the fMRIPrep premise: carry integer run_dict["run"] through processed_files as "run_labels" (local name per_run_nums, distinct from the string per_run_labels/"labels"), include it in the equal-length check, write explicit top-level run_labels into every task_act/task_conn/rest_conn block, plus an orchestrator-side duplicate check raising OrchestratorError.</research>
      <approaches>
        <approach id="A1" label="Explicit integer run_labels from parsed run key" feasibility="high" risk="low">
          <description>As above, but the source must be rebased from fMRIPrep filenames to producer keys ({sub}_{ses}_{task}_run-NN; tar member {task}_{run}/...).</description>
          <pros>Removes dependence on upstream basename inference; survives non-contiguous surviving runs.</pros>
          <cons>Premise depends on C1 discovery design.</cons>
        </approach>
      </approaches>
      <decision status="open" chosen="none">NOT locked. User response to the presentation was the producer correction, which invalidates the fMRIPrep premise. Re-present after C1 locks; re-run N=3 verification with the producer premise.</decision>
    </topic>

    <topic id="T3" title="remove_previous under the ledger">
      <summary>v4 G4 refusal raises InputError on unrecorded prefixed files; orchestrator forcing remove_previous=True disables ledger reuse. Options: orchestrator pre-clear of first_level_out/{analysis}; keep True; ledger-based reuse.</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T4" title="Task tissue regressors">
      <summary>Renderer replaces task paths wholesale, dropping template CSF_path/WM_path; orchestrator extracts no task CSF/WM. Must be reconsidered in light of producer confounds (no mean CSF/WM columns; aCompCor available); see C5.</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T5" title="Smoothing migration">
      <summary>Remove orchestrator apply_smoothing (orchestrator_utils.py 2140-2197; validation 3259-3269; default 3326; call sites orchestrate_first_level.py 84,165,375,750-762); adopt v4 smoothing:{enabled,fwhm}; add explicit smoothing and roi_zero_handling blocks to templates. producer output is unsmoothed (confirmed).</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T6" title="chdir removal and absolute paths">
      <summary>Remove os.chdir (orchestrate_first_level.py 817-821, 913); output_dir/fmriprep_dir never abspath'ed; chdir precedes load_and_validate; v4 writes nothing to CWD.</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T7" title="Strict v4 events validation">
      <summary>v4 rejects non-finite onset/duration, onset >= run end, and (Mode A) requires every condition per run file; format_task_timing (2037-2134) checks none. Events source itself is now unresolved (C4).</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T8" title="GSR always on">
      <summary>build_first_level_config injects GS_paths (line ~3031) despite template GS_paths: null. producer confounds provide global_signal.</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T9" title="Output collection and QC builder">
      <summary>output_files is final-only; _psc filter (2686-2690, 2782-2786); n_nifti_outputs semantics differ success vs failure; min_outlier_epi; _raw_psc_ptseries; _s{N}mm siblings.</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T10" title="QC embedding and flagging">
      <summary>trials and roi_coverage/low_baseline records bloat orchestrator_qc.json; pct_censored scope differs task vs rest; new high-severity codes inflate qc_flagged; surface excluded_runs. Also: producer compileFirstLevelQCmetrics.py scrapes log lines v4 no longer emits (C8).</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T11" title="S3 upload scope">
      <summary>enumerate_upload_targets uploads all of first_level_out including v4 4D intermediates. Interacts with producer queue completion semantics (any object under derivatives/first_levels/{subj}/ marks complete).</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T12" title="Censoring metric labeling">
      <summary>fd_threshold is AFNI enorm, not Power FD (also in v3.1.0); README:377 / INPUT_SPEC:472 falsely claim radian conversion; are 0.9/0.4 FD-derived? producer confounds carry Power FD (50 mm radius, degrees converted) and producer motion rotations are in degrees.</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T13" title="NaN-to-999 motion imputation x rest notch filter">
      <summary>Pre-existing. producer derivative columns carry n/a in row 0 (source of NaN).</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T14" title="Documentation">
      <summary>Version refs, CSV-timing removal claim (README:262, INPUT_SPEC:424), sequenced-intermediates path, chdir text, Python floor, warning codes, underscore URL in CLAUDE.md/MEMORY.md, and all fMRIPrep references (now obsolete).</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T15" title="Tests">
      <summary>Five upstream-touching files (test_cr_implementation_extended.py, test_simulated_pipeline.py, test_coverage_gaps.py, test_motion_and_qc.py, test_preprocessing.py); load_and_validate render test; N=1 real-data smoke run. Scope expands substantially with the producer input migration (fixtures are fMRIPrep-shaped).</summary>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>
    <topic id="T16" title="Upstream report-back selection">
      <summary>Select which Group G findings go back to fmri-first-level-proc.</summary>
      <research>Group G (code-verified on v4.0.0-build):
        G1 task_act_first_level.py 980-1005: smoothed GLM omits -gltsym contrasts (present at 798-805); no smoothed index JSON.
        G2 rest_conn_first_level.py 663-672: concat mode (keep_run_res_dtseries:false) with use_tissue_derivs omits the GS derivative; per-run mode (544-552) includes it; nuisance.py 359-361 counts it in the DOF dry run. LIVE IN PRODUCTION via producer proc_config_final.yaml.
        G3 Mode A requires every condition in each run file (events.py 130-135, per run); Mode B checks the union.
        G4 Timing violations checked on all rows, including unmodeled conditions (events.py 211-221).
        G5 Stale upstream INPUT_SPEC/README: global-only keys raise ConfigError not warning; remove_previous semantics; Mode A, run_labels, smoothing, roi_zero_handling undocumented; TRIALS_FEW text; lockfile location/schema; QC schema; output names _parcellated.csv and sequenced dir; binary list (19 vs 15); nonexistent run-first-level alias.
        G6 log_nuisance_model logs singular paths only; undocumented 13-run caps.</research>
      <decision status="open" chosen="none">Not yet discussed.</decision>
    </topic>

    <topic id="GC" title="PROPOSED Group C: producer input migration (awaiting user approval)">
      <summary>Proposed in the last message, not yet approved. Intended to be walked before T2.
        C1 Discovery/parsing: replace fMRIPrep archive/session discovery with producer per-run tarballs; parse {sub}_{ses}_{task}_{run}. T2 rebases onto this.
        C2 Input integrity gate for the corrupt-corpus window: compare MNI in-mask mean against confounds global_signal mean (native-space, never corrupt; example run 268 decoded vs 317.3; corrupt files read ~100x). Decide gate design and fail vs skip.
        C3 Motion source: confounds TSV motion columns vs a persistent motion prefix (depends on Q1).
        C4 Events source: producer produces none (depends on Q1).
        C5 Rest tissue regressors without mean CSF/WM columns: (a) aCompCor components, (b) orchestrator computes mean CSF/WM (needs tissue masks, not shipped), (c) request producer add columns. Scientific choice; R1 candidate.
        C6 NSS handling at 5/8/16 flagged-not-trimmed frames: keep trimming via outlier columns; verify event-onset shifting alignment.
        C7 Redundant/broken orchestrator preprocessing: re-masking an already-masked BOLD; registration Dice (no anatomical mask shipped); tSNR/carpet overlap with producer FuncQC. Keep, drop or replace each.
        C8 Cross-repo coupling: source of truth for configs/env/image; stale producer vendored artifacts; queue completion semantics; log-scraping QC consumer; 2h/2 CPU/10 GiB pod limits under v4.</summary>
      <research>Research candidate R1 (awaiting approval; zero agents dispatched): evidence comparing mean WM/CSF signal regression with aCompCor for subject-level resting-state FC denoising, including motion-QC benchmark studies.</research>
      <decision status="open" chosen="none">Awaiting approval/amendment of C1-C8 and R1.</decision>
    </topic>
  </topics>

  <open_questions_for_user>
    <question id="Q1">Do mmps_mproc_noimages/{sub}/{ses}/func/*_events.tsv (and *_motion.tsv) still exist in the production bucket, and do they survive producer's post-success deletion of mmps_mproc/{subjID}/ inputs?</question>
    <question id="Q2">Has the corrupt MNI BOLD corpus been re-run or repaired? If so, from what date or commit are derivatives/func outputs trustworthy?</question>
    <question id="Q3">Who changes producer's images/fmri-first-level-proc/ and prefect/ code: this project directly, or the producer owner via a report?</question>
  </open_questions_for_user>

  <verified_facts>
    <producer>
      Per-run tarball key: derivatives/func/{subj}/{ses}/{subj}_{ses}_{task}_{run}_space-MNI152NLin2009cAsym_bold.tar.gz; subj is <individual_id> without sub- prefix in the producer flow (corruption handoff example uses sub- prefix; the first-level Argo template strips sub-; reconcile during C1). Tar rooted at {task}_{run}/.
      Tar members: {prefix}_space-MNI152NLin2009cAsym_bold.nii.gz (masked after warp via 3dcalc a*step(b), -datum float, TR set in pixdim[4] since the fix); {prefix}_space-MNI152NLin2009cAsym_brainmask.nii.gz (uint8); {prefix}_desc-boldref.nii.gz (native space, mean of NSS frames); {prefix}_desc-confounds_timeseries.tsv. Surface components are excluded from the volumetric tarball. No events, no raw motion file, no anatomical/tissue masks.
      Confounds columns: t_indx; non_steady_state_outlier_NN (one per NSS frame, flagged not trimmed); rot_z, rot_x, rot_y (degrees), trans_z, trans_x, trans_y (mm), each with _derivative1, _power2, _derivative1_power2; framewise_displacement (Power, 50 mm radius); dvars; global_signal (native-space brain mask mean); a_comp_cor_wm_NN, a_comp_cor_csf_NN (separate WM/CSF, up to 5 or 50% variance, zero-padded on NSS rows); t_comp_cor_NN (5); cosine_NN (1/128 Hz). n/a written for row 0 of derivatives, FD, DVARS. NO csf or white_matter mean-signal columns; orchestrator_utils.py 1883-1918 expects exactly those fMRIPrep names, so the rest path would fail.
      NSS counts: 5, 8 or 16 per run (nss_volumes.csv; 5s are GE DV25), passed as --nss-frames.
      Pipeline: ABCD minimal preprocessing (3dvolreg, B0, gradient nonlinearity, between-scan alignment), then producer warp to MNI152NLin2009cAsym 2 mm grid (97x115x97); no slice timing, no smoothing, no bandpass.
      Corruption: MNI BOLD written 2026-05-25 to 2026-10-06 stores float16 bit patterns under DT_UINT16 and is x-z transposed, with about 18% of brain voxels zeroed; not losslessly repairable; full re-run of Stages 3-5 needs native inputs that may have been deleted. Fix (int16 + scl_slope, F-order write, read-back verification) landed on a producer branch; deployment date unknown. Blast radius explicitly flags fmri_first_level_proc outputs built on these files.
      First-level hand-off: Prefect first_level_queue_manager submits one Argo workflow per subject; is_completed() treats ANY object under derivatives/first_levels/{subj}/ as complete. generate_first_level_subjects.py still lists derivatives/fmriprep/ with sub- prefixes. compileFirstLevelQCmetrics.py parses CloudWatch log lines (Step 5 mask, Dice, rotation unit check, censor/DOF lines) emitted by the v3-era pipeline.
      First-level pod: 2 CPU, 10Gi, 300Gi emptyDir /scratch, activeDeadlineSeconds 7200, spot retries.
      Vendored stale artifacts: orch_config_final.yaml (concatenate_runs, fmriprep_s3_prefix derivatives/fmriprep, mmps_mproc_s3_prefix mmps_mproc_noimages, "rotations converted to radians" comment); proc_config_final.yaml (Mode B singular task paths, legacy extract keys, rest keep_run_res_dtseries:false + use_tissue_derivs:true + injected GS = G2); environment.yaml python>=3.8 unpinned; Dockerfile AFNI_26.3.02.
    </producer>
    <orchestrator>
      discover_session_files globs fMRIPrep BOLD/confounds/masks and builds run_label f"{ses_label}_task-{task}_run-{run_num}". NSS detection counts non_steady_state_outlier* columns (compatible with producer naming). Motion currently from mmps_mproc motion.tsv; fMRIPrep-style base column names (trans_x..rot_z) match producer confounds names. run_orchestrator.py sees only exit codes 0/1. Working tree on main carries the uncommitted v3.2.0-target C1-C19 build (README, INPUT_SPEC, environment.yaml, example configs, both .py files modified).
    </orchestrator>
    <upstream_v4>
      Closed schema (TOP_KEYS; GLOBAL_KEYS = tr, num_cores, template_path, force_diff_atlas, derivatives_dirs; _COMMON includes run_labels, smoothing, roi_zero_handling; PATH_KEYS allow singular and plural tissue keys for tasks). Each run() calls clear_warnings() then runtime_check. Engine ledger {pre}_ledger.jsonl, G4 refusal, ledger-scoped remove_previous, staging .staging_{pre}_{step}; FirstLevelResult has output_files (final) and intermediate_files; qc.QC_KEYS schema; frozen primaries unchanged. Upstream state per its 2026-10-12 session memory: 634/634 tests passing, not yet documented/merged/tagged; its orchestrator TODO items are to land only after publication (consistent with T1).
    </upstream_v4>
  </verified_facts>

  <action_items>
    <item priority="P0" target_mode="brainstorm" description="Resume this brainstorm: obtain Q1-Q3 answers and approval of Group C (C1-C8) and R1; re-init progress tracker with Group C ahead of T2-T16; continue one topic per message with N=3 proposal verification." />
    <item priority="P0" target_mode="brainstorm" description="Re-request the session-bound read-only grant on the fmri-first-level-proc workspace at the start of the next session (user approval required; no self-grant)." />
    <item priority="P1" target_mode="brainstorm" description="Re-fetch producer files via gh api if the scratchpad copies are gone; read the confound-estimation openspec spec, images/shared/func_iqm.py, computeNumNSS.ipynb, metrics_data_dictionary.md, architecture.md, ADR 017 as needed for C-topics." />
    <item priority="P1" target_mode="brainstorm" description="Flag to user for cross-repo reporting: upstream bug G2 is live in producer's production proc config; producer corruption handoff names fmri_first_level_proc outputs as potentially built on corrupt BOLD." />
  </action_items>

  <next_steps>Resume /brainstorm in the next session (after /resume-project) at resume_point. No /implement work may begin until the brainstorm reaches a final report that passes the Pre-Report Audit; T1 additionally gates merge on upstream tag v4.0.0.</next_steps>
</brainstorm_report>
