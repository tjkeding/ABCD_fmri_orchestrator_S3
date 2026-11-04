<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-10-15T19:09:13Z" report_type="final" scope="orchestrator" supersedes="brainstorm_history/ABCD_fmri_orchestrator_S3_brainstorm_20261013_191742.md (interim)" companion="ABCD_fmri_orchestrator_S3_brainstorm_upstream_20261015_150913.md (fmri-first-level-proc work, separate report per user directive)" />

  <scope>
    This report specifies ONLY work in the ABCD_fmri_orchestrator_S3 repository plus two local-only production config deliverables. All work that fmri-first-level-proc must do is in the companion upstream report; this report references it only where the orchestrator depends on it. Every decision below is user-locked. Where a later lock amended an earlier one, only the final state is stated. Producer is out of scope: no producer changes are specified; the orchestrator remains callable exactly as before (single --subj_id).
  </scope>

  <user_directives>
    <directive>Proc config is authoritative for HOW (processing options); the orchestrator controls WHAT/WHEN and fills only run-specific paths, output locations/prefixes, run_labels and the selection of analyses to run.</directive>
    <directive>Producer is out of scope; the deliverable production configs are handed to the producer maintainer to adopt as he sees fit.</directive>
    <directive>Absolute paths everywhere; relative paths are rejected.</directive>
    <directive>No code change goes untested; /test design precedes /implement build.</directive>
    <directive>Before proposing any new exception class, confirm no existing orchestrator or fmri-first-level-proc class fits.</directive>
    <directive>Orchestrator and upstream reports are kept separate.</directive>
  </user_directives>

  <context_files>
    <file path="orchestrator_utils.py" relevance="Discovery/download, routing and sentinel, extract_motion_regressors (~1731-1867), extract_tissue_signals (~1877), fix_nback_cue_labels (~1938-2020), format_task_timing (~2037), apply_smoothing (~2140), compute_preproc_qc (~2442), PRIMARY_OUTPUT_SUFFIX (~2625), consolidate_session_qc (~2804-2908), build_first_level_config (~3009-3024), config validators (~3259-3326)." />
    <file path="orchestrate_first_level.py" relevance="_derive_session_status (~111-126), smoothing call site (~750-762), chdir wrapper (~817-821, 912-913), upload and sentinel." />
    <file path="orch_config_final.yaml" relevance="O1 template (2026-04-07 16:37:21), source of orch_config_production.yaml; left untouched." />
    <file path="proc_config_final.yaml" relevance="P1 template (2026-10-01 12:12:17), source of proc_config_production.yaml; left untouched." />
    <file path="<local_path>/fmri-first-level-proc (branch v4.0.0-build, read-only)" relevance="v4 contract: first_level_config.py closed schema (87-144), estimability.py floors (41, 440-499), task_conn_first_level.py non-beta nuisance (337-350) and floor scope (537-541), roi.py extract_roi_stats (131-177) and roi_qc (381-460), engine.py ledger remove_previous (195-299), first_level_utils.py build_hrf_string (414-427), nuisance.py censoring (12-20, 50-80, 382-406)." />
    <file path="<local_path>/Schaefer2018_400Parcels_7Networks_order_Tian_Subcortex_S4_3T_MNI152NLin2009cAsym_2mm.nii.gz" relevance="Parcellation template (97x115x97, 2 mm, AFNI space ORIG); copied by the deployment image to /app/ABCD_fmri_orchestrator_S3/." />
  </context_files>

  <topics>
    <topic id="T1" title="Sequencing, environment and version gate">
      <summary>Exact pins and upstream version gate; baseline for implementation.</summary>
      <decision status="decided" chosen="A1">Exact pins python 3.14, numpy 2.4.2, pandas 3.0.1. MIN_UPSTREAM_VERSION "4.0.0"; reject upstream &gt;= 5.0.0. The hyphenated upstream URL at tag v4.0.0 (git+https://github.com/tjkeding/fmri-first-level-proc.git@v4.0.0) enters environment.yaml only in the final commit, after the upstream v4.0.0 tag exists and tests re-pass. Upstream v4.0.0 must contain FD censoring (companion report U1); no separate version bump. BASELINE (supersedes the earlier feature-branch plan): /implement works on main directly on top of the current uncommitted working tree (superseded v3.2.0 build); no branch, no preservation commit; commit and push to main at the end after tests and /document. /implement plan enumerates kept items (per-run path collection and list-form keys, legacy-key rejection, stat_buckets/QC alignment) versus revised items (forced remove_previous replaced by proc-config authority; MIN 3.2.0 replaced by 4.0.0; fMRIPrep discovery replaced by the per-file layout; docs rewritten).</decision>
    </topic>

    <topic id="C1" title="Discovery, S3 contract and input-completeness gate">
      <summary>Per-file producer layout only; one listing per subject; subject-level completeness gate.</summary>
      <decision status="decided" chosen="C1-REV + C1-GATE">(1) New per-file layout only: no tarball extraction, no mmps_mproc, no legacy-layout reader. (2) All S3 input LOCATIONS are configurable path templates (placeholders) to be filled after the producer rerun: s3.func_prefix_template and s3.anat_prefix_template. Formats as specified: per-run BOLD, brainmask, confounds TSV, nback events TSV (in the derivatives func directory), MNI CSF and WM binary masks (separate files, anat grid). boldref is dropped. (3) One paginated list_objects_v2 per subject; session, task and integer run parsed from the BOLD filename by strict regex; non-matching keys logged WARNING and ignored; configured tasks only; events and tissue availability determined from the same listing. (4) Subject-level gate, evaluated from that listing before any download, analysis or upload: every discovered run of a configured task requires brainmask and confounds TSV; every session containing a run of an analysis whose proc template requests CSF_paths/WM_paths requires its CSF and WM masks. Any absence raises OrchestratorError listing every missing key; nothing is uploaded; exit non-zero. No runs found raises the same. Carve-outs: missing nback events stays a terminal run-level exclusion (C4); a zero-voxel tissue mask stays run-level tissue_mask_empty. After the gate passes, C9 operates unchanged. (5) Config: bucket via s3.bucket; legacy keys fmriprep_s3_prefix, mmps_mproc_s3_prefix and available_sessions raise ConfigError. (6) Local mode: study.input_dir mirrors the S3 layout and is parsed by the same function. (7) CLI unchanged. (8) Documented limitation (T14): a session with no files written yet is invisible; first-level invocation must follow producer completion.</decision>
    </topic>

    <topic id="C2" title="Input integrity">
      <decision status="decided" chosen="none">No orchestrator-side corruption handling, cutoff gate, settle window or content QC. Producer output correctness is producer's responsibility.</decision>
    </topic>

    <topic id="C3" title="Motion source">
      <decision status="decided" chosen="A1">Six base parameters (trans_* mm, rot_* degrees) read by name from the per-run confounds TSV; extract_motion_regressors re-pointed to it with derivative/NSS logic unchanged; precomputed derivative/power columns ignored; OrchestratorError if a base column is missing or confounds rows differ from BOLD frames.</decision>
    </topic>

    <topic id="C4" title="Runs without events and run-level exclusion">
      <decision status="decided" chosen="A1">A non-rest run without events (per the listing) is excluded before download; run-level, exit status unaffected. Session QC JSON top-level preproc_excluded_runs (schema in T9). A configured analysis with zero usable runs gets status 'skipped', reason 'no_usable_runs'. Expected per-run data problems raise the existing OrchestratorError extended with an optional context dict (mirroring FmriFirstLevelError) carrying a categorized reason; unexpected exceptions remain run-level skips with reason 'unexpected_error', class and message, logged with traceback. Name preproc_excluded_runs avoids collision with upstream's excluded_runs QC key.</decision>
    </topic>

    <topic id="C5" title="Tissue regressors (rest and task)">
      <decision status="decided" chosen="C5-MASK + T4">Mean CSF and WM per run computed by the orchestrator from producer MNI binary masks, only when the proc template's plural key (CSF_paths/WM_paths) is non-null: (1) 3dresample -master &lt;run BOLD&gt; -rmode NN -input &lt;mask&gt;; (2) no erosion or threshold; (3) intersect with the run BOLD brainmask via 3dcalc step(a)*step(b); (4) 3dmaskave -quiet -mask on the NSS-trimmed BOLD, single-column regressor; (5) zero voxels after intersection raises OrchestratorError reason tissue_mask_empty (run lands in preproc_excluded_runs); missing masks are caught by the C1 gate; (6) per-run post-intersection csf_mask_voxels/wm_mask_voxels in session QC JSON. No GS computation (GS_paths null in production; fill GS_paths only if the template key is non-null). Task blocks: same procedure into per-run Mode A lists aligned with scan_paths only when non-null; null/absent keys are omitted; validate_proc_template raises ConfigError for non-null singular CSF_path/WM_path in task blocks; replace the orchestrator_utils.py ~3018-3024 overwrite that drops tissue keys; task processed_files carry csf/wm lists. Production: task blocks null (no task tissue regressors); rest non-null. extract_tissue_signals (~1877) fMRIPrep-column logic is replaced. Never pass multi-column aCompCor.</decision>
    </topic>

    <topic id="C6" title="Non-steady-state frames and event timing">
      <decision status="decided" chosen="A1">Trim NSS frames (n_remove = count of non_steady_state_outlier_* columns) and shift onsets by n_remove*TR; drop an event only if shifted onset + duration &lt;= 0; clip straddling events to onset 0 with remaining duration; per-run QC n_remove, n_events_dropped_before_start, n_events_clipped_at_start, clipped_seconds; document GE first-cue clipping as a modeling assumption with a scanner-covariate recommendation (T14).</decision>
    </topic>

    <topic id="C7" title="Remove redundant preprocessing">
      <decision status="decided" chosen="A1">Remove apply_brain_mask (pass shipped BOLD to NSS trimming). Remove registration Dice QC (compute_registration_quality, anat-mask discovery, anat_mask_path plumbing, 'registration' QC field; qc.preproc.registration_quality raises ConfigError). Keep DVARS, tSNR, mask volume, carpet plots. Remove legacy-archive migration (migrate_session_from_archive, LegacyArchiveCorruptError, 'migrate' routing, 'migrated' status); routing is full|skip. Update fMRIPrep docstring references.</decision>
    </topic>

    <topic id="C9" title="Checkpoint and restart safety">
      <decision status="decided" chosen="B (as amended by T8 and SCOPE-CLEANUP)">(1) Session-granular recompute: a session without _COMPLETE is recomputed from an empty out_dir; no S3 ledger restore; remove_previous is taken from the proc config (not forced). (2) _COMPLETE is written only if every configured analysis reached a terminal outcome: FirstLevelResult success/warning, C4 skipped/no_usable_runs or events_missing_conditions, or ModelError; all other exceptions are non-terminal: upload without sentinel and exit non-zero after all sessions. (3) Upload, verify sizes, sweep remote keys not in the expected set (T11) on BOTH terminal and non-terminal paths, write _COMPLETE last. (4) No per-analysis or per-stage S3 checkpointing. (5) No _SUBJECT_COMPLETE marker and no --reset-subject-markers mode (dropped as producer-facing). No attempt counter or retry cap; operators rerun; session QC JSON error_type identifies deterministic failures. README documents the per-session completion contract only.</decision>
    </topic>

    <topic id="C8" title="Cross-repo coupling (orchestrator-side remainder)">
      <decision status="decided" chosen="C8a-C8d after SCOPE-CLEANUP">C8a: orchestrator tag is the single version authority: __version__ 4.0.0 and annotated tag v4.0.0 on the final commit, only after the upstream v4.0.0 tag exists; environment.yaml carries the T1 exact pins and the pip git URL at @v4.0.0; the T1 runtime gate is the backstop. No producer relay items. C8b: no AFNI validity check; AFNI &gt;= upstream minimum (26.0.09) proceeds; a logging.Filter on the logger passed to the upstream run function drops records whose getMessage() starts with "[UNVALIDATED_VERSION] AFNI "; outcome warnings drop records with code UNVALIDATED_VERSION and message starting "AFNI " (Python/numpy/pandas drift kept); one INFO provenance line per invocation (installed vs validated AFNI, suppression policy). C8d: session QC JSON records peak pod memory from cgroup v2 /sys/fs/cgroup/memory.peak, fallback ru_maxrss labeled 'rusage_largest_child'.</decision>
    </topic>

    <topic id="C8e" title="Production config deliverables (<bucket>)">
      <summary>Two local-only, git-ignored production configs built from O1/P1 adapted to the v4 contract; not published in either repo; handed to the producer maintainer.</summary>
      <decision status="decided" chosen="C8e-O1 + C8e-P1 + AUDIT-P2">
        FILES: orch_config_production.yaml and proc_config_production.yaml; both added to .gitignore; orch_config_final.yaml and proc_config_final.yaml untouched.

        ORCH_CONFIG_PRODUCTION (31 keys). KEEP: study.space MNI152NLin2009cAsym; study.TR 0.8; tasks[nback].condition_column trial_type; tasks[nback].conditions_exclude [dummy]; tasks[nback].fix_nback_cues true; qc.preproc.enabled true; qc.preproc.tsnr true; qc.preproc.carpet_plots true; qc.first_level.enabled true; analyses[*].name/type/task_label/post_id_out_pre/post_id_extract_pre/post_id_conn_pre; s3.enabled true; s3.upload_prefix derivatives/first_levels; s3.cleanup_after_upload true. SET: study.output_dir /scratch/first-level-proc-out/; study.force_recompute false; s3.bucket __PRODUCER_BUCKET__; s3.upload_max_workers 1. RENAME: study.fmriprep_dir -> study.input_dir = /scratch/fmriprep_in/ (local mode only). NEW placeholders: s3.func_prefix_template derivatives/sub-{subject}/ses-{session}/func; s3.anat_prefix_template derivatives/sub-{subject}/ses-{session}/anat (provisional pending producer rerun). REMOVED (ConfigError if present): study.calc_n_motion_derivs (derived = max over analyses of task: 1 if include_motion_derivs else 0; rest: max(motion_deriv_degree-1, 0); production gives 1), tasks[*].concatenate_runs, smoothing section, qc.preproc.registration_quality, analyses[*].fd_threshold, analyses[*].censor_prev_tr, s3.fmriprep_s3_prefix, s3.mmps_mproc_s3_prefix, s3.available_sessions.

        PROC_CONFIG_PRODUCTION, global: tr 0.8; num_cores 1; force_diff_atlas true; template_path /app/ABCD_fmri_orchestrator_S3/Schaefer2018_400Parcels_7Networks_order_Tian_Subcortex_S4_3T_MNI152NLin2009cAsym_2mm.nii.gz; derivatives_dirs ABSENT (would trigger upstream discovery of untrimmed producer files, bypassing orchestrator NSS trim, motion, timing and tissue preparation).

        ALL THREE BLOCKS: average_type mean; scale_to_psc true; smoothing {enabled: false}; remove_previous true; roi_zero_handling exclude (explicit). Orchestrator-filled (neutral placeholders in the file): run_labels, out_dir, out_file_pre, extraction.extract_out_file_pre, connectivity.conn_out_file_pre (conn blocks), and per-run path lists. fd_threshold comments state Power FD (50 mm) computed by fmri-first-level-proc (notch-filtered for rest); no comment claims remove_previous is forced.

        nback_act (task_act): fd_threshold 0.9; censor_prev_tr false; hrf_model GAM; custom_hrf null; include_motion_derivs false; use_tissue_derivs false; paths: scan_paths, task_timing_paths, motion_paths (orchestrator-filled), CSF_paths null, WM_paths null; cond_labels [0_back_negface, 0_back_posface, 0_back_neutface, 0_back_place, 2_back_negface, 2_back_posface, 2_back_neutface, 2_back_place, target_0back, instruction]; contrasts: the 29 functions and labels in CONTRAST_TABLE order; extraction: stat_buckets [Coef, Tstat, Zstat], extract_resids true, extract_raw_ptseries true.

        nback_conn (task_conn): fd_threshold 0.9; censor_prev_tr false; hrf_model GAM; custom_hrf null; include_motion_derivs false; use_tissue_derivs false; paths as nback_act with CSF_paths null, WM_paths null; cond_beta_labels [0_back_negface, 0_back_posface, 0_back_neutface, 0_back_place, 2_back_negface, 2_back_posface, 2_back_neutface, 2_back_place] (target_0back and instruction are modeled upstream as non-beta nuisance regressors, never floor-dropped); extraction: extract_pbseries true, extract_raw_ptseries true; connectivity: calc_conn parcellated, pcorr false, fishZ true, contrasts: the same 29 functions and labels in the same CONTRAST_TABLE order.

        rest_conn: fd_threshold 0.4; censor_prev_tr true; paths: scan_paths, motion_paths, CSF_paths, WM_paths (orchestrator-filled, non-null), GS_paths null (no GSR); bandpass [0.01, 0.1]; notch_filter_band [0.31, 0.43]; motion_deriv_degree 2; use_tissue_derivs true; keep_run_res_dtseries true; use_sequenced_bandpass false; extraction: extract_ptseries true, extract_raw_ptseries true; connectivity: calc_conn parcellated, pcorr false, fishZ true.

        CONTRAST_TABLE (label: function), identical for nback_act contrasts and nback_conn connectivity.contrasts:
        1 ave_negface: 0.5*0_back_negface+0.5*2_back_negface
        2 ave_posface: 0.5*0_back_posface+0.5*2_back_posface
        3 ave_neutface: 0.5*0_back_neutface+0.5*2_back_neutface
        4 ave_place: 0.5*0_back_place+0.5*2_back_place
        5 ave_zback: 0.25*0_back_negface+0.25*0_back_posface+0.25*0_back_neutface+0.25*0_back_place
        6 ave_tback: 0.25*2_back_negface+0.25*2_back_posface+0.25*2_back_neutface+0.25*2_back_place
        7 ave_negface-ave_posface: 0.5*0_back_negface+0.5*2_back_negface-0.5*0_back_posface-0.5*2_back_posface
        8 ave_negface-ave_neutface: 0.5*0_back_negface+0.5*2_back_negface-0.5*0_back_neutface-0.5*2_back_neutface
        9 ave_negface-ave_place: 0.5*0_back_negface+0.5*2_back_negface-0.5*0_back_place-0.5*2_back_place
        10 ave_posface-ave_neutface: 0.5*0_back_posface+0.5*2_back_posface-0.5*0_back_neutface-0.5*2_back_neutface
        11 ave_posface-ave_place: 0.5*0_back_posface+0.5*2_back_posface-0.5*0_back_place-0.5*2_back_place
        12 ave_neutface-ave_place: 0.5*0_back_neutface+0.5*2_back_neutface-0.5*0_back_place-0.5*2_back_place
        13 ave_tback-ave_zback: 0.25*2_back_negface+0.25*2_back_posface+0.25*2_back_neutface+0.25*2_back_place-0.25*0_back_negface-0.25*0_back_posface-0.25*0_back_neutface-0.25*0_back_place
        14 negface_tback-zback: 1*2_back_negface-1*0_back_negface
        15 posface_tback-zback: 1*2_back_posface-1*0_back_posface
        16 neutface_tback-zback: 1*2_back_neutface-1*0_back_neutface
        17 place_tback-zback: 1*2_back_place-1*0_back_place
        18 zback_negface-posface: 1*0_back_negface-1*0_back_posface
        19 zback_negface-neutface: 1*0_back_negface-1*0_back_neutface
        20 zback_negface-place: 1*0_back_negface-1*0_back_place
        21 zback_posface-neutface: 1*0_back_posface-1*0_back_neutface
        22 zback_posface-place: 1*0_back_posface-1*0_back_place
        23 zback_neutface-place: 1*0_back_neutface-1*0_back_place
        24 tback_negface-posface: 1*2_back_negface-1*2_back_posface
        25 tback_negface-neutface: 1*2_back_negface-1*2_back_neutface
        26 tback_negface-place: 1*2_back_negface-1*2_back_place
        27 tback_posface-neutface: 1*2_back_posface-1*2_back_neutface
        28 tback_posface-place: 1*2_back_posface-1*2_back_place
        29 tback_neutface-place: 1*2_back_neutface-1*2_back_place
        Downstream code and documentation must address outputs by contrast LABEL, never by sub-brick/index: activation and connectivity outputs carry different statistics, so identical contrast order does not imply identical indices.
      </decision>
    </topic>

    <topic id="C8e-NBACK" title="n-back event model">
      <research>41 sample ABCD nback events files: each run has 8 blocks (4 stimuli x 0-back/2-back, each once), each block 10 trials of exactly 1.9 s at SOA 2.50-2.62 s (block 24.4-24.8 s), each block preceded by one 'cue' event of 2.85-2.90 s, plus an initial 'dummy' (excluded). Per the investigator: the 0-back pre-block event is the presentation of the target image to be matched (encoding), the 2-back pre-block event is the text '2-back'. Stimulus-specific labels (one event per run, at most 2 per session) can never reach the task_conn floor of 3 and fall below the task_act floor of 2 when a single run survives (estimability.py:41, 440-499); task_conn floor applies only to cond_beta_labels (task_conn_first_level.py:537-541).</research>
      <approaches>
        <approach id="A" label="Stimulus-specific target labels (P1 as written)" feasibility="high" risk="high"><description>4 labels; frequently floor-dropped with their contrasts.</description><pros>Category-specific encoding estimate.</pros><cons>Unestimable in task_conn; unstable in task_act.</cons></approach>
        <approach id="B" label="Pooled target_0back" feasibility="high" risk="low"><description>One label for all 0-back target presentations.</description><pros>Every event modeled; never floor-dropped.</pros><cons>No per-stimulus encoding estimate (not planned).</cons></approach>
        <approach id="C" label="Fold target into 0-back block" feasibility="high" risk="med"><description>Relabel as 0_back_&lt;stim&gt;.</description><pros>Stimulus-specific.</pros><cons>Contaminates every load contrast.</cons></approach>
      </approaches>
      <decision status="decided" chosen="B">PV 3/3 verified. fix_nback_cue_labels: every cue preceding a 0-back block becomes target_0back; every cue preceding a 2-back block becomes instruction (the current level == "0" branch that writes the bare stimulus name is replaced). Label sets and contrasts as in C8e. instruction is a non-beta nuisance regressor in nback_conn (no beta series). No target_0back contrast; its Coef/Tstat/Zstat are still emitted per condition in nback_act. hrf_model GAM for both (PV 3/3): upstream builds GAM(8.6,.547,&lt;condition mean duration&gt;) (1.9 s trials, about 2.9 s target_0back and instruction); durations are constant within condition so per-event dmBLOCK timing adds nothing; methods text states that the duration-dependent height scaling differs from dmBLOCK only by a per-condition constant.</decision>
    </topic>

    <topic id="C8e-REST" title="Rest nuisance details">
      <research>Receipt-verified: ABCD DAIRC (Hagler et al. 2019, NeuroImage 202:116091, PMC6981278): "the mean time courses of cerebral white matter, ventricles, and whole brain, as well as their first derivatives"; rest "Frames with displacement (FD) greater than 0.3 mm are excluded from the regression"; task "Time points with FD greater than 0.9 mm are censored". Satterthwaite et al. 2013 18P/36P and Ciric et al. 2017 36P include tissue derivatives (with GSR); a 2025 study (PMC12336063) 28P includes WM/CSF derivatives without GSR; XCP-D 27P/aroma omit them; fMRIPrep provides derivative columns without a default. No benchmark isolates the tissue-derivative effect.</research>
      <decision status="decided" chosen="use_tissue_derivs true">Initial false recommendation withdrawn after 3/3 PV concerns. rest_conn use_tissue_derivs true. Methods caveats: no isolating benchmark; this model has no GSR and no motion quadratics, so DAIRC consistency applies to tissue derivatives only; cost 2 columns per run.</decision>
    </topic>

    <topic id="C8e-ROI" title="roi_zero_handling">
      <decision status="decided" chosen="exclude">PV 1 verified + 2 concerns resolved empirically: AFNI 3dROIstats -nzmean returns 0.000000 for an all-zero sub-brick (identical to include; no NaN). exclude removes subject-varying coverage attenuation from amplitude outputs; Pearson correlations are identical under both (static mask, constant scale); 3dTproject -polort makes retained-frame residuals zero-mean, so zeroed censored rows do not change r under 1ddot -dem. Parcel minimum-coverage rules are a group-level choice using upstream roi_coverage QC.</decision>
    </topic>

    <topic id="C8e-FD" title="Censoring metric and thresholds">
      <decision status="decided" chosen="D (upstream FD censoring)">The production thresholds were intended as Power FD thresholds. Upstream v4.0.0 computes and records both AFNI enorm and Power FD and censors on Power FD (companion report U1); no censor_metric key. Thresholds: nback_act 0.9, nback_conn 0.9, rest_conn 0.4 (filtered FD; user chose 0.4 over DAIRC 0.3) with censor_prev_tr true. Orchestrator MIN_UPSTREAM_VERSION stays 4.0.0 (FD ships in v4.0.0).</decision>
    </topic>

    <topic id="T2" title="Explicit run_labels">
      <decision status="decided" chosen="A1">Explicit integer run_labels in every analysis block (top-level key, sibling of paths), from the BOLD key's integer run (run-01 -&gt; 1). One per-run record per surviving run (run int plus BOLD/motion/timing/tissue paths); after exclusions derive scan_paths, companion lists and run_labels from the same filtered record list (no parallel appends). String run_label for QC unchanged. No orchestrator duplicate/length checks (upstream ConfigError). Unit test for non-contiguous survivors (1, 3, 4).</decision>
    </topic>

    <topic id="T4" title="Task tissue regressors">
      <decision status="decided" chosen="A1">See C5 (task part). Production task blocks null.</decision>
    </topic>

    <topic id="T5" title="Smoothing">
      <decision status="decided" chosen="A1">Remove orchestrator smoothing entirely: apply_smoothing (orchestrator_utils.py ~2140, Section J), call site orchestrate_first_level.py ~750-762, config reads ~165 and ~375, validator/default ~3259-3269 and ~3326, smoothing section of orch config schema/examples. Orch-config smoothing key raises ConfigError pointing to proc per-block smoothing {enabled, fwhm}. Production: smoothing {enabled: false} in all blocks.</decision>
    </topic>

    <topic id="T6" title="chdir removal and absolute paths">
      <decision status="decided" chosen="T6-PATHS A">Remove the os.chdir wrapper (orchestrate_first_level.py ~817-821, ~912-913) and its comment. Relative paths are rejected with ConfigError at startup for study.output_dir, study.input_dir (legacy fmriprep_dir raises ConfigError naming input_dir) and proc global.template_path (validated, never rewritten). Unit test asserts rejection.</decision>
    </topic>

    <topic id="T7" title="Events validation">
      <decision status="decided" chosen="A1">(A) Per-run checks on the final timing file (after cue relabel, conditions_exclude, C6 shift/clip), single parse: finite onset/duration else events_invalid_values; 0 &lt;= onset &lt; retained frames x TR else events_onset_out_of_range; run-level terminal exclusion into preproc_excluded_runs; no per-run condition-presence check. (B) Per analysis after exclusions: every required condition (task_act cond_labels; task_conn cond_beta_labels) present in the union of surviving runs, else outcome skipped/events_missing_conditions (terminal, not dispatched); presence only; trial floors stay upstream. (D) Session QC JSON per-run event counts and conditions present. Upstream Mode A union change is in the companion report (U2).</decision>
    </topic>

    <topic id="T8" title="Proc-config authority">
      <decision status="decided" chosen="A1">Keep: out_dir, out_file_pre, extraction/connectivity prefixes, per-run paths, run_labels, analysis selection. Fill CSF_paths/WM_paths/GS_paths only when the template key is non-null (GS_paths null = no GSR). fd_threshold and censor_prev_tr come only from the proc config (orch-config keys raise ConfigError). remove_previous honored from the proc config. global.tr: consistency check vs study.TR, no injection; missing global.tr raises ConfigError.</decision>
    </topic>

    <topic id="T9" title="Session QC JSON schema">
      <decision status="decided" chosen="A1">(1) preprocessing[run]: keep non_steady_state_trs, dvars, tsnr, brain_mask, carpet_plot_path; drop registration; task runs add n_events_dropped_before_start, n_events_clipped_at_start, clipped_seconds, n_events_written, conditions_present; csf_mask_voxels/wm_mask_voxels when computed; producer_fd_mean/producer_fd_max (T12). (2) top-level preproc_excluded_runs [{run_label, task, reason, error_type, message}] with reasons events_missing, events_invalid_values, events_onset_out_of_range, tissue_mask_empty, motion_non_finite, unexpected_error. (3) analyses[name]: type, status success|failed|skipped (skip reasons no_usable_runs, events_missing_conditions), upstream_status, qc_flagged, error, error_type, error_context, warnings, wall_time_seconds, qc, run_labels, terminal. (4) session: status success|partial|failed, n_analyses_skipped, all_terminal, peak_memory_bytes, peak_memory_source. (5) Replace PRIMARY_OUTPUT_SUFFIX with upstream names.frozen_primary.</decision>
    </topic>

    <topic id="T10" title="QC embedding and flagging">
      <decision status="decided" chosen="A1">(1) Embed upstream qc_summary verbatim under analyses[name].qc.upstream_qc_summary; warnings = upstream graded warnings after the C8b AFNI filter. (2) qc_flagged = upstream status == warning. (3) session.flags [{code, detail}] and session.any_flag with codes preproc_excluded_runs, analysis_skipped, analysis_failed, upstream_warning, events_clipped_at_start. No tissue_mask_small flag and no tissue-mask threshold keys. (4) Flags never affect outcomes, exclusions, terminal status or _COMPLETE. (5) No cohort QC aggregation tool this release.</decision>
    </topic>

    <topic id="T11" title="S3 upload scope">
      <decision status="decided" chosen="A1">Per-session expected key set: (1) analyses with FirstLevelResult: exactly result.output_files plus {pre}_ledger.jsonl; (2) raised analyses: {pre}_timing_errors.json and {pre}_qc_summary.json if present; (3) all of qc/; (4) preproc/ non-NIfTI per-run inputs passed upstream (motion, timing, CSF/WM regressors); (5) rendered sub-{id}_{ses}_first_level_config.yaml at session root. Remove the legacy concat/ branch. Plumb per-analysis FirstLevelResult file lists into upload.</decision>
    </topic>

    <topic id="T12" title="Censoring-metric labeling and QC">
      <decision status="decided" chosen="revised by C8e-FD and AUDIT-P2">(1) Everywhere the orchestrator describes censoring (README, INPUT_SPECIFICATION, code comments, example and <bucket> proc-config comments, QC descriptions): fd_threshold is a Power FD threshold (50 mm sphere), computed by fmri-first-level-proc from the motion parameters (notch-filtered for rest); replace the old radian-conversion text (README ~377, INPUT_SPECIFICATION ~472) with this accurate description. (2) Per-run preproc QC producer_fd_mean/producer_fd_max from the producer framewise_displacement column (unfiltered, 50 mm), over retained post-NSS frames, n/a ignored, source-labeled, informational only. No percent-above-threshold metric.</decision>
    </topic>

    <topic id="T13" title="Motion non-finite handling">
      <decision status="decided" chosen="A">Remove NaN-&gt;999.0 imputation (orchestrator_utils.py ~1847-1867). Check finiteness of the FINAL written motion matrix (all columns, post-NSS trim; derivatives computed pre-trim at ~1826-1845); any non-finite value excludes the run with OrchestratorError reason motion_non_finite and affected frame count/indices (run-level terminal, all tasks).</decision>
    </topic>

    <topic id="T14" title="Documentation (orchestrator repo)">
      <decision status="decided" chosen="A1">/document after implementation and tests: (a) README/INPUT_SPECIFICATION rewrite for the per-file layout with placeholder S3 templates; remove fMRIPrep/mmps_mproc. (b) Motion source, tissue procedure, NSS trim and GE clipping assumption with scanner-covariate recommendation, events validation including non-uniform conditions, motion_non_finite. (c) Censoring metric per T12. (d) Per-session completion contract, subject-level gate, producer-completion limitation, QC schema and flags, upload scope. (e) Retired-keys table (ConfigError plus replacement) and the absolute-path rule. (f) AFNI policy and exact pins. (g) KI-006 URL fix in CLAUDE.md. (h) Example configs regenerated from the <bucket> configs. (i) n-back model: target_0back semantics, instruction, label sets, 0.5/0.5 ave contrasts; outputs addressed by label, never by index. (j) remove_previous described as ledger-scoped removal; roi_zero_handling exclude documented; never quote stale upstream wording. (k) Methods notes: GAM duration convolution vs dmBLOCK scaling; tissue-derivative caveats. AID_LOG/.aid per convention.</decision>
    </topic>

    <topic id="T15" title="Tests (orchestrator repo)">
      <decision status="decided" chosen="A1">(1) /test design before /implement build. (2) Retire tests for removed subjects (tarball, legacy routing/migration, fMRIPrep discovery, orchestrator smoothing, registration Dice, NaN-&gt;999, chdir), each recorded with its locked decision. (3) Unit tests via _get_s3_client patch and tests/mock_subprocess.py: discovery/paginated listing/templates; subject gate; run_labels non-contiguous (1, 3, 4); proc authority (null keys, retired keys and singular task CSF/WM ConfigError, remove_previous, global.tr); relative-path rejection; events per-run and per-analysis union including split conditions; motion_non_finite including derivative leak and no 999; C5 mask sequence and tissue_mask_empty; QC schema/flags/all_terminal; terminal classification; upload set/sweep/_COMPLETE order; AFNI UNVALIDATED_VERSION filter; peak-memory fallback. (4) One real-AFNI integration test (skip if absent): synthetic 1 mm LIA mask to 2 mm differently oriented BOLD, 3dresample NN/3dcalc/3dmaskave vs numpy. (5) Shared-fixture orchestrator-vs-upstream events test. (6) NEW: fix_nback_cue_labels test (0-back pre-block cue -&gt; target_0back, 2-back -&gt; instruction; fixture covering all 8 block types). (7) NEW: both <bucket> configs pass the orchestrator validators and the upstream config loader (closed schema; contrasts reference only declared labels). (8) Small-N /run-local acceptance after the producer rerun.</decision>
    </topic>

    <topic id="T16" title="Upstream dependencies (orchestrator view)">
      <decision status="decided" chosen="A1">The orchestrator depends on upstream v4.0.0 containing U1-U5 of the companion report. Production is protected from upstream G2 (GS_paths null; keep_run_res_dtseries true). The orchestrator tag and the environment.yaml URL wait for the upstream v4.0.0 tag.</decision>
    </topic>
  </topics>

  <action_items>
    <item priority="P0" target_mode="test" description="/test design for the orchestrator test suite per T15 (including new items 6 and 7) before /implement build." />
    <item priority="P0" target_mode="implement" description="Implement T1, C1, C3-C7, C9, C8a/b/d, T2, T4-T13 and the n-back relabel (target_0back) per this report; generate orch_config_production.yaml and proc_config_production.yaml exactly per C8e and add both to .gitignore." />
    <item priority="P0" target_mode="implement" description="Gate the final environment.yaml URL, __version__ 4.0.0 and tag v4.0.0 on the upstream v4.0.0 tag (companion report)." />
    <item priority="P1" target_mode="test" description="/test run of the full suite; small-N /run-local acceptance after the producer rerun." />
    <item priority="P1" target_mode="document" description="/document per T14." />
  </action_items>

  <next_steps>/test design (T15) for the orchestrator, then /implement plan and build from this report. Upstream work proceeds from the companion report in the fmri-first-level-proc repository; the orchestrator's final pin and tag wait for upstream v4.0.0.</next_steps>
</brainstorm_report>
