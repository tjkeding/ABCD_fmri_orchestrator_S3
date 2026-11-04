<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-10-16T15:44:24Z" supersedes="ABCD_fmri_orchestrator_S3_implement_plan_20261016_143111.md (same session; superseded after the user's assumption review: A10-A13 resolved, A11 replaced by a three-way exit classification, A13 reversed)" />

  <assumptions_and_derived_decisions>
    Every item below was reviewed and approved by the user on 2026-10-16. Items marked [LOCKED 2026-10-16] were explicitly decided in discussion; the remaining items were approved as written.
    A1 [LOCKED 2026-10-16] Discovery accepts both input naming families: session token "ses-NN" or "ses-NNA" (both map to session code "NN"; outputs are always labeled "ses-NNA"); BOLD "_desc-preproc_bold.nii.gz" or "_bold.nii.gz" (after the space entity); brainmask "_desc-brain_mask.nii.gz" or "_brainmask.nii.gz". All patterns live in one module-level constant block.
    A2 [LOCKED 2026-10-16] Hard dependency on upstream v4 module fmri_first_level_proc.names. PRIMARY_OUTPUT_SUFFIX stays a public constant but is derived from names.frozen_primary. The test environment is moved to the local fmri-first-level-proc v4.0.0-build workspace before /test (separate, approval-gated environment change; not part of this build).
    A3 [LOCKED 2026-10-16] Brainstorm action item 3 (environment.yaml hyphenated URL at @v4.0.0, annotated tag v4.0.0) is excluded from this plan and runs as a later /implement after the upstream v4.0.0 tag exists. This build applies only the T1 exact pins, MIN_UPSTREAM_VERSION "4.0.0" and MAX_UPSTREAM_VERSION "5.0.0"; the environment.yaml pip line is left untouched.
    A4 [LOCKED 2026-10-16] __version__ = "4.0.0" is set in this build (tests/test_simulated_pipeline.py:400 asserts it). Nothing is committed until the release step.
    A5 [LOCKED 2026-10-16] study.input_dir is required in both modes. S3 mode downloads each needed key to os.path.join(input_dir, key); local mode reads the same tree.
    A6 [LOCKED 2026-10-16] Two defects in tests/test_production_configs.py (module-scoped fixture depending on function-scoped mock_logger; upstream closed-schema test loading placeholder/nonexistent paths) are fixed by the post-build /test pass, not this build. They are recorded in the build report.
    A7 [LOCKED 2026-10-16] Global-signal regressors use the same non-null switch as CSF/WM: when a block's template GS_paths is non-null, each run's GS regressor is the 3dmaskave mean over the run brainmask on the NSS-trimmed BOLD (no resample step, no extra input file, no gate requirement); QC field gs_mask_voxels; an empty brainmask excludes the run with reason tissue_mask_empty. This reverses the "No GS computation" clause of C5 only.
    A8 "ConfigError" in the brainstorm is implemented as the existing OrchestratorError (the orchestrator has no ConfigError; tests expect OrchestratorError; standing reuse-existing-exception-classes rule).
    A9 A per-run OrchestratorError that carries no context reason (missing base motion column, confounds/BOLD frame-count mismatch, no events after conditions_exclude, unreadable TR count) is recorded in preproc_excluded_runs with reason "unexpected_error" and error_type "OrchestratorError". No reason codes beyond the T9 list are introduced.
    A10 [LOCKED 2026-10-16] The subject-level input-completeness gate is evaluated over the sessions selected for processing: every discovered session in a normal invocation, only the selected session under --session. It runs before any download, analysis or upload.
    A11 [LOCKED 2026-10-16] Three-way exit classification (reverses C4's "exit status unaffected" for unexpected_error run exclusions only). Per FULL session: category 1 "failed, unexplained" when a session-level exception occurred, any analysis outcome is non-terminal, or any run was excluded with reason unexpected_error; else category 2 "failed, known cause" when any analysis outcome is failed with error_type ModelError; else category 0 "complete" (success, success with upstream warnings, skips with reasons no_usable_runs/events_missing_conditions, run exclusions with documented reasons events_missing, events_invalid_values, events_onset_out_of_range, motion_non_finite, tissue_mask_empty). A SKIP-routed session is category 0. The _COMPLETE sentinel is written for categories 0 and 2 and never for category 1. The subject exit code is the most severe category across processed sessions, with precedence 1 over 2 over 0. Under --skip-first-level no analysis outcomes exist: only session-level exceptions and unexpected_error exclusions yield category 1, everything else is 0, and no sentinel is written. Configuration, discovery, gate and other OrchestratorErrors raised before session processing exit 1 as today.
    A12 [LOCKED 2026-10-16] On the FULL path in S3 mode, any pre-existing local session output directory is removed before processing (C9 "recomputed from an empty out_dir"). In local mode existing local content is kept, so study.force_recompute-governed caching remains usable. S3 objects and downloaded inputs are never touched by this removal.
    A13 [LOCKED 2026-10-16] Preprocessing QC (tSNR, carpet plot, brain-mask volume) is computed on the NSS-trimmed BOLD. DVARS and producer FD are read from the confounds TSV and trimmed by n_remove, so every QC series covers the same retained frames.
    A14 Per-run preproc filenames use a zero-padded run tag (prefix "sub-{id}_ses-{NN}A_task-{task}_run-{run:02d}"); the QC string run_label stays "ses-{NN}A_task-{task}_run-{run}" (unpadded). Required by tests/test_run_labels.py.
    A15 Two files mapping to the same (session, task, run, role) or (session, anat role) raise OrchestratorError at discovery naming both keys. A discovered subject token different from --subj_id, or a space entity different from study.space when study_config carries "space", is logged WARNING and ignored.
    A16 decompress_if_needed is retained (directly unit-tested) but is no longer called by the pipeline (C1: no tarball extraction).
    A17 format_task_timing keeps its cache semantics: a cache hit returns zero drop/clip counts.
    A18 calc_n_motion_derivs is derived once per invocation as the maximum over the configured analyses' template blocks: task_act/task_conn contribute 1 if include_motion_derivs is true else 0 (absent = false); rest_conn contributes max(motion_deriv_degree - 1, 0) (absent or null = 0).
    A19 The AFNI provenance INFO line calls upstream runtime_check(logger) and load_lockfile() once in main() (not under --dry-run). Upstream run functions call clear_warnings() at their start, so the provenance call does not leak warnings into analysis results.
    A20 Session status: "success" iff every outcome is success; "failed" iff no outcome is success (including all-skipped and no outcomes); otherwise "partial". Status is descriptive QC; the exit category (A11) is computed separately.
    A21 Exceptions from load_and_validate on the rendered config remain session-level errors (category 1, no upload), as today.
    A22 --dry-run performs discovery (S3 listing or local walk) and the input gate (both read-only) and downloads nothing; it exits 0 when both pass.
    A23 S3 templates must contain both "{subject}" and "{session}" and must not start or end with "/". The single listing prefix is the common prefix of the two templates' text before "{session}" with "{subject}" substituted.
    A24 compute_tissue_mask_regressor writes its intermediate resampled and intersected masks to the output directory and deletes them after the voxel count; the intersection is computed as 3dcalc -a resampled_tissue -b brainmask -expr "step(a)*step(b)".
    A25 Paths in first_level_file_paths that are absent on disk or not under the session output directory are logged WARNING and excluded from the upload set.
  </assumptions_and_derived_decisions>

  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_brainstorm_20261015_150913.md" mode="brainstorm" key_items="2 implement action items in scope (item 2: T1, C1, C3-C7, C9, C8a/b/d, T2, T4-T13, n-back relabel, <bucket> configs, .gitignore); item 3 excluded per A3" />
    <report path="ABCD_fmri_orchestrator_S3_test_20261016_130307.md" mode="test" key_items="13 product-bug root causes; read as the test-contract checklist for item 2 (not a separate source of action items)" />
  </input_reports>

  <baseline_kept_vs_revised>
    Per T1, this build works on main directly on top of the current uncommitted working tree (the superseded v3.2.0-target build). KEPT from that tree: per-run path collection and list-form path keys (scan_paths, motion_paths, task_timing_paths, CSF_paths, WM_paths, GS_paths); legacy-key rejection (concatenate_runs, censor_path(s), legacy extraction keys); stat_buckets and FirstLevelResult-based QC alignment (build_first_level_qc_from_result, json_safe_context, typed upstream exception handling); per-file upload with _COMPLETE sentinel. REVISED: forced remove_previous replaced by proc-config authority; MIN_UPSTREAM_VERSION 3.2.0 replaced by 4.0.0 plus MAX 5.0.0; fMRIPrep tarball/mmps_mproc discovery replaced by the per-file producer layout; orchestrator masking, smoothing, registration QC, legacy migration and NaN-to-999 imputation removed; fd_threshold/censor_prev_tr/global.tr injection removed; the "all sessions failed" exit rule replaced by the A11 classification. User-facing documentation (README, INPUT_SPECIFICATION, example configs) is /document scope (T14) and is not edited here.
  </baseline_kept_vs_revised>

  <changes>

    <change id="C1" priority="P0" source_item="T1, C4, T9(5); test punch list items 5, 6">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Exception context, upstream version window, hard dependency on upstream names, derived primary-output table.</description>
      <spec>
        (a) OrchestratorError: define __init__(self, message="", context=None); call super().__init__(message); set self.context = context (None when not given). str(err) equals message. Docstring: mirrors fmri_first_level_proc FmriFirstLevelError; context carries a categorized "reason" for run-level exclusions.
        (b) Add at module top: "from fmri_first_level_proc import names as upstream_names" (unguarded import; v4 is a hard dependency per A2).
        (c) Section B: MIN_UPSTREAM_VERSION = "4.0.0"; add MAX_UPSTREAM_VERSION = "5.0.0". verify_upstream_version(logger, min_version=MIN_UPSTREAM_VERSION, max_version=MAX_UPSTREAM_VERSION): existing checks unchanged; additionally parse max_version with packaging Version (InvalidVersion raises OrchestratorError) and raise OrchestratorError when installed &gt;= max_version, message naming the installed version and "&lt; {max_version}". Info log: "fmri_first_level_proc version: %s (required &gt;= %s, &lt; %s)".
        (d) Replace the literal PRIMARY_OUTPUT_SUFFIX dict with PRIMARY_OUTPUT_SUFFIX = {t: upstream_names.frozen_primary(t, "") for t in ("task_act", "task_conn", "rest_conn")} and a comment stating the names are owned by fmri_first_level_proc.names (frozen interface). _primary_output_ok(analysis_type, out_dir, out_file_pre): return False for an unknown type; otherwise path = os.path.join(str(out_dir), upstream_names.frozen_primary(analysis_type, out_file_pre)); return isfile and size &gt; 0. Update compute_first_level_qc and build_first_level_qc_from_result docstrings that cite "&gt;= 3.2.0" to cite the upstream names module.
        (e) Header comment block: "Last updated: 10/16/26".
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - the unguarded names import makes the module unimportable on upstream &lt; 4.0.0, by design (A2); every test run requires the v4 workspace install.</risk>
      <rollback>Restore orchestrator_utils.py from the pre-build working-tree copy (the copy saved by the build before editing).</rollback>
    </change>

    <change id="C2" priority="P0" source_item="C7, T5, C5, C1-REV; test punch list item 3">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Delete the eleven retired symbols and the legacy routing branch.</description>
      <spec>
        Delete entirely: LegacyArchiveCorruptError, migrate_session_from_archive, discover_available_sessions, download_session_data, discover_local_mmps_files, extract_session_archive, discover_session_files, apply_brain_mask (Section E removed), extract_tissue_signals, apply_smoothing (Section J removed), compute_registration_quality. Keep the tarfile and bz2 imports (decompress_if_needed still uses both). determine_session_routing(s3_config, sub_id, session, force_recompute, logger): delete Step 2 (legacy tarball probe); routing is "skip" or "full"; docstring lists only these two; the "full" path performs exactly one head_object call (the sentinel probe via check_session_complete). Remove the "Legacy layout ... concat/" comments from the file. Update every docstring that says fMRIPrep to describe the producer producer (confounds TSV, brainmask, BOLD) without changing behavior (C7 docstring cleanup).
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - pure removal; call sites in orchestrate_first_level.py are rewritten in C12.</risk>
      <rollback>Restore the deleted functions from the pre-build copy.</rollback>
    </change>

    <change id="C3" priority="P0" source_item="C1 (C1-REV), A1, A5, A15, A22, A23">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Single-listing discovery of the per-file producer layout and S3 download into input_dir.</description>
      <spec>
        New Section C "File Discovery (producer per-file layout)".
        (a) Module constants (single edit point for the post-rerun naming correction): _SES = r"ses-(?P&lt;ses&gt;[0-9]+)A?"; _RUN_ENTITIES = r"^sub-(?P&lt;sub&gt;[A-Za-z0-9]+)_" + _SES + r"_task-(?P&lt;task&gt;[A-Za-z0-9]+)_run-(?P&lt;run&gt;[0-9]+)"; INPUT_FILE_PATTERNS = ordered dict role -&gt; compiled regex over the basename:
          "bold": _RUN_ENTITIES + r"_space-(?P&lt;space&gt;[A-Za-z0-9]+)_(?:desc-preproc_)?bold\.nii\.gz$"
          "brainmask": _RUN_ENTITIES + r"_space-(?P&lt;space&gt;[A-Za-z0-9]+)_(?:desc-brain_mask|brainmask)\.nii\.gz$"
          "confounds": _RUN_ENTITIES + r"_desc-confounds_timeseries\.tsv$"
          "events": _RUN_ENTITIES + r"_events\.tsv$"
          "csf_mask": r"^sub-(?P&lt;sub&gt;[A-Za-z0-9]+)_" + _SES + r"_space-(?P&lt;space&gt;[A-Za-z0-9]+)_label-CSF_mask\.nii\.gz$"
          "wm_mask": same with label-WM_mask.
        Comment: both naming families are accepted until the producer rerun fixes the layout (A1).
        (b) _parse_input_basename(basename) -&gt; (role, groupdict) or (None, None): first matching role in INPUT_FILE_PATTERNS order.
        (c) discover_subject_inputs(study_config, s3_config, sub_id, task_defs, logger) -&gt; dict. Candidate list: if s3_config.get("enabled"): one paginated list_objects_v2 via _get_s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=listing_prefix) where listing_prefix = os.path.commonprefix([t.split("{session}")[0].replace("{subject}", sub_id) for t in (func_prefix_template, anat_prefix_template)]); candidates are object keys. Else: os.walk(study_config["input_dir"]) and candidates are absolute file paths. For each candidate, parse os.path.basename; unmatched -&gt; logger.warning and skip; sub token != sub_id -&gt; warning, skip; space present and study_config.get("space") set and different -&gt; warning, skip; run-level role with task not in configured task labels -&gt; warning, skip. Session code = "ses" group. Build: {ses: {task_label: {run_int: {"bold", "brainmask", "confounds", "events"}} for every configured task (empty dict when none), "anat": {"csf_mask": None, "wm_mask": None}}}; a session entry is created on the first BOLD or anat-mask match. Runs are defined by BOLD presence: a brainmask/confounds/events file whose (ses, task, run) has no BOLD is logged WARNING and ignored (second pass after all BOLDs are recorded). A second file for an already-filled slot raises OrchestratorError naming both values (A15). Value stored is the S3 key (S3 mode) or the absolute path (local mode). Returns {} when nothing matches. Log one INFO summary per session (n runs per task, anat masks present).
        (d) download_session_inputs(s3_config, input_dir, session_inputs, logger) -&gt; (local_session_inputs, downloaded_paths). session_inputs is one session's dict from (c), already filtered by the caller to the runs and anat roles needed. Deep-copies the structure; every non-None value v becomes local = os.path.join(input_dir, v); os.makedirs(dirname); download via s3_client.download_file(bucket, v, local) unless os.path.isfile(local); downloaded local paths (including pre-existing ones reused) are appended to downloaded_paths for cleanup. ClientError raises OrchestratorError naming the key. Logs one INFO line with the file count.
      </spec>
      <dependencies>C1, C2</dependencies>
      <risk>medium - naming is provisional by design (A1); a wrong regex makes real-data discovery empty, which the C4 gate turns into a loud failure.</risk>
      <rollback>Delete the new section; restore Section C from the pre-build copy.</rollback>
    </change>

    <change id="C4" priority="P0" source_item="C1 (C1-GATE), A7, A10">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Subject-level input-completeness gate.</description>
      <spec>
        check_subject_input_completeness(discovered, analyses, proc_template, logger) -&gt; None. discovered is the C3 structure restricted by the caller to the selected sessions (A10). Let tmpl = {b["name"]: b for b in proc_template.get("analyses", [])}. For each orchestrator analysis a, needs_csf[a.task_label] |= tmpl[a.name].paths.get("CSF_paths") is not None; needs_wm likewise with "WM_paths" (GS needs no file, A7). Collect problems (list of strings): no sessions at all -&gt; "no sessions discovered"; for each session (sorted): if no configured task has any run -&gt; "{ses}: no runs discovered for any configured task"; for each task, run (sorted): brainmask None -&gt; "ses-{ses} task-{task} run-{run}: missing brainmask"; confounds None -&gt; "...: missing confounds"; if the session has at least one run of a task with needs_csf true and anat csf_mask is None -&gt; "ses-{ses}: missing CSF mask (csf_mask) required by CSF_paths"; same for WM ("wm_mask", WM_paths). Events are not checked (C4 carve-out). Presence only; no filesystem access. Non-empty problems -&gt; raise OrchestratorError("Subject input-completeness gate failed for sub-...: " + "; ".join(problems)) listing every problem. Otherwise logger.info a pass line.
      </spec>
      <dependencies>C3</dependencies>
      <risk>low - pure function over dicts.</risk>
      <rollback>Delete the function.</rollback>
    </change>

    <change id="C5" priority="P0" source_item="C3, T13, A18">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Motion from the confounds TSV, finiteness check replacing 999 imputation, frame-count check, derived derivative count.</description>
      <spec>
        (a) extract_motion_regressors(confounds_path, n_remove, calc_n_motion_derivs, out_path, logger, force_recompute=False, expected_n_frames=None) -&gt; (out_path, rotation_unit_ambiguous). Rename the first parameter to confounds_path and update the docstring (source: producer confounds TSV, six base columns by name, trans mm, rot degrees; precomputed derivative/power columns ignored). Keep: empty-file check, missing base column check, rotation-unit check, derivative construction before trimming, NSS trim. Add after reading: if expected_n_frames is not None and len(df) != expected_n_frames -&gt; OrchestratorError("Confounds rows (%d) differ from BOLD frames (%d) ...") without context reason (A9). The all-NaN-column check raises OrchestratorError with context {"reason": "motion_non_finite", "n_affected_frames": len(df) - n_remove, "affected_frame_indices": list(range(len(df) - n_remove))}. Delete the NaN-to-999 block and its comment entirely. After trimming: bad_rows = np.where(~np.all(np.isfinite(motion_data), axis=1))[0]; if any: raise OrchestratorError(message naming the file, count and up to 20 indices, context={"reason": "motion_non_finite", "n_affected_frames": int(len(bad_rows)), "affected_frame_indices": [int(i) for i in bad_rows]}) before any file is written. The log must not contain "999.0" or "Imputing".
        (b) derive_calc_n_motion_derivs(analyses, proc_template) -&gt; int per A18 over the template blocks whose names appear in analyses; returns 0 for an empty analyses list.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - behavior locked by test_motion_non_finite.py.</risk>
      <rollback>Restore the function from the pre-build copy; delete the helper.</rollback>
    </change>

    <change id="C6" priority="P0" source_item="C5 (C5-MASK), T4, A7, A24">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Mask-based tissue and global-signal regressors.</description>
      <spec>
        New Section G2. compute_tissue_mask_regressor(trimmed_bold_path, brainmask_path, tissue_mask_path, out_path, logger, force_recompute=False) -&gt; (out_path, n_voxels).
        Cache: if out_path exists and not force_recompute -&gt; log INFO and return (out_path, None). If force_recompute and exists -&gt; remove.
        stem = out_path without extension; resampled = stem + "_mask_resampled.nii.gz"; intersect = stem + "_mask_intersect.nii.gz".
        If tissue_mask_path == brainmask_path (global-signal case, A7): skip resampling and use brainmask_path as the "a" input. Else run ["3dresample", "-master", trimmed_bold_path, "-rmode", "NN", "-input", tissue_mask_path, "-prefix", resampled]. No erosion, no threshold.
        Run ["3dcalc", "-a", &lt;resampled or brainmask&gt;, "-b", brainmask_path, "-expr", "step(a)*step(b)", "-prefix", intersect].
        n_voxels = int(np.count_nonzero(np.asarray(nibabel.load(intersect).dataobj))) (import nibabel at module top; nibabel 5.4.2 is in the environment).
        n_voxels == 0 -&gt; remove intermediates; raise OrchestratorError(message, context={"reason": "tissue_mask_empty", "tissue_mask": tissue_mask_path, "n_voxels": 0}); no out_path written.
        Run ["3dmaskave", "-quiet", "-mask", intersect, trimmed_bold_path]; values = whitespace-split stdout parsed as floats; empty -&gt; OrchestratorError. np.savetxt(out_path, values, fmt="%.10g"). Delete resampled/intersect (best effort). Any CalledProcessError -&gt; OrchestratorError with stderr. Log INFO with voxel count and row count. Return (out_path, n_voxels).
      </spec>
      <dependencies>C1</dependencies>
      <risk>medium - real-AFNI orientation handling; covered by the real-AFNI acceptance test in test_tissue_mask_regressors.py.</risk>
      <rollback>Delete the function and the nibabel import.</rollback>
    </change>

    <change id="C7" priority="P0" source_item="C6, C8e-NBACK, T7; test punch list items 10, 11">
      <file path="orchestrator_utils.py" action="modify" />
      <description>n-back relabel, drop-versus-clip timing, events validation.</description>
      <spec>
        (a) fix_nback_cue_labels: the level == "0" branch writes the literal "target_0back"; any other level writes "instruction". Track n_target and n_instruction; INFO log "Relabeled %d cue trial(s) (%d as 'target_0back', %d as 'instruction')". Docstring: 0-back pre-block event is the target image presentation (pooled label target_0back, C8e-NBACK); 2-back pre-block event is the "2-back" instruction screen.
        (b) format_task_timing(...) -&gt; (out_path, n_dropped, n_clipped, clipped_seconds). Cache hit returns (out_path, 0, 0, 0.0) (A17). After building timing_df and subtracting offset = n_remove * TR: end = onset + duration; drop_mask = end &lt;= 0; clip_mask = (onset &lt; 0) &amp; (end &gt; 0); n_dropped = sum(drop_mask); n_clipped = sum(clip_mask); clipped_seconds = float(-onset[clip_mask].sum()); for clipped rows set duration = end and onset = 0.0; remove dropped rows; sort by onset; write. WARNING log for drops and for clips (counts and seconds). Return values as Python int/int/float.
        (c) validate_task_timing(timing_path, n_frames, TR, logger) -&gt; {"conditions_present": set(str), "n_events_written": int}. Single read (sep="\t"). onset/duration coerced with pd.to_numeric(errors="coerce"); any non-finite -&gt; OrchestratorError(context={"reason": "events_invalid_values", "n_rows": k}); any onset &lt; 0 or onset &gt;= n_frames * TR -&gt; OrchestratorError(context={"reason": "events_onset_out_of_range", "n_rows": k, "retained_seconds": n_frames * TR}). Return the distinct trial_type set and row count.
        (d) check_required_conditions_present(required_labels, conditions_present_by_run) -&gt; sorted(set(required_labels) - union(conditions_present_by_run)).
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - contracts fixed by tests.</risk>
      <rollback>Restore Section H from the pre-build copy; delete (c) and (d).</rollback>
    </change>

    <change id="C8" priority="P0" source_item="C7, T9, T10, T12, C8b, C8d, C9(2), A11, A13; test punch list items 8, 9">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Preproc QC on the trimmed BOLD without registration and with producer FD; peak memory; terminal classification; v4 session QC JSON with flags; AFNI warning policy helpers.</description>
      <spec>
        (a) compute_preproc_qc(run_info, confounds_path, bold_path, mask_path, n_remove, qc_config, out_dir, sub_id, space, logger): remove anat_mask_path and the whole registration block (no "registration" key). Docstring: bold_path is the NSS-trimmed BOLD (A13); n_remove is the number of frames already removed from it and is used only to trim the confounds-derived series (DVARS, producer FD, carpet-plot DVARS trace) so every QC series covers the retained frames. Add: fd = confounds_df["framewise_displacement"] when the column exists, trimmed by n_remove, coerced numeric, NaN ignored; producer_fd_mean/producer_fd_max = float mean/max or None when no valid values or column absent; producer_fd_source = "producer_confounds_framewise_displacement_unfiltered_50mm" (informational, T12). generate_carpet_plot keeps its signature; its docstring states bold_path is the trimmed BOLD and n_remove trims the DVARS trace to match.
        (b) get_peak_memory_usage(logger) -&gt; (peak_bytes, source). If os.path.isfile("/sys/fs/cgroup/memory.peak"): read and int(strip()) -&gt; (value, "cgroup_v2"); ValueError/OSError -&gt; fall through. Fallback: import resource; r = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss; bytes = r if sys.platform == "darwin" else r * 1024; return (int(bytes) if bytes &gt; 0 else None, "rusage_largest_child"); any exception -&gt; (None, "rusage_largest_child"). Add "import sys" and "import logging" as needed.
        (c) is_outcome_terminal(status, reason=None, error_type=None) -&gt; bool: success -&gt; True; skipped with reason in {"no_usable_runs", "events_missing_conditions"} -&gt; True; failed with error_type == "ModelError" -&gt; True; else False.
        (d) consolidate_session_qc(sub_id, ses_label, session_status, session_wall_time, preproc_qc_by_run, analysis_outcomes, preproc_excluded_runs, out_path, logger). Keep provenance and existing session fields. Add top-level "preproc_excluded_runs" (verbatim list). Per-analysis entry adds "reason" (outcome.get("reason")), "run_labels" (outcome.get("run_labels", [])), "terminal" (bool(outcome.get("terminal", False))). Session adds: n_analyses_skipped (status == "skipped"), all_terminal (len(outcomes) &gt; 0 and all terminal), peak_memory_bytes/peak_memory_source from get_peak_memory_usage(logger), flags, any_flag. flags is a list of {code, detail}, in this order when triggered: preproc_excluded_runs (non-empty list; detail "N run(s) excluded: label (reason), ..."), analysis_skipped (any skipped; detail names and reasons), analysis_failed (any failed; detail names and error_type), upstream_warning (any qc_flagged; detail names), events_clipped_at_start (any preprocessing entry with n_events_clipped_at_start &gt; 0; detail run labels). any_flag = len(flags) &gt; 0. Flags never alter status or all_terminal. No tissue-mask-size flag. Preprocessing dicts are passed through verbatim.
        (e) Add class AfniUnvalidatedVersionFilter(logging.Filter): filter(record) returns False when record.getMessage().startswith("[UNVALIDATED_VERSION] AFNI "), else True. Add attach_afni_version_filter(logger): adds one instance unless an instance is already attached. Add filter_afni_unvalidated_warnings(warnings) -&gt; list excluding dicts with code == "UNVALIDATED_VERSION" and str(message).startswith("AFNI ") (Python/numpy/pandas drift kept).
        (f) Exit categories (A11): module constants EXIT_COMPLETE = 0, EXIT_FAILED_UNEXPLAINED = 1, EXIT_FAILED_KNOWN_CAUSE = 2. classify_session_exit(session_error, analysis_outcomes, preproc_excluded_runs, skip_first_level) -&gt; int: session_error truthy -&gt; 1; any exclusion with reason "unexpected_error" -&gt; 1; when not skip_first_level: any outcome with terminal False -&gt; 1, then any outcome with status "failed" and error_type "ModelError" -&gt; 2; otherwise 0. most_severe_exit(codes) -&gt; 1 if 1 in codes, else 2 if 2 in codes, else 0. Docstrings state the category meanings verbatim from A11.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - schema fixed by test_qc_schema_v4.py; exit helpers are pure functions.</risk>
      <rollback>Restore Sections K and K3 from the pre-build copy; delete the new helpers.</rollback>
    </change>

    <change id="C9" priority="P0" source_item="T5, T6, T8, C1(5), C5, C7, C8e-O1; test punch list items 1, 13">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Orchestrator config and proc-template validation for the v4 contract.</description>
      <spec>
        (a) load_orchestrator_config: before the required-key check, if "fmriprep_dir" in study raise OrchestratorError("study.fmriprep_dir is retired; use study.input_dir (absolute path) ..."). Required study keys: input_dir, output_dir, space, TR. input_dir and output_dir must be str and os.path.isabs, else OrchestratorError naming the key ("must be an absolute path"); never normalized. Retired keys raise OrchestratorError naming the key and its replacement: study.calc_n_motion_derivs (derived from the proc template); top-level "smoothing" (use per-block smoothing {enabled, fwhm} in the proc config); qc.preproc.registration_quality (registration QC removed); analyses[*].fd_threshold and analyses[*].censor_prev_tr (set in the proc config); s3.fmriprep_s3_prefix, s3.mmps_mproc_s3_prefix, s3.available_sessions (replaced by s3.func_prefix_template/anat_prefix_template); these s3 checks apply whether or not s3.enabled. Delete the fd_threshold/censor_prev_tr validation, the smoothing validator, the available_sessions validator, the calc_n_motion_derivs default/validator, and the smoothing/available_sessions defaults. When s3.enabled: required non-empty bucket, func_prefix_template, anat_prefix_template, upload_prefix; bucket must not start with "s3://"; templates and upload_prefix must not start or end with "/"; both templates must contain "{subject}" and "{session}" (A23). Keep concatenate_runs rejection, deprecated-field warnings, force_recompute validation, qc/s3 defaults, cleanup_after_upload default, upload_max_workers default 8 and range check. Docstring updated.
        (b) validate_proc_template(orchestrator_config, proc_template, logger): keep existing checks in order. Add: global.template_path non-None and not os.path.isabs -&gt; OrchestratorError naming global.template_path (never rewritten). In the per-block loop, for blocks with type in ("task_act", "task_conn"): a paths dict containing "CSF_path" or "WM_path" with a non-None value -&gt; OrchestratorError naming the key and stating that task blocks accept only the Mode A plural CSF_paths/WM_paths. After the per-analysis loop: "tr" absent from global -&gt; OrchestratorError("global.tr is required in the proc template ..."); study TR = orchestrator_config.get("study", {}).get("TR"); when both present and abs difference &gt; 1e-6 -&gt; OrchestratorError("global.tr ... does not match study.TR ..."). Update the global-block error message (tr is required, never injected). Docstring lists the new checks.
      </spec>
      <dependencies>C1</dependencies>
      <risk>medium - tightens startup validation; covered by test_preprocessing.py, test_absolute_paths.py and test_coverage_gaps(_v2).py.</risk>
      <rollback>Restore Section M from the pre-build copy.</rollback>
    </change>

    <change id="C10" priority="P0" source_item="T2, T4, T8, C5, A7; test punch list item 2">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Rendered first-level config under proc-config authority with explicit run_labels and conditional tissue paths.</description>
      <spec>
        build_first_level_config(sub_id, session, study_config, task_defs, processed_files, analyses, proc_template, logger): global.tr missing -&gt; OrchestratorError("global.tr is required ..."); mismatch -&gt; existing "does not match" error; never injected. Delete the fd_threshold read and the fd_threshold/censor_prev_tr/remove_previous assignments. Per analysis: block["run_labels"] = [int(r) for r in pf["run_labels"]] (top-level, sibling of paths; no orchestrator duplicate/length checks). tmpl_paths = block.get("paths") or {}. Task types: paths = {"scan_paths": list(pf["bolds"]), "motion_paths": list(pf["motions"]), "task_timing_paths": list(pf["timings"])}. Rest: paths = {"scan_paths": list(pf["bolds"]), "motion_paths": list(pf["motions"])}. For key, pf_key in (("CSF_paths", "csf"), ("WM_paths", "wm"), ("GS_paths", "gs")): GS applies to rest_conn only (task blocks have no GS key in the v4 schema); when tmpl_paths.get(key) is not None: pf.get(pf_key) None -&gt; OrchestratorError("[name] template requests key but no per-run regressors were produced"); else paths[key] = list(pf[pf_key]). Keys whose template value is null or absent are omitted. block["paths"] = paths. Keep out_dir, out_file_pre, extraction and connectivity prefix overrides and the unreferenced-block removal. Docstring: processed_files[task] keys run_labels, bolds, motions, timings (task), csf, wm, gs (each list or None); remove_previous, fd_threshold, censor_prev_tr and every other processing option come from the proc template.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - fixed by TestBuildFirstLevelConfigV4 and the golden-file test.</risk>
      <rollback>Restore the function from the pre-build copy.</rollback>
    </change>

    <change id="C11" priority="P0" source_item="T11, C9(3); test punch list item 7">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Explicit upload set, sentinel control and remote sweep.</description>
      <spec>
        (a) enumerate_upload_targets(session_out_dir, sub_id, session, first_level_file_paths): replace the first_level_out/ walk with the explicit list (A25: skip with WARNING any path not a file or whose realpath is not under realpath(session_out_dir)); keep the qc/ recursive walk and the preproc/ single-level non-.nii.gz walk; delete the concat/ branch; add the session-root file "sub-{sub_id}_ses-{session}A_first_level_config.yaml" when it exists. De-duplicate by s3_key_suffix; sort by s3_key_suffix. The function no longer takes a logger; skipped paths are collected and logged by the caller-facing upload function via the returned list only (log through a module logger obtained with logging.getLogger(__name__)).
        (b) upload_session_to_s3(s3_config, sub_id, session, session_out_dir, logger, first_level_file_paths=None, write_sentinel=True): call enumerate_upload_targets(session_out_dir, sub_id, session, first_level_file_paths or []). Keep steps 1 to 6 (empty enumeration, stale-sentinel abort, gap, parallel upload, verification). New step 7 (sweep, runs regardless of write_sentinel): stale = sorted(k for k in final listing if k != sentinel_key and k not in expected); delete each with s3_client.delete_object(Bucket=bucket, Key=k); failures raise OrchestratorError; INFO log of the count. Step 8: when write_sentinel put the zero-byte sentinel (after the sweep); otherwise INFO "Sentinel not written (session exit category 1, failed unexplained)". Return {"n_files_uploaded", "n_files_total", "verified_keys", "sentinel_key": sentinel_key if written else None, "sentinel_written": bool, "swept_keys": stale}.
        (c) determine_session_routing docstring and return values per C2.
      </spec>
      <dependencies>C1, C2</dependencies>
      <risk>medium - the sweep deletes remote objects; it is confined to the session prefix and excludes expected keys and the sentinel (test_restart_safety.py).</risk>
      <rollback>Restore Section A upload functions from the pre-build copy.</rollback>
    </change>

    <change id="C12" priority="P0" source_item="T1, T2, T6, C1, C3-C7, C9, C8b, C8d, T7, T9-T11, T13, A4, A5, A7, A9-A14, A19-A22; test punch list item 12">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Entry point rewritten for discovery, gate, run-level exclusion, conditional regressors, three-way exit classification, explicit upload and AFNI policy; chdir removed.</description>
      <spec>
        Header: describe the producer per-file inputs (no fMRIPrep tarball, no mmps_mproc, no masking/smoothing) and the exit codes 0 complete, 2 failed with known cause, 1 failed unexplained; "Version: 4.0.0"; "Last updated: 10/16/26". __version__ = "4.0.0".
        Imports from orchestrator_utils: OrchestratorError, VALID_TASK_LABELS, discover_subject_inputs, check_subject_input_completeness, download_session_inputs, determine_session_routing, upload_session_to_s3, cleanup_local_inputs, verify_upstream_version, MIN_UPSTREAM_VERSION, json_safe_context, build_first_level_qc_from_result, load_orchestrator_config, validate_proc_template, detect_non_steady_state_trs, remove_initial_trs_bold, extract_motion_regressors, derive_calc_n_motion_derivs, compute_tissue_mask_regressor, fix_nback_cue_labels, format_task_timing, validate_task_timing, check_required_conditions_present, compute_preproc_qc, consolidate_session_qc, is_outcome_terminal, classify_session_exit, most_severe_exit, EXIT_COMPLETE, EXIT_FAILED_UNEXPLAINED, EXIT_FAILED_KNOWN_CAUSE, build_first_level_config, write_temp_config, compute_first_level_qc, attach_afni_version_filter, filter_afni_unvalidated_warnings, upstream_names. Remove every retired import. Keep the guarded upstream exceptions/runtime import block; add runtime_check and load_lockfile to it.
        _derive_session_status(routing, analysis_outcomes): "skip" -&gt; "skipped"; otherwise per A20. Delete the migrate branch and docstring lines.
        process_participant(config, sub_id, proc_template, skip_qc, skip_first_level, dry_run, logger, session_filter=None) -&gt; int exit code:
          1. Import-error guard unchanged.
          2. discovered = discover_subject_inputs(config["study"], s3_cfg, sub_id, task_defs, logger); sessions = sorted(discovered). Session filter: not in sessions -&gt; OrchestratorError("Session filter '...' not found in available sessions ..."); else sessions = [filter].
          3. check_subject_input_completeness({s: discovered[s] for s in sessions}, analyses, proc_template, logger) (A10).
          4. Dry run: log "[DRY RUN]" lines for sessions, per-session runs per task, and analyses as "Analysis: name (type: t, task: k)" (no fd_threshold); return EXIT_COMPLETE.
          5. For each session: _process_session(..., discovered_session=discovered[session]); store status, routing, analyses, qc_path, remote_metadata, preproc_excluded_runs and exit_category (from the returned dict). Exceptions are recorded as today plus "exit_category": EXIT_FAILED_UNEXPLAINED and "preproc_excluded_runs": [].
          6. Summary: drop migrate counters and banner; add "[SKIPPED] name: reason" lines; keep the high-severity warning count line; each session banner line appends its exit category ("complete", "failed, known cause", "failed, unexplained"); totals line adds counts per exit category.
          7. code = most_severe_exit([r["exit_category"] for r in results]); log INFO "Subject exit code %d (%s)"; return code. Delete the "All N session(s) failed" raise (replaced by A11).
        _process_session(config, sub_id, session, proc_template, skip_qc, skip_first_level, logger, discovered_session=None):
          1. When discovered_session is None: discovered = discover_subject_inputs(...); session absent -&gt; OrchestratorError("Session ... not found ..."); check_subject_input_completeness({session: discovered[session]}, ...); discovered_session = discovered[session].
          2. Routing (S3 only) via determine_session_routing; "skip" returns early with exit_category EXIT_COMPLETE. Delete the migrate block.
          3. FULL path: in S3 mode remove a pre-existing local session_out (A12); create session_out, preproc/, qc/, qc/preproc/, first_level_out/.
          4. preproc_excluded_runs = []. For each configured non-rest task and run with events None: append {"run_label", "task", "reason": "events_missing", "error_type": None, "message": "no events file in discovery listing"}, log WARNING, drop the run. Anat roles needed: csf_mask when any analysis on a task with runs in this session has a non-null template CSF_paths; wm_mask likewise.
          5. S3 mode: local_inputs, downloaded_paths = download_session_inputs(s3_cfg, study["input_dir"], filtered session (runs plus needed anat roles), logger). Local mode: local_inputs = filtered session, downloaded_paths = [].
          6. n_derivs = derive_calc_n_motion_derivs(analyses, proc_template); per task: csf/wm/gs flags = any analysis on that task whose template CSF_paths/WM_paths/GS_paths is non-null.
          7. Per task in configured order (VALID_TASK_LABELS check unchanged), per run in ascending run int: run_label = f"{ses_label}_task-{task}_run-{run}"; file_prefix = f"sub-{sub_id}_{ses_label}_task-{task}_run-{run:02d}" (A14). Inside try, in this order: n_remove = detect_non_steady_state_trs(confounds); trimmed_bold, n_trs = remove_initial_trs_bold(bold, n_remove, preproc_dir, file_prefix, logger, force_recompute=...); n_trs &lt;= 0 -&gt; OrchestratorError (A9); preproc QC when enabled: compute_preproc_qc(run_info, confounds, trimmed_bold, brainmask, n_remove, ...) on the trimmed BOLD (A13), stored via preproc_qc_by_run[run_label] update; motion via extract_motion_regressors(confounds, n_remove, n_derivs, preproc/{file_prefix}_motion.1D, logger, force_recompute=..., expected_n_frames=n_trs + n_remove); rotation_unit_ambiguous recorded via setdefault as today; csf/wm via compute_tissue_mask_regressor(trimmed_bold, brainmask, anat csf/wm mask, preproc/{file_prefix}_csf.1D or _wm.1D) when the task needs them, QC csf_mask_voxels/wm_mask_voxels when the returned count is not None; gs via compute_tissue_mask_regressor(trimmed_bold, brainmask, brainmask, preproc/{file_prefix}_gs.1D) when needed, QC gs_mask_voxels; task runs: optional fix_nback_cue_labels to preproc/{file_prefix}_events_fixed.tsv, then format_task_timing(..., n_remove, TR, preproc/{file_prefix}_timing.tsv, ...) -&gt; 4-tuple, then validate_task_timing(timing, n_trs, TR, logger); QC fields n_events_dropped_before_start, n_events_clipped_at_start, clipped_seconds, n_events_written, conditions_present (sorted list) set via preproc_qc_by_run.setdefault(run_label, {}). On success append one run record {"run": run, "bold": trimmed_bold, "motion": ..., "timing": ... or None, "csf": ..., "wm": ..., "gs": ..., "conditions": set or None}. Except OrchestratorError as e: reason = (e.context or {}).get("reason", "unexpected_error"); append {"run_label", "task", "reason", "error_type": "OrchestratorError", "message": str(e)}; WARNING log. Except Exception as e: append reason "unexpected_error", error_type type(e).__name__, message str(e); logger.warning(..., exc_info=True). Keep the existing per-task summary warning text ("failed preprocessing", "surviving", "Downstream analyses will use only surviving run(s)").
          8. processed_files[task] = {"run_labels": [r["run"] ...], "bolds", "motions", "timings" (task only), "csf", "wm", "gs" (each list when the task needs it, else None), "conditions_by_run": [r["conditions"] ...] (task only)}, all derived from the same survivor record list (T2); tasks with zero survivors get no entry. INFO "Step 11: Collecting per-run files for task '%s' (%d run(s))." retained. Delete the smoothing block and the "No tasks produced usable runs" raise (C4: such analyses become skipped outcomes).
          9. Analyses (not skip_first_level): attach_afni_version_filter(logger). For each orchestrator analysis in config order: pf = processed_files.get(task); no pf -&gt; skipped outcome reason "no_usable_runs", run_labels []; task types: required = template cond_labels (task_act) or cond_beta_labels (task_conn) or []; missing = check_required_conditions_present(required, pf["conditions_by_run"]); non-empty -&gt; skipped outcome reason "events_missing_conditions", error_context {"missing_conditions": missing}. Skipped outcome dict: name, type, status "skipped", reason, upstream_status None, qc_flagged False, error None, error_type None, error_context, warnings [], wall_time_seconds 0.0, run_labels, terminal True; not dispatched; INFO log. Remaining analyses are active: when non-empty, build_first_level_config(..., active, ...), write_temp_config, configs = load_and_validate(path, logger) (A21). No os.chdir anywhere; delete the chdir wrapper, its comment and the try/finally. Dispatch loop as today, plus: warnings = filter_afni_unvalidated_warnings(list(result.warnings)) when result exists; outcome adds reason None, run_labels = processed_files[task]["run_labels"], terminal = is_outcome_terminal(status, None, error_type). First-level file list: success -&gt; result.output_files plus os.path.join(cfg.out_dir, upstream_names.ledger(cfg.out_file_pre)); raised -&gt; os.path.join(cfg.out_dir, upstream_names.timing_errors(pre)) and upstream_names.qc_summary(pre); missing files are dropped by enumerate_upload_targets (A25). The failure-path compute_first_level_qc uses cfg.out_file_pre and the first_level_out directory as today.
          10. exit_category = classify_session_exit(None, outcomes, preproc_excluded_runs, skip_first_level); all_terminal = len(outcomes) &gt; 0 and all(o["terminal"]). Session QC JSON when not skip_qc and (preproc_qc_by_run or outcomes or preproc_excluded_runs): consolidate_session_qc(sub_id, ses_label, _derive_session_status("full", outcomes), wall, preproc_qc_by_run, outcomes, preproc_excluded_runs, qc_json_path, logger). INFO log of the exit category.
          11. S3 mode: upload_session_to_s3(s3_cfg, sub_id, session, session_out, logger, first_level_file_paths=fl_paths, write_sentinel=(not skip_first_level and exit_category != EXIT_FAILED_UNEXPLAINED)); log sentinel_written and swept count; cleanup_after_upload: cleanup_local_inputs(downloaded_paths) and rmtree(session_out). Error path: delete the extracted_dir cleanup; keep downloaded-input cleanup; re-raise.
          12. Return {"routing": "full", "analysis_outcomes", "qc_path", "remote_metadata", "all_terminal", "preproc_excluded_runs", "exit_category"}.
        main(): unchanged arguments. After logger setup: attach_afni_version_filter(logger). verify_upstream_version(logger) (min and max). Import-error guard. Not dry run: preflight_tool_environments(logger=logger); then AFNI provenance: versions = runtime_check(logger); lock = load_lockfile(); one INFO line "AFNI provenance: installed %s, validated %s, minimum %s; '[UNVALIDATED_VERSION] AFNI' warnings are suppressed by orchestrator policy (AFNI &gt;= minimum accepted)" from versions["afni_version"], lock["tools"]["afni"]["validated"], lock["tools"]["afni"]["minimum"]. ToolVersionError from runtime_check -&gt; OrchestratorError. Then config load, proc template load, validate_proc_template, code = process_participant(...). OrchestratorError -&gt; log "Fatal" and sys.exit(1) as today. Log total runtime, then sys.exit(code) when code != 0.
      </spec>
      <dependencies>C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11</dependencies>
      <risk>high - the session pipeline is rewritten end to end; covered by test_simulated_pipeline.py, test_run_labels.py, test_events_validation.py, test_cr_implementation(_extended).py and test_absolute_paths.py. The three-way exit classification has no dedicated tests yet; the post-build /test pass must add them.</risk>
      <rollback>Restore orchestrate_first_level.py from the pre-build copy.</rollback>
    </change>

    <change id="C13" priority="P1" source_item="T1, A3">
      <file path="environment.yaml" action="modify" />
      <description>Exact interpreter and numeric-library pins.</description>
      <spec>Replace "python&gt;=3.8", "numpy", "pandas" with "python=3.14", "numpy=2.4.2", "pandas=3.0.1". Add "nibabel" (now imported by orchestrator_utils for the tissue voxel count). Leave every other line, including the pip git URL line and its comment, unchanged (A3).</spec>
      <dependencies>none</dependencies>
      <risk>low - environment.yaml is not applied by this build; no environment is modified.</risk>
      <rollback>Restore the three original dependency lines and remove the nibabel line.</rollback>
    </change>

    <change id="C14" priority="P0" source_item="C8e (C8e-O1, C8e-P1, AUDIT-P2), C8e-NBACK, C8e-REST, C8e-ROI, C8e-FD, T12(1)">
      <file path="orch_config_production.yaml" action="create" />
      <file path="proc_config_production.yaml" action="create" />
      <file path=".gitignore" action="modify" />
      <description>Local-only <bucket> production configs and their git-ignore entries; orch_config_final.yaml and proc_config_final.yaml untouched.</description>
      <spec>
        orch_config_production.yaml (built from orch_config_final.yaml with v4 comments; comments describe the producer per-file layout, that input_dir is the local mirror used in both modes, and that the S3 templates are placeholders filled after the producer rerun):
          study: input_dir "/scratch/fmriprep_in/"; output_dir "/scratch/first-level-proc-out/"; space "MNI152NLin2009cAsym"; TR 0.8; force_recompute false.
          tasks: [{task_label "nback", condition_column "trial_type", conditions_exclude ["dummy"], fix_nback_cues true}, {task_label "rest"}].
          qc: preproc {enabled true, tsnr true, carpet_plots true}; first_level {enabled true}.
          analyses: nback_act (task_act, nback, post_id_out_pre "nback", post_id_extract_pre "nback_SchaeferTian"); nback_conn (task_conn, nback, post_id_out_pre "nback", post_id_extract_pre "nback_SchaeferTian", post_id_conn_pre "nback_SchaeferTian"); rest_conn (rest_conn, rest, post_id_out_pre "rest", post_id_extract_pre "rest_SchaeferTian", post_id_conn_pre "rest_SchaeferTian"). No fd_threshold or censor_prev_tr keys.
          s3: enabled true; bucket "__PRODUCER_BUCKET__"; func_prefix_template "derivatives/sub-{subject}/ses-{session}/func"; anat_prefix_template "derivatives/sub-{subject}/ses-{session}/anat"; upload_prefix "derivatives/first_levels"; cleanup_after_upload true; upload_max_workers 1.
          Absent: calc_n_motion_derivs, concatenate_runs, smoothing, registration_quality, fmriprep_s3_prefix, mmps_mproc_s3_prefix, available_sessions.
        proc_config_production.yaml (built from proc_config_final.yaml, restricted to keys in the upstream v4 closed schema, fmri_first_level_proc/first_level_config.py lines 87-144 of the v4.0.0-build workspace):
          global: tr 0.8; num_cores 1; force_diff_atlas true; template_path "/app/ABCD_fmri_orchestrator_S3/Schaefer2018_400Parcels_7Networks_order_Tian_Subcortex_S4_3T_MNI152NLin2009cAsym_2mm.nii.gz"; no derivatives_dirs key (comment: its presence would trigger upstream discovery of untrimmed producer files).
          Every block: average_type mean; scale_to_psc true; smoothing {enabled: false}; remove_previous true; roi_zero_handling exclude; orchestrator-filled placeholders run_labels [1], out_dir "/__ORCHESTRATOR_FILLED__", out_file_pre "__ORCHESTRATOR_FILLED__", extraction.extract_out_file_pre "__ORCHESTRATOR_FILLED__", connectivity.conn_out_file_pre "__ORCHESTRATOR_FILLED__" (conn blocks), per-run path lists ["/__ORCHESTRATOR_FILLED__"]. fd_threshold comment: Power framewise displacement (50 mm sphere) computed by fmri-first-level-proc from the motion parameters (notch-filtered for rest). No comment claims remove_previous is forced.
          nback_act (task_act): fd_threshold 0.9; censor_prev_tr false; hrf_model GAM; custom_hrf null; include_motion_derivs false; use_tissue_derivs false; paths {scan_paths, task_timing_paths, motion_paths placeholders; CSF_paths null; WM_paths null}; cond_labels [0_back_negface, 0_back_posface, 0_back_neutface, 0_back_place, 2_back_negface, 2_back_posface, 2_back_neutface, 2_back_place, target_0back, instruction]; contrasts {functions, labels} = the 29 rows below in order; extraction {stat_buckets [Coef, Tstat, Zstat], extract_resids true, extract_raw_ptseries true}.
          nback_conn (task_conn): fd_threshold 0.9; censor_prev_tr false; hrf_model GAM; custom_hrf null; include_motion_derivs false; use_tissue_derivs false; paths as nback_act (CSF_paths null, WM_paths null); cond_beta_labels [0_back_negface, 0_back_posface, 0_back_neutface, 0_back_place, 2_back_negface, 2_back_posface, 2_back_neutface, 2_back_place] (comment: target_0back and instruction are modeled upstream as non-beta nuisance regressors); extraction {extract_pbseries true, extract_raw_ptseries true}; connectivity {calc_conn parcellated, pcorr false, fishZ true, contrasts = the same 29 rows in the same order}.
          rest_conn: fd_threshold 0.4; censor_prev_tr true; paths {scan_paths, motion_paths, CSF_paths, WM_paths placeholders; GS_paths null (no GSR)}; bandpass [0.01, 0.1]; notch_filter_band [0.31, 0.43]; motion_deriv_degree 2; use_tissue_derivs true; keep_run_res_dtseries true; use_sequenced_bandpass false; extraction {extract_ptseries true, extract_raw_ptseries true}; connectivity {calc_conn parcellated, pcorr false, fishZ true}.
          Contrast rows (label: function), identical in both blocks, with a comment that outputs are addressed by label, never by sub-brick index:
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
        .gitignore: append "/orch_config_production.yaml" and "/proc_config_production.yaml" under the local-only section.
      </spec>
      <dependencies>C9 (validators the configs must satisfy)</dependencies>
      <risk>low - new local-only files; acceptance by test_production_configs.py once its two test defects are fixed (A6).</risk>
      <rollback>Delete the two new files; remove the two .gitignore lines.</rollback>
    </change>

  </changes>

  <execution_order>C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14</execution_order>

  <build_notes>
    orchestrator_utils.py changes C1-C11 touch one file and run sequentially in a single dispatch group; C12 (orchestrate_first_level.py) runs after them; C13 and C14 touch separate files and may run in parallel with C12. Tests are not run by this build (/implement constraint). For the post-build /test pass, the build report records: the conda env must carry the fmri-first-level-proc v4.0.0-build workspace (A2; separate approval-gated environment change); the two test_production_configs.py defects (A6); new tests needed for the A11 exit classification (classify_session_exit, most_severe_exit, sentinel suppression on unexpected_error exclusions, process_participant return codes) and for A13 (preproc QC computed on the trimmed BOLD). User-facing documentation is /document scope (T14), including the exit-code contract.
  </build_notes>
</implement_plan>
