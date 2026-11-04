<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build" timestamp="2026-10-16T16:09:34Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20261016_154424.md</spec_ref>

  <changes_applied>

    <change id="C1" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        OrchestratorError now carries an optional context dict via __init__(self, message="", context=None). MIN_UPSTREAM_VERSION raised to "4.0.0"; MAX_UPSTREAM_VERSION "5.0.0" added with the >= rejection. verify_upstream_version accepts both min_version and max_version parameters. PRIMARY_OUTPUT_SUFFIX is now derived from upstream_names.frozen_primary (hard import of fmri_first_level_proc.names). _primary_output_ok uses upstream_names.frozen_primary for path construction. The upstream names module import is unguarded per the approved hard-dependency decision.
      </notes>
    </change>

    <change id="C2" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        Deleted: LegacyArchiveCorruptError, migrate_session_from_archive, discover_available_sessions, download_session_data, discover_local_mmps_files, extract_session_archive, discover_session_files, apply_brain_mask (Section E), extract_tissue_signals, apply_smoothing (Section J), compute_registration_quality. determine_session_routing now routes "skip" or "full" only (one head_object call; no tarball probe).
      </notes>
    </change>

    <change id="C3" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        New Section C implements discover_subject_inputs (single paginated listing in S3 mode, os.walk in local mode, both using INPUT_FILE_PATTERNS). Both naming families accepted per the approved dual-regex decision. download_session_inputs mirrors S3 keys to input_dir. Module constants _SES, _RUN_ENTITIES and INPUT_FILE_PATTERNS provide a single edit point for the post-rerun naming correction.
      </notes>
    </change>

    <change id="C4" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        check_subject_input_completeness aggregates all missing items (brainmask, confounds, CSF/WM masks) and raises one OrchestratorError listing every problem. Events are not checked (downstream run-level exclusion). Tissue masks are required only when the proc template's corresponding key is non-null.
      </notes>
    </change>

    <change id="C5" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        extract_motion_regressors now reads from the confounds TSV (parameter renamed from motion_tsv_path). The NaN-to-999 imputation block is deleted; a post-trim finiteness check raises OrchestratorError with context reason "motion_non_finite" and affected frame indices. The expected_n_frames parameter validates confounds row count against BOLD frames. derive_calc_n_motion_derivs computes the derivative count from template blocks.
      </notes>
    </change>

    <change id="C6" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        New compute_tissue_mask_regressor implements the 3dresample/3dcalc/3dmaskave pipeline for CSF/WM masks and the brainmask-as-mask global-signal case. Voxel count is read via nibabel (no 3dBrickStat round-trip). Zero voxels raises OrchestratorError with context reason "tissue_mask_empty". Intermediate files are cleaned up in a finally block.
      </notes>
    </change>

    <change id="C7" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        fix_nback_cue_labels: 0-back pre-block events are now labeled "target_0back" (not the bare stimulus name); 2-back events remain "instruction". format_task_timing returns a 4-tuple (path, n_dropped, n_clipped, clipped_seconds) with drop-only-if-fully-elapsed and clip-if-straddling semantics. validate_task_timing checks finiteness and onset range; check_required_conditions_present computes the union over surviving runs.
      </notes>
    </change>

    <change id="C8" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        compute_preproc_qc: anat_mask_path parameter and registration block removed; producer_fd_mean/producer_fd_max added from the confounds framewise_displacement column. QC is computed on the trimmed BOLD. get_peak_memory_usage reads cgroup v2 or rusage fallback. is_outcome_terminal classifies success, documented skips and ModelError as terminal. classify_session_exit and most_severe_exit implement the three-way exit code scheme. consolidate_session_qc gains preproc_excluded_runs, all_terminal, n_analyses_skipped, peak_memory, per-analysis run_labels/terminal, and the five session flags. AfniUnvalidatedVersionFilter and filter_afni_unvalidated_warnings suppress AFNI-specific version warnings.
      </notes>
    </change>

    <change id="C9" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        load_orchestrator_config: required key is now input_dir (fmriprep_dir raises naming it); calc_n_motion_derivs, smoothing, registration_quality, fd_threshold, censor_prev_tr and the three legacy S3 keys are retired with OrchestratorError; input_dir and output_dir must be absolute; S3 requires func_prefix_template/anat_prefix_template with {subject}/{session} placeholders. validate_proc_template: global.tr required (never injected); global.template_path non-null must be absolute; singular CSF_path/WM_path in task blocks rejected; global.tr consistency check added.
      </notes>
    </change>

    <change id="C10" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        build_first_level_config: global.tr is required (OrchestratorError if missing), never injected; remove_previous, fd_threshold and censor_prev_tr are not overridden (proc-config authority); run_labels injected as a top-level key from processed_files; tissue/GS paths are filled only when the template key is non-null, otherwise omitted.
      </notes>
    </change>

    <change id="C11" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="3319" />
      </files_modified>
      <notes>
        enumerate_upload_targets takes first_level_file_paths (explicit list replacing the blind walk) plus the session config YAML; the concat/ branch is deleted. upload_session_to_s3 gains write_sentinel and first_level_file_paths parameters; a new sweep step deletes stale remote keys under the session prefix; sentinel is written only when write_sentinel is True. Return dict includes sentinel_written and swept_keys.
      </notes>
    </change>

    <change id="C12" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="1159" />
      </files_modified>
      <notes>
        __version__ set to "4.0.0". All retired imports removed; new imports added for the v4 API (discovery, gate, tissue regressors, events validation, exit classification, AFNI filter, upstream names). os.chdir removed entirely. process_participant returns an exit code (0/1/2). _derive_session_status no longer has a "migrate" branch. Session processing uses discover_subject_inputs, check_subject_input_completeness, download_session_inputs. Per-run preprocessing: shipped BOLD is trimmed (no orchestrator masking), preproc QC on trimmed BOLD, motion from confounds TSV with finiteness check, mask-based tissue/GS regressors, 4-tuple timing with validation. Run-level exclusions with categorized reasons populate preproc_excluded_runs. Analyses: condition-presence check before dispatch; skipped analyses get terminal outcomes. File lists for upload collected from FirstLevelResult.output_files plus upstream_names infrastructure files. Session exit category drives sentinel writing. Subject summary uses the three-way classification. AFNI provenance logged via runtime_check/load_lockfile in main().
      </notes>
    </change>

    <change id="C13" status="done" user_decision="n/a">
      <files_modified>
        <file path="environment.yaml" lines_changed="31" />
      </files_modified>
      <notes>
        Pinned python=3.14, numpy=2.4.2, pandas=3.0.1. Added nibabel. The pip git URL line is unchanged per the approved exclusion of the tagged-release step.
      </notes>
    </change>

    <change id="C14" status="done" user_decision="n/a">
      <files_modified>
        <file path="orch_config_production.yaml" lines_changed="116" />
        <file path="proc_config_production.yaml" lines_changed="319" />
        <file path=".gitignore" lines_changed="36" />
      </files_modified>
      <notes>
        Both configs created exactly per the locked specification: orch_config uses input_dir, func/anat_prefix_template placeholders, no retired keys; proc_config carries all three analysis blocks (nback_act, nback_conn, rest_conn) with the full 29-contrast table in identical label/function order, the locked condition labels (target_0back/instruction), all scalar settings from the brainstorm report, and orchestrator-filled placeholders. Both files added to .gitignore.
      </notes>
    </change>

  </changes_applied>

  <summary>
    <total_changes>14</total_changes>
    <completed>14</completed>
    <skipped>0</skipped>
    <blocked>0</blocked>
  </summary>

  <deferred_to_test>
    The following items require a /test pass after this build:
    1. The conda environment must be rebuilt with fmri-first-level-proc from the local v4.0.0-build workspace (approval-gated environment change) before any test run.
    2. tests/test_production_configs.py has two pre-existing defects that will cause errors once the configs exist: (a) module-scoped orch_config fixture depends on function-scoped mock_logger (ScopeMismatch); (b) the upstream closed-schema test passes placeholder/nonexistent paths to load_and_validate, which checks file existence.
    3. New tests are needed for: the three-way exit classification (classify_session_exit, most_severe_exit, sentinel suppression on unexpected_error exclusions, process_participant return codes); preprocessing QC on the trimmed BOLD (the existing tests assert this but the implementation change was part of this build).
  </deferred_to_test>

  <next_steps>Recommended: run /test to validate all changes. The test environment requires the local fmri-first-level-proc v4.0.0-build workspace installed first.</next_steps>
</implement_report>
