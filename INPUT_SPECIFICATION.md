# Input Specification

Complete reference for `ABCD_fmri_orchestrator_S3` (orchestrator version 4.0.0): command-line arguments, both configuration files, the producer input layout, every validation rule, the per-run and per-analysis processing rules, and all outputs. It requires fmri-first-level-proc >= 4.0.0 and < 5.0.0 ([repository](https://github.com/tjkeding/fmri-first-level-proc)). For an overview and quick start, see [README.md](README.md).

Placeholders used throughout: `{ID}` is the participant ID passed with `--subj_id`; `{NN}` is a session code (digits, for example `00`); `{task}` is a task label; `{RR}` is a zero-padded run number as it appears in producer filenames; `{R}` is the same run number without padding.

## Inputs at a glance

| Input | How it is supplied | Reference |
|-------|--------------------|-----------|
| Orchestrator config (YAML) | `--orchestrate_config` | Orchestrator config |
| Proc template config (YAML) | `--proc_config` | Proc config |
| Participant ID | `--subj_id` | Participant, session and subject list |
| Session filter | `--session` | Participant, session and subject list |
| Subject list | `--subject-list` (`run_orchestrator.py`) | Participant, session and subject list |
| Producer per-file outputs | Discovered on S3 | Producer input layout |
| Producer completion manifest | `_complete.json` in each session directory | Completion manifest |

## Command-line arguments

### `orchestrate_first_level.py`

| Flag | Type | Required | Default | Behavior |
|------|------|----------|---------|----------|
| `--orchestrate_config` | path | Yes | | Orchestrator config file |
| `--proc_config` | path | Yes | | Proc template config file |
| `--subj_id` | str | Yes | | Participant ID; must equal the `sub-` entity of the participant's input filenames |
| `--session` | str | No | all sessions | Process only this session code. The code must be among the discovered sessions, otherwise the participant fails |
| `--dry-run` | flag | No | off | Runs the version check, config validation, discovery and the input-completeness gate, then logs the sessions, per-task run counts and analyses. Skips the AFNI checks, downloads, processing and uploads. Exits 0 when everything passes |
| `--log-file` | path | No | none | Log file in addition to the console |
| `--skip-qc` | flag | No | off | No preprocessing QC, no first-level QC, and no session QC JSON |
| `--skip-first-level` | flag | No | off | Runs discovery, download and run preparation only. No analysis is run and the `_COMPLETE` sentinel is never written; outputs are still uploaded |

Startup order: fmri-first-level-proc version check; AFNI checks (not under `--dry-run`); orchestrator config loading and validation; proc config loading; cross-validation of the two configs; then participant processing. Any failure before participant processing ends the run with exit code 1.

### `run_orchestrator.py`

| Flag | Type | Required | Default | Behavior |
|------|------|----------|---------|----------|
| `--orchestrate_config` | path | Yes | | Passed to every participant run |
| `--proc_config` | path | Yes | | Passed to every participant run |
| `--subject-list` | path | Yes | | Subject list file |
| `--log-dir` | path | Yes | | Created if absent; each participant logs to `{log_dir}/{ID}.log` |
| `--n-jobs` | int | No | `1` | Number of participants processed at once (thread pool) |
| `--dry-run` | flag | No | off | Forwarded to every participant run |
| `--skip-qc` | flag | No | off | Forwarded to every participant run |
| `--skip-first-level` | flag | No | off | Forwarded to every participant run |
| `--session` | str | No | | Forwarded to every participant run |
| `--summary-file` | path | No | `{log_dir}/run_summary_{YYYYMMDD_HHMMSS}.csv` | Batch summary CSV |

Each participant is run as a subprocess (`orchestrate_first_level.py` from the same directory as `run_orchestrator.py`, with the current Python interpreter). A progress line is printed every 2 seconds. The batch fails immediately (exit 1) when `orchestrate_first_level.py` is not found next to the runner, when the subject list is missing, or when it contains no IDs.

### Exit codes

`orchestrate_first_level.py`:

| Code | Meaning |
|------|---------|
| `0` | Every processed session is complete (see Session outcome classification), or a dry run passed |
| `1` | At least one session failed with an unexplained cause, or a fatal error stopped the participant before session processing |
| `2` | At least one session failed with a known cause and no session failed with an unexplained cause |

The participant's code is the most severe session code, with precedence 1 over 2 over 0.

`run_orchestrator.py`:

| Code | Meaning |
|------|---------|
| `0` | Every participant exited 0 |
| `1` | At least one participant exited 1, exited with any code other than 0 or 2, or its subprocess could not be run |
| `2` | At least one participant exited 2 and none failed as described for code 1 |
| `130` | Interrupted with Ctrl+C. Participants already running finish; participants not yet started are recorded as cancelled; the summary CSV is written |

## Orchestrator config

A YAML mapping. Required sections: `study`, `tasks`, `analyses`, `s3`. Optional section: `qc`. Keys not described here are ignored, except that a fixed set of keys from earlier schema versions is rejected with an error that names the replacement.

### `study`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `input_dir` | str | Yes | | Absolute path. The download root: each input S3 key is written to `{input_dir}/{key}` |
| `output_dir` | str | Yes | | Absolute path. Session outputs are written to `{output_dir}/sub-{ID}/ses-{NN}A/` |
| `space` | str | Yes | | Template space entity (for example `MNI152NLin2009cAsym`). Input files whose `space-` entity differs are ignored with a warning |
| `TR` | number | Yes | | Repetition time in seconds, > 0. Must equal the proc config's `global.tr` within 1e-6 |
| `force_recompute` | bool | No | `false` | An existing `_COMPLETE` sentinel is deleted and the session recomputed |

### `tasks`

A non-empty list. Runs are discovered from the input files, not configured.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `task_label` | str | Yes | | `nback` or `rest`. Must match the `task-` entity of input filenames. Only `rest` is treated as resting state (no events) |
| `condition_column` | str | No | `trial_type` | Events column holding condition labels |
| `conditions_exclude` | list of str or null | No | `null` | Conditions removed before timing files are written (for example `["dummy"]`) |
| `fix_nback_cues` | bool | No | `false` | Relabel n-back cue events (see Events TSV) |

`condition_column`, `conditions_exclude` and `fix_nback_cues` are used only for the `nback` task.

### `qc`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `preproc.enabled` | bool | `false` | Per-run preprocessing QC (DVARS, brain-mask coverage, producer framewise displacement) |
| `preproc.tsnr` | bool | `false` | Median brain tSNR and a tSNR map (requires `preproc.enabled`) |
| `preproc.carpet_plots` | bool | `false` | Carpet plot PNG (requires `preproc.enabled`) |
| `first_level.enabled` | bool | `false` | Per-analysis QC block in the session QC JSON |

An omitted `qc` section disables everything. `--skip-qc` overrides all four switches.

### `analyses`

A non-empty list. Each entry links to the proc-config analysis block with the same `name`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | str | Yes | Name of the matching proc-config block |
| `type` | str | Yes | `task_act`, `task_conn` or `rest_conn`; must equal the proc block's `type` |
| `task_label` | str | Yes | A `task_label` defined in `tasks` |
| `post_id_out_pre` | str | Yes | Sets `out_file_pre = sub-{ID}_ses-{NN}A_{post_id_out_pre}` |
| `post_id_extract_pre` | str | When the proc block has an `extraction` block | Sets `extraction.extract_out_file_pre = sub-{ID}_ses-{NN}A_{post_id_extract_pre}` |
| `post_id_conn_pre` | str | For `task_conn` and `rest_conn` | Sets `connectivity.conn_out_file_pre = sub-{ID}_ses-{NN}A_{post_id_conn_pre}` |

Example for participant `SUBJ01`, session `00`: `post_id_out_pre: "nback"` gives `sub-SUBJ01_ses-00A_nback`, and `post_id_extract_pre: "nback_SchaeferTian"` gives `sub-SUBJ01_ses-00A_nback_SchaeferTian`.

Analysis settings that belong in the proc config (for example `hrf_model`, `contrasts`, `bandpass`, `extraction`, `connectivity`) are ignored with a warning when they appear in an orchestrator analysis entry. Censoring thresholds are not accepted in the orchestrator config.

### `s3`

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | bool | Yes | | Must be `true` |
| `bucket` | str | Yes | | Bucket name only, without `s3://` |
| `func_prefix_template` | str | Yes | | Key prefix of the session directory holding the run files, with `{subject}` and `{session}` placeholders (for example `derivatives/func/sub-{subject}/ses-{session}A`). No leading or trailing `/` |
| `anat_prefix_template` | str | Yes | | Key prefix of the session directory holding the tissue masks; same rules. May equal `func_prefix_template` |
| `upload_prefix` | str | Yes | | Output key prefix. No leading or trailing `/` |
| `cleanup_after_upload` | bool | No | `true` | Delete downloaded inputs and the local session tree after a successful upload, and downloaded inputs after a failed session |
| `upload_max_workers` | int | No | `8` | Upload threads, 1 to 64 |

`{subject}` is replaced with `--subj_id`. Discovery lists S3 once per participant, from the longest common prefix of the two templates up to `{session}` (for the example above, `derivatives/func/sub-{ID}/ses-`). The completion manifest is located from the listing itself: it must sit in a directory named `ses-{NN}A` directly under `sub-{ID}`.

### Orchestrator config validation

Performed by `load_orchestrator_config` at startup. Every failure is fatal (exit 1) unless marked as a warning.

| Check | Message pattern |
|-------|-----------------|
| File exists | `Orchestrator config not found: {path}` |
| Valid YAML | `YAML parse error in {path}: {error}` |
| Not empty | `Config file is empty: {path}` |
| `study`, `tasks`, `analyses`, `s3` present | `Config missing required section '{section}'.` |
| `input_dir`, `output_dir`, `space`, `TR` present and non-null | `Config study section missing required key '{key}'.` |
| `input_dir`, `output_dir` absolute | `study.{key} must be an absolute path, got: {value}` |
| `TR` positive number | `study.TR must be a positive number, got: {value}` |
| `tasks` non-empty list | `'tasks' must be a non-empty list.` |
| Each task has `task_label` | `tasks[{i}] missing required key 'task_label'.` |
| `task_label` is `nback` or `rest` | `tasks[{i}] task_label '{label}' is not a recognized task label for this orchestrator. ...` |
| `analyses` non-empty list | `'analyses' must be a non-empty list.` |
| `type` present and valid | `[{name}] Missing required key 'type'.` / `[{name}] Invalid type '{type}'. ...` |
| `task_label` present and defined in `tasks` | `[{name}] Missing required key 'task_label'.` / `[{name}] task_label '{label}' not defined in tasks section. ...` |
| `post_id_out_pre` present | `[{name}] Missing required key 'post_id_out_pre'.` |
| `post_id_conn_pre` for `task_conn`, `rest_conn` | `[{name}] {type} analysis requires 'post_id_conn_pre'.` |
| Proc-config settings in an analysis entry | Warning: `[{name}] Field '{field}' in orchestrator analysis block is deprecated. ...` |
| `s3.enabled` is true | `s3.enabled must be true: this orchestrator reads its inputs from S3 and writes its outputs to S3.` |
| Required S3 fields | `Required field 's3.{field}' is null or missing.` |
| Bucket without `s3://` | `s3.bucket must be the bucket name only (no 's3://' prefix), got: {value}` |
| Prefixes without leading or trailing `/` | `s3.{field} must not have a leading or trailing '/', got: {value}` |
| Templates contain `{subject}` and `{session}` | `s3.{field} must contain both '{subject}' and '{session}' placeholders, got: {value}` |
| `force_recompute` is boolean | `study.force_recompute must be a boolean, got: {value}` |
| `upload_max_workers` integer in [1, 64] | `s3.upload_max_workers must be an integer in [1, 64], got: {value}` |
| Key from an earlier schema version | Message names the key and its replacement |

## Proc config

The proc config is an fmri-first-level-proc configuration used as a template; its full schema is defined in fmri-first-level-proc's `INPUT_SPECIFICATION.md`. For each session the orchestrator deep-copies it, fills the fields below, removes analysis blocks not named in the orchestrator config (with a warning), writes the result to `{output_dir}/sub-{ID}/ses-{NN}A/sub-{ID}_ses-{NN}A_first_level_config.yaml`, and validates it with fmri-first-level-proc's own loader before running the analyses. Every other field, including `global.tr`, censoring thresholds, `remove_previous`, HRF model, contrasts, bandpass and nuisance settings, is passed through unchanged.

### Fields filled by the orchestrator

| Field | Value |
|-------|-------|
| `analyses[].out_dir` | `{output_dir}/sub-{ID}/ses-{NN}A/first_level_out/{name}` |
| `analyses[].out_file_pre` | `sub-{ID}_ses-{NN}A_{post_id_out_pre}` |
| `analyses[].extraction.extract_out_file_pre` | `sub-{ID}_ses-{NN}A_{post_id_extract_pre}`, when both the key and the block exist |
| `analyses[].connectivity.conn_out_file_pre` | `sub-{ID}_ses-{NN}A_{post_id_conn_pre}`, when both the key and the block exist |
| `analyses[].run_labels` | Integer run numbers of the surviving runs of the analysis's task, in ascending order |
| `analyses[].paths` | Replaced with the per-run lists below |

`paths` for `task_act` and `task_conn`: `scan_paths`, `motion_paths` and `task_timing_paths`. For `rest_conn`: `scan_paths` and `motion_paths`. For any type, `CSF_paths`, `WM_paths` and `GS_paths` are filled only when the template's own value for that key is non-null; a null or absent key is omitted from the rendered config. All lists are aligned with `run_labels`.

| Path list | Contents |
|-----------|----------|
| `scan_paths` | Trimmed BOLD per run (the shipped BOLD when no volume was trimmed) |
| `motion_paths` | `{run_prefix}_motion.1D` per run |
| `task_timing_paths` | `{run_prefix}_timing.tsv` per run |
| `CSF_paths`, `WM_paths`, `GS_paths` | `{run_prefix}_csf.1D`, `_wm.1D`, `_gs.1D` per run |

`{run_prefix}` is `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}`.

### Values the orchestrator reads from the proc config

| Field | Use |
|-------|-----|
| `global.tr` | Required; must equal `study.TR` within 1e-6. Never injected |
| `global.template_path` | Must be an absolute path when set |
| `analyses[].paths.CSF_paths`, `WM_paths`, `GS_paths` | Non-null in any analysis of a task means the regressor is computed for that task's runs, and the corresponding mask is required by the input-completeness gate (CSF and WM) |
| `analyses[].cond_labels` (`task_act`), `cond_beta_labels` (`task_conn`) | Required condition labels for the analysis (see Analysis-level processing) |
| `analyses[].include_motion_derivs` (`task_act`, `task_conn`), `motion_deriv_degree` (`rest_conn`) | Number of motion derivative sets (see Motion regressors) |

### Proc config validation

Performed by `validate_proc_template` at startup. Every failure is fatal (exit 1) unless marked as a warning.

| Check | Message pattern |
|-------|-----------------|
| Not null | `Proc template config is empty (null).` |
| `analyses` is a non-empty list | `Proc template must have 'analyses' as a non-empty list.` |
| `global` is a mapping | `Proc template must have a 'global' block (dict). ...` |
| `global.tr` present | `global.tr is required in the proc template (it is never injected by the orchestrator).` |
| `global.template_path` absolute when set | `global.template_path must be an absolute path when set, got: {value}` |
| `global.tr` equals `study.TR` | `global.tr in proc template ({tr}) does not match study.TR ({TR}). These must be consistent.` |
| Every block has `name` | `Proc template has an analysis block without a 'name' field.` |
| No `censor_path` or `censor_paths` in `paths` | `Proc template analysis '{name}' contains 'censor_path' in its paths block. ...` |
| No singular non-null `CSF_path` or `WM_path` in task blocks | `Proc template analysis '{name}' uses singular '{key}' in its paths block. ...` |
| No `extract`, `extract_labels` or `extract_stat` in `extraction` | `Proc template analysis '{name}' uses legacy extraction keys {keys}. ...` |
| Each orchestrator analysis has a block with the same name | `Orchestrator analysis '{name}' has no matching entry in proc template. ...` |
| Types match | `Type mismatch for analysis '{name}': orchestrator has '{a}', proc template has '{b}'.` |
| `post_id_out_pre` present | `[{name}] Missing required key 'post_id_out_pre'.` |
| `post_id_extract_pre` present when the block has `extraction` | `[{name}] Proc template has an 'extraction' block but orchestrator analysis is missing 'post_id_extract_pre'.` |
| `post_id_conn_pre` for `task_conn`, `rest_conn` | `[{name}] {type} analysis requires 'post_id_conn_pre'.` |
| Template blocks not named in the orchestrator config | Warning; the blocks are removed from the rendered config |

## Participant, session and subject list

**Participant ID.** Any string of letters and digits, used verbatim in filenames and S3 keys. It must equal the `sub-` entity of the input filenames; input files carrying another participant's ID are ignored with a warning.

**Session.** Session directories and filenames use `ses-{NN}A`, where `{NN}` is one or more digits (the session code) and `A` is a fixed suffix. `--session` takes the code only (`00`, not `ses-00A`).

**Subject list** (`run_orchestrator.py`): plain text, one ID per line. Leading and trailing whitespace is stripped; blank lines and lines starting with `#` are ignored; a repeated ID is skipped with a warning.

```
# example subject list
SUBJ01
SUBJ02
SUBJ03
```

## Producer input layout

### Discovery

Inputs are discovered with one paginated S3 listing per participant, from the prefix described under `s3`. When `--session` is given, only that session is kept, and only its manifest is read and validated. Every listed file is classified by its basename:

| Role | Basename pattern |
|------|------------------|
| BOLD | `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}_space-{space}_bold.nii.gz` |
| Brain mask | `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}_space-{space}_brainmask.nii.gz` |
| Confounds | `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}_desc-confounds_timeseries.tsv` |
| Events | `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}_events.tsv` |
| CSF mask | `sub-{ID}_ses-{NN}A_space-{space}_label-CSF_mask.nii.gz`, optionally with `_res-{res}` before `_label` |
| WM mask | `sub-{ID}_ses-{NN}A_space-{space}_label-WM_mask.nii.gz`, optionally with `_res-{res}` before `_label` |

`{ID}`, `{task}`, `{space}` and `{res}` are letters and digits; `{NN}` and `{RR}` are digits, and run numbers are read as integers (`run-01` is run 1).

Classification rules:

- A file is ignored with a warning when its participant ID differs from `--subj_id`, its `space-` entity differs from `study.space`, or (for run files) its task is not configured.
- Known producer files that are not inputs are ignored silently (logged at debug level): segmentation images and tables (`_res-{res}_dseg`), GM masks, tissue probability maps (`_probseg`), BOLD reference volumes (`_desc-boldref`), the completion manifest itself, and archived BOLD bundles (`_bold.tar.gz`). Any other unrecognized file is ignored with a warning.
- A BOLD file defines a run. A brain mask, confounds or events file without a BOLD for the same session, task and run is ignored with a warning.
- Two files with the same role for the same run, or two CSF or WM masks for the same session, stop the participant with an error.

### Completion manifest

The producer writes `_complete.json` into a session directory after all of that session's files. The orchestrator uses a session only when the manifest is present:

- A session directory without `_complete.json` is skipped (logged at info level) as still being written.
- The manifest must be a JSON object with `schema_version` equal to `1` and `kind` equal to `"func-session"`.
- Every file listed under `runs` for a configured task (keys of the form `task-{task}_run-{RR}`) must be present in the listing, and every CSF or WM mask listed under `session_files` must be present.
- Any violation stops the participant with an error (exit 1). With `--session`, only the selected session's manifest is read; without it, every session's manifest is validated.
- `written_at` and `schema_version` are copied into the session QC JSON (`provenance.producer_manifest`). Other manifest fields are not read.

```json
{
  "schema_version": 1,
  "kind": "func-session",
  "written_at": "2026-01-01T00:00:00+00:00",
  "runs": {
    "task-nback_run-01": [
      "sub-SUBJ01_ses-00A_task-nback_run-01_desc-confounds_timeseries.tsv",
      "sub-SUBJ01_ses-00A_task-nback_run-01_events.tsv",
      "sub-SUBJ01_ses-00A_task-nback_run-01_space-MNI152NLin2009cAsym_bold.nii.gz",
      "sub-SUBJ01_ses-00A_task-nback_run-01_space-MNI152NLin2009cAsym_brainmask.nii.gz"
    ]
  },
  "session_files": [
    "sub-SUBJ01_ses-00A_space-MNI152NLin2009cAsym_res-1_label-CSF_mask.nii.gz",
    "sub-SUBJ01_ses-00A_space-MNI152NLin2009cAsym_res-1_label-WM_mask.nii.gz"
  ]
}
```

### Input-completeness gate

Evaluated on the selected sessions before any download. The participant stops with one error listing every problem (exit 1) when:

- no session was discovered;
- a session has no run for any configured task;
- a run has no brain mask or no confounds file;
- a session has a run of a task for which some analysis requests non-null `CSF_paths` (or `WM_paths`) and the session has no CSF (or WM) mask.

Events files and file contents are not checked here.

### Confounds TSV

Tab-separated with a header; one row per BOLD volume before trimming. Columns are selected by name:

| Column | Required | Use |
|--------|----------|-----|
| `trans_x`, `trans_y`, `trans_z` | Yes | Translations in mm |
| `rot_x`, `rot_y`, `rot_z` | Yes | Rotations in degrees |
| `non_steady_state_outlier*` | No | One column per initial non-steady-state volume; their count is the number of volumes trimmed |
| `framewise_displacement` | No | Producer FD in mm, for the rotation-unit check and informational QC |
| `dvars` | No | DVARS QC and the carpet-plot trace |

Other columns, including precomputed derivatives, are ignored.

### BOLD and brain mask

The BOLD image is used as shipped (no additional masking). The run brain mask is used for tSNR, brain-mask QC, the carpet plot, tissue-mask intersection and the global-signal regressor.

### Tissue masks

Binary session-level CSF and WM masks in the study space, at any resolution. For each run they are resampled to the run's BOLD grid with nearest-neighbor interpolation and intersected with the run brain mask.

### Events TSV

BIDS events for `nback` runs: tab-separated with columns `onset` and `duration` (seconds from the first acquired volume) and the condition column. Rest runs have no events.

## Run preparation

Each run of each configured task is prepared in this order. A failure excludes the run with a reason (see Run exclusion reasons) and processing continues with the next run.

1. **Events presence.** A non-rest run without an events file in the listing is excluded (`events_missing`) before any processing.
2. **Non-steady-state trimming.** The number of `non_steady_state_outlier*` columns is the number of initial volumes removed with `3dTcat` (`{run_prefix}_trimmed.nii.gz`). With no such column the shipped BOLD is used directly. The retained volume count is read with `3dinfo -nv`; a missing or non-positive count excludes the run.
3. **Preprocessing QC**, when enabled (see Preprocessing QC).
4. **Motion regressors** (see Motion regressors).
5. **Tissue and global-signal regressors**, for tasks whose analyses request them (see Tissue and global-signal regressors).
6. **Task timing**, for `nback` runs (see Task timing).

Surviving runs are collected in ascending run order into run-aligned lists for the task.

### Motion regressors

- The confounds row count must equal the BOLD volume count before trimming.
- The six base columns must exist and none may be entirely missing.
- Rotation-unit check: framewise displacement is recomputed from the six base parameters (Power et al., 2012) as the sum of absolute backward differences, with rotations taken as degrees and converted to arc length on a 50 mm sphere, and compared with `framewise_displacement` using an absolute tolerance of 1e-6 mm over frames where both values are finite. A mismatch excludes the run (`producer_fd_inconsistent`). When `framewise_displacement` is absent or no frame is comparable, the check records `not_checkable` and rotations are taken as degrees.
- The number of derivative sets `n` is the maximum over all configured analyses of: 1 for a task analysis with `include_motion_derivs: true`, 0 for one with `false`, and `motion_deriv_degree - 1` (minimum 0) for `rest_conn`. The same `n` applies to every run of the session.
- Derivatives are backward differences computed before trimming, with 0 in the first row; each derivative set is the difference of the previous set.
- After trimming, any non-finite value excludes the run (`motion_non_finite`). No imputation is performed.
- Output `{run_prefix}_motion.1D`: tab-delimited, no header, one row per retained volume, `6 * (1 + n)` columns in the order `trans_x, trans_y, trans_z, rot_x, rot_y, rot_z`, then the same six for each derivative set. Translations in mm, rotations in degrees.

### Tissue and global-signal regressors

For each run of a task whose analyses request `CSF_paths` or `WM_paths`: the session mask is resampled to the run's trimmed BOLD grid (`3dresample`, nearest neighbor), intersected with the run brain mask (`3dcalc`, `step(a)*step(b)`), its voxels counted (`3dBrickStat -count -non-zero`), and the mean time course within it extracted from the trimmed BOLD (`3dmaskave`). A mask with no voxels inside the brain mask excludes the run (`tissue_mask_empty`). For `GS_paths` the run brain mask itself is used. Outputs `{run_prefix}_csf.1D`, `_wm.1D` and `_gs.1D` hold one value per retained volume. Intermediate masks are deleted.

### Task timing

1. **Cue relabeling** (`fix_nback_cues: true`). Each event whose condition is `cue` (case-insensitive, surrounding whitespace and quotes ignored) is relabeled from the next event whose condition matches `{level}_back_{stimulus}`: level `0` gives `target_0back` (the 0-back target presentation, pooled across stimuli) and any other level gives `instruction` (the 2-back instruction screen). Recall trials keep their labels. A cue with no later n-back event excludes the run. The relabeled file is written to `{run_prefix}_events_fixed.tsv`.
2. **Filtering.** Events whose condition is in `conditions_exclude` are removed. A run with no events left is excluded.
3. **Onset shift.** `onset = onset - n_trimmed * TR`. Every event with a negative shifted onset is dropped, including events that straddle the cutoff (shifted onset below 0 and shifted end above 0). Straddling events are counted separately and named in the log.
4. **Output.** `{run_prefix}_timing.tsv`, tab-separated with columns `trial_type`, `onset`, `duration`, sorted by onset. Onsets are relative to the first retained volume of the run.
5. **Validation.** Every onset and duration must be finite (otherwise `events_invalid_values`), and every onset must lie in `[0, n_retained * TR)` (otherwise `events_onset_out_of_range`).

Example of cue relabeling in an n-back run:

| Raw `trial_type` | Written as |
|------------------|------------|
| `cue` followed by `0_back_posface` trials | `target_0back` |
| `0_back_posface` | `0_back_posface` |
| `cue` followed by `2_back_place` trials | `instruction` |
| `2_back_place` | `2_back_place` |

### Run exclusion reasons

| Reason | Cause | Session classification |
|--------|-------|------------------------|
| `events_missing` | Non-rest run without an events file | Documented (terminal) |
| `motion_non_finite` | Non-finite motion values after derivatives and trimming, or a base motion column entirely missing values | Documented (terminal) |
| `tissue_mask_empty` | Tissue mask has no voxels inside the run brain mask | Documented (terminal) |
| `events_invalid_values` | Non-finite onset or duration in the timing file | Documented (terminal) |
| `events_onset_out_of_range` | Onset outside the retained scan | Documented (terminal) |
| `producer_fd_inconsistent` | Rotation-unit check failed | Unexplained |
| `unexpected_error` | Any other failure, for example an AFNI command error, a confounds row count that differs from the BOLD volume count, missing motion columns, a missing condition column, no events left after filtering, or a cue with no following n-back event | Unexplained |

Each exclusion is recorded in the session QC JSON (`preproc_excluded_runs`) with `run_label`, `task`, `reason`, `error_type` and `message`.

## Analysis-level processing

For each orchestrator analysis, in config order:

- **No usable runs.** When no run of the analysis's task survived, the analysis is skipped (`no_usable_runs`).
- **Required conditions** (`task_act`, `task_conn`). The required labels are `cond_labels` (`task_act`) or `cond_beta_labels` (`task_conn`) from the proc block. When any label is absent from the union of conditions across the surviving runs, the analysis is skipped (`events_missing_conditions`) and the missing labels are recorded in `error_context.missing_conditions`. A condition present in only some runs counts as present.

The remaining analyses share one rendered config, validated by fmri-first-level-proc's loader; a validation error stops the session (unexplained). Each analysis is then run by fmri-first-level-proc. Any exception from one analysis is recorded and the next analysis still runs.

| Outcome field | Value |
|---------------|-------|
| `status` | `success` when no exception was raised; `failed` otherwise; `skipped` for the two skip reasons |
| `upstream_status` | `FirstLevelResult.status` from fmri-first-level-proc (`success` or `warning`), or null |
| `qc_flagged` | True when `upstream_status` is `warning` |
| `error`, `error_type`, `error_context` | Exception message, class name and JSON-safe context |
| `warnings` | Graded warning records from fmri-first-level-proc; warnings about an unvalidated AFNI version are removed (AFNI at or above the upstream minimum is accepted) |
| `terminal` | True for success, for the two documented skips, and for a failure whose `error_type` is `ModelError` |

### First-level QC

When `qc.first_level.enabled` is true, each analysis that ran gets a `qc` block:

| Field | Description |
|-------|-------------|
| `sub_id` | `sub-{ID}` |
| `analysis_name`, `type` | Analysis name and type |
| `completed_successfully` | True only when the analysis raised no exception and its primary output file exists and is non-empty. The primary output is defined by fmri-first-level-proc; in 4.0.0 it is `{out_file_pre}_concat_bucket_stats.nii.gz` (`task_act`), `{out_file_pre}_concat_LSS.nii.gz` (`task_conn`) and `{out_file_pre}_concat_residual_dtseries.nii.gz` (`rest_conn`) |
| `pct_censored` | `pct_censored` from the fmri-first-level-proc QC summary, rounded to 2 decimals |
| `error` | Error message, or null |
| `n_nifti_outputs` | Number of NIfTI outputs, excluding percent-signal-change intermediates (`_psc.nii.gz`) |
| `output_files` | Output filenames |
| `upstream_qc_summary` | The full fmri-first-level-proc QC summary (`{out_file_pre}_qc_summary.json`), or null when missing or unreadable |
| `session` | `ses-{NN}A` |

## Session outcome classification

| Category | Rule (evaluated in order) |
|----------|---------------------------|
| `1` unexplained | The session raised an error; or a run was excluded for `unexpected_error` or `producer_fd_inconsistent`; or, unless `--skip-first-level` was used, any analysis outcome is not terminal |
| `2` known cause | Otherwise, unless `--skip-first-level` was used, at least one analysis failed with `ModelError` |
| `0` complete | Otherwise |

Session status in the QC JSON: `success` when every analysis succeeded, `partial` when at least one succeeded, `failed` when none succeeded (including when every analysis was skipped). A session skipped because of an existing sentinel is reported as `skipped` in the log only.

## S3 routing, upload and cleanup

**Routing.** Before a session is processed, `{upload_prefix}/sub-{ID}/ses-{NN}A/_COMPLETE` is probed once. When it exists and `force_recompute` is false, the session is skipped (category 0). When it exists and `force_recompute` is true, it is deleted. Otherwise the session is processed from an empty local output directory (any existing local session tree is removed first).

**Download.** Each input key is downloaded to `{input_dir}/{key}`; a file already present locally is reused. A failed download stops the session (unexplained).

**Upload set.** For each analysis that returned a result: the files it reports as outputs, plus its ledger (`{out_file_pre}_ledger.jsonl`) when present. For each analysis that failed: its timing-error file (`{out_file_pre}_timing_errors.json`) and QC summary when present. In addition: every file under `qc/`, every file directly under `preproc/` except `.nii.gz` images, and the rendered config. Files outside the session directory are never uploaded. Keys are `{upload_prefix}/sub-{ID}/ses-{NN}A/{path relative to the session directory}`.

**Upload procedure.**

1. Stop with an error when the upload set is empty.
2. List the session prefix. Stop with an error when `_COMPLETE` is already present (`Stale sentinel present at upload start`).
3. Upload, with `upload_max_workers` threads, only files that are missing remotely or whose remote size differs. Progress is logged at info level about every 10% of the files being uploaded. Any failed upload stops the session.
4. List again and verify that every expected key exists with the expected size; otherwise stop (`Post-upload verification failed`).
5. Delete every other key under the session prefix (stale outputs of earlier attempts).
6. Write the zero-byte `_COMPLETE` object unless the session is unexplained (category 1) or `--skip-first-level` was used.

**Cleanup.** With `cleanup_after_upload: true`, the downloaded inputs and the local session directory are deleted after a successful upload. When a session raises an error, its downloaded inputs are deleted and its local outputs are kept; nothing is uploaded and no sentinel is written, so the next invocation reprocesses it.

## Outputs

### Session directory

`{output_dir}/sub-{ID}/ses-{NN}A/`:

| Path | Written when | Uploaded |
|------|--------------|----------|
| `sub-{ID}_ses-{NN}A_first_level_config.yaml` | At least one analysis is run | Yes |
| `preproc/{run_prefix}_trimmed.nii.gz` | Non-steady-state volumes were trimmed | No |
| `preproc/{run_prefix}_motion.1D` | Every prepared run | Yes |
| `preproc/{run_prefix}_csf.1D`, `_wm.1D`, `_gs.1D` | The regressor is requested | Yes |
| `preproc/{run_prefix}_events_fixed.tsv` | `nback` runs with `fix_nback_cues: true` | Yes |
| `preproc/{run_prefix}_timing.tsv` | `nback` runs | Yes |
| `qc/sub-{ID}_ses-{NN}A_orchestrator_qc.json` | QC is not skipped and there is something to report | Yes |
| `qc/preproc/sub-{ID}_{run_label}_tsnr.nii.gz` | `qc.preproc.tsnr` | Yes |
| `qc/preproc/sub-{ID}_{run_label}_carpet.png` | `qc.preproc.carpet_plots` | Yes |
| `first_level_out/{name}/...` | fmri-first-level-proc outputs | Reported outputs only |

`{run_label}` is `ses-{NN}A_task-{task}_run-{R}`. The same label keys the `preprocessing` entries in the QC JSON.

### Preprocessing QC

Computed per run on the trimmed BOLD when `qc.preproc.enabled` is true and `--skip-qc` is not set. Confounds-derived series are trimmed by the same number of volumes.

| Metric | Method |
|--------|--------|
| `dvars` | Mean and maximum of the non-missing `dvars` values, or null |
| `tsnr.median_brain` | Voxelwise mean divided by standard deviation over time (`3dTstat`; denominator floored at 0.001) within the brain mask; median over the mask with `3dBrickStat`. Null when disabled or on failure. The tSNR map is kept |
| `brain_mask` | Non-zero voxel count and volume in mm3 (voxel count times voxel volume from `3dinfo -ad3`) |
| `carpet_plot_path` | DVARS trace above a voxel-by-volume heatmap of at most 2000 evenly spaced brain voxels, each z-scored over time and displayed from -2 to 2; 150 dpi PNG. Skipped with a warning when matplotlib is unavailable, an AFNI step fails, or the streamed voxel count differs from the mask count |
| `producer_fd_mean`, `producer_fd_max` | Mean and maximum of the finite `framewise_displacement` values (informational, unfiltered); null when the column is absent |

### Session QC JSON

`qc/sub-{ID}_ses-{NN}A_orchestrator_qc.json`, written for every processed session unless `--skip-qc` is set or there is nothing to report.

| Field | Description |
|-------|-------------|
| `provenance.orchestrator_version` | Orchestrator version |
| `provenance.fmri_first_level_proc_version` | Installed fmri-first-level-proc version |
| `provenance.afni_version` | Output of `afni --version`, or null |
| `provenance.timestamp_utc` | ISO 8601 write time |
| `provenance.sub_id`, `provenance.session` | `{ID}` and `ses-{NN}A` |
| `provenance.producer_manifest` | `{written_at, schema_version}` from the completion manifest |
| `preprocessing.{run_label}` | Per-run record: `sub_id`, `session`, `task`, `run`, `non_steady_state_trs` and the Preprocessing QC metrics (when enabled); `motion_units_check` (`producer_fd_consistent`, `not_checkable` or `producer_fd_inconsistent`); `csf_mask_voxels`, `wm_mask_voxels`, `gs_mask_voxels` (voxels after intersection, when the regressor is computed); for `nback` runs `n_events_dropped_before_start` (all dropped events, including straddling ones), `n_events_dropped_straddling`, `n_events_written` and `conditions_present` |
| `preproc_excluded_runs` | List of `{run_label, task, reason, error_type, message}` |
| `analyses.{name}` | `type`, `status`, `reason`, `upstream_status`, `qc_flagged`, `error`, `error_type`, `error_context`, `warnings`, `wall_time_seconds`, `qc` (First-level QC, or null), `run_labels`, `terminal` |
| `session.status` | `success`, `partial` or `failed` |
| `session.wall_time_seconds` | Session wall time |
| `session.n_analyses_attempted`, `n_analyses_succeeded`, `n_analyses_skipped`, `n_analyses_flagged` | Counts over the analysis outcomes (attempted includes skipped) |
| `session.any_analysis_flagged` | True when any analysis is `qc_flagged` |
| `session.all_terminal` | True when there is at least one analysis outcome and all are terminal |
| `session.peak_memory_bytes`, `session.peak_memory_source` | `memory.peak` from cgroup v2 (`cgroup_v2`) when available, otherwise the largest child process resident size (`rusage_largest_child`) |
| `session.flags` | List of `{code, detail}`: `preproc_excluded_runs`, `analysis_skipped`, `analysis_failed`, `upstream_warning`. Informational only; flags never change status or classification |
| `session.any_flag` | True when `flags` is non-empty |

Abbreviated example (values illustrative):

```json
{
  "provenance": {
    "orchestrator_version": "4.0.0",
    "fmri_first_level_proc_version": "4.0.0",
    "afni_version": "...",
    "timestamp_utc": "2026-01-01T00:00:00+00:00",
    "sub_id": "SUBJ01",
    "session": "ses-00A",
    "producer_manifest": {"written_at": "2026-01-01T00:00:00+00:00", "schema_version": 1}
  },
  "preprocessing": {
    "ses-00A_task-nback_run-1": {
      "sub_id": "sub-SUBJ01", "session": "00", "task": "nback", "run": 1,
      "non_steady_state_trs": 8,
      "dvars": {"mean": 30.0, "max": 80.0},
      "tsnr": {"median_brain": 50.0},
      "brain_mask": {"n_voxels": 200000, "volume_mm3": 1600000.0},
      "carpet_plot_path": "{output_dir}/sub-SUBJ01/ses-00A/qc/preproc/sub-SUBJ01_ses-00A_task-nback_run-1_carpet.png",
      "producer_fd_mean": 0.15, "producer_fd_max": 0.9,
      "motion_units_check": "producer_fd_consistent",
      "n_events_dropped_before_start": 1, "n_events_dropped_straddling": 0,
      "n_events_written": 80,
      "conditions_present": ["0_back_negface", "...", "instruction", "target_0back"]
    }
  },
  "preproc_excluded_runs": [],
  "analyses": {
    "nback_act": {
      "type": "task_act", "status": "success", "reason": null,
      "upstream_status": "success", "qc_flagged": false,
      "error": null, "error_type": null, "error_context": {},
      "warnings": [], "wall_time_seconds": 300.0,
      "qc": {
        "sub_id": "sub-SUBJ01", "analysis_name": "nback_act", "type": "task_act",
        "pct_censored": 5.0, "completed_successfully": true, "error": null,
        "n_nifti_outputs": 3, "output_files": ["..."],
        "upstream_qc_summary": {"pct_censored": 5.0},
        "session": "ses-00A"
      },
      "run_labels": [1, 2], "terminal": true
    }
  },
  "session": {
    "status": "success", "wall_time_seconds": 1500.0,
    "n_analyses_attempted": 3, "n_analyses_succeeded": 3,
    "n_analyses_skipped": 0, "n_analyses_flagged": 0,
    "any_analysis_flagged": false, "all_terminal": true,
    "peak_memory_bytes": 4000000000, "peak_memory_source": "cgroup_v2",
    "flags": [], "any_flag": false
  }
}
```

### S3 outputs

`s3://{bucket}/{upload_prefix}/sub-{ID}/ses-{NN}A/` mirrors the uploaded part of the session directory, and `_COMPLETE` (zero bytes) marks a finished session. Objects named `_COMPLETE` must not be placed under a session prefix by other tools: the orchestrator treats one as a completion marker.

### Batch summary CSV

Written by `run_orchestrator.py` after all participants finish, and also after an interrupt.

| Column | Description |
|--------|-------------|
| `sub_id` | Participant ID |
| `status` | `success`, `failed_unexplained`, `failed_known_cause` or `cancelled` |
| `runtime_seconds` | Wall time, rounded to 2 decimals |
| `error_message` | Last non-empty stderr line of the participant run, else its last stdout line, else `Exit code {N}`; empty on success |
| `log_file` | `{log_dir}/{ID}.log` |

## Environment

- **Python packages**: as listed in `environment.yaml` (Python 3.14, numpy, pandas, PyYAML, matplotlib, boto3, packaging, and fmri-first-level-proc installed from GitHub).
- **fmri-first-level-proc**: >= 4.0.0 and < 5.0.0, compared under PEP 440 (pre-releases and development builds of 4.0.0 do not qualify).
- **AFNI**: on `PATH`. The orchestrator itself calls `3dinfo`, `3dTcat`, `3dresample`, `3dcalc`, `3dBrickStat`, `3dmaskave`, `3dTstat`, `3dmaskdump` and `afni --version`; fmri-first-level-proc calls its own set. At startup (except `--dry-run`) fmri-first-level-proc's tool preflight and version check run; AFNI must be at or above the minimum in its tool lock file. The installed, validated and minimum versions are logged.
- **AWS**: credentials from environment variables, a credentials file or an instance role. Required permissions: list the input and upload prefixes; read input objects (including completion manifests); read, write and delete objects under the upload prefix.
- **Disk**: one session's inputs and outputs at a time under `study.input_dir` and `study.output_dir`.

## Edge cases and known limitations

| Situation | Behavior |
|-----------|----------|
| A session is still being written by the producer | No `_complete.json` yet; the session is skipped and picked up by a later invocation |
| An invalid manifest in a session other than `--session` | Not read; only the selected session's manifest is validated |
| Task analyses with `include_motion_derivs: false` alongside `rest_conn` with `motion_deriv_degree: 2` | Every run's motion file carries one derivative set (12 columns), because the derivative count is a session-wide maximum |
| All runs of a task excluded | Analyses on that task are skipped (`no_usable_runs`); the session can still be complete |
| One analysis fails | Other analyses still run; the failure is recorded in the QC JSON |
| Two processes handle the same participant | Not supported; can produce `Stale sentinel present at upload start` |
| `upload_max_workers` outside 1 to 64 | Config validation error |
| Ctrl+C during a batch | Running participants finish, pending ones are cancelled, summary CSV written, exit 130 |
