# Input Specification

This document provides a complete reference for all inputs accepted by `ABCD_fmri_orchestrator_S3`. It covers every configuration field, input file format, validation rule, and output schema. For a high-level overview and usage guide, see [README.md](README.md).

## 1. Overview of Inputs

| Input | How It's Passed | Documentation |
|-------|----------------|---------------|
| Orchestrator config (YAML) | `--orchestrate_config` flag | Section 2 below |
| Proc template config (YAML) | `--proc_config` flag | Section 3 below; [fmri_first_level_proc INPUT_SPECIFICATION.md](https://github.com/tjkeding/fmri_first_level_proc/blob/main/INPUT_SPECIFICATION.md) |
| Subject ID | `--subj_id` flag | Section 4 below |
| Subject list (batch mode) | `--subject-list` flag | Section 4 below |
| fMRIPrep archive on S3 | Downloaded automatically | Section 5 below |
| Events files on S3 | Downloaded automatically | Section 5 below |
| fMRIPrep confounds TSV | Inside archive | Section 7 below |
| Task events TSV | From mmps_mproc on S3 | Section 8 below |

---

## 2. Orchestrator Config (YAML) — Complete Field Reference

The orchestrator config is a YAML file that defines study-level settings. See `example_orchestrator_config.yaml` for an annotated example.

Required top-level sections: `study`, `tasks`, `analyses`. Optional sections: `smoothing`, `qc`, `s3`.

### 2.1 `study` Block

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `fmriprep_dir` | str | Yes | — | Local root for fMRIPrep derivatives. Used as the base directory for extracted archives. Must be an absolute path. |
| `output_dir` | str | Yes | — | Root output directory. The orchestrator creates `sub-{ID}/ses-{session}A/` subdirectories inside. Must be an absolute path. |
| `space` | str | Yes | — | fMRIPrep template space label (e.g., `MNI152NLin2009cAsym`). Must exactly match the `space-` entity in fMRIPrep output filenames. |
| `TR` | float | Yes | — | Repetition time in seconds. Must be a positive number. Used for onset adjustment and clean time calculation. |
| `calc_n_motion_derivs` | int | No | `1` | Number of temporal derivative sets for motion regressors. Total motion columns = `6 * (1 + calc_n_motion_derivs)`. Must be a non-negative integer. `0` = base parameters only (6 columns), `1` = base + first derivatives (12 columns), `2` = base + first + second derivatives (18 columns). |

**Validation rules:**
- `fmriprep_dir`, `output_dir`, `space`, and `TR` must all be present and non-null
- `TR` must be a positive number (`int` or `float`)
- `calc_n_motion_derivs` must be a non-negative integer

### 2.2 `tasks` Block

A non-empty list of task definitions. Each entry defines a BIDS task entity. Runs are discovered dynamically from the extracted archive (not specified in config).

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `task_label` | str | Yes | — | BIDS task label (e.g., `nback`, `rest`). Must match the `task-` entity in fMRIPrep filenames. If the label starts with `rest` (case-insensitive), rest-mode behavior is triggered: no events files are expected, tissue signals are extracted, and `concatenate_runs` defaults to `false`. |
| `condition_column` | str | No | `trial_type` | Column name in the events TSV containing condition labels. Only used for non-rest tasks. |
| `conditions_exclude` | list of str or null | No | `null` | Conditions to drop from the events file before formatting timing. `null` = drop nothing. |
| `concatenate_runs` | bool | No | `true` for task, `false` for rest | Whether to concatenate runs before first-level analysis. When `true`, all runs are concatenated into a single timeseries; when `false`, each run is kept separate (required for rest_conn analyses). |
| `fix_nback_cues` | bool | No | `false` | When `true`, generic "cue"/"Cue" trial types in n-back events files are relabeled with the stimulus condition inferred from the subsequent block of trials. Only applicable to n-back tasks. |

**Validation rules:**
- `tasks` must be a non-empty list
- Each task must have a `task_label` field
- Task labels starting with `rest` (case-insensitive) trigger rest-mode behavior

### 2.3 `analyses` Block

A non-empty list of first-level analysis specifications. Each entry links to a matching analysis block (by `name`) in the proc template config.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | str | Yes | — | Analysis name. Must exactly match a `name` field in the proc template's `analyses` list. |
| `type` | str | Yes | — | Analysis type. Must be one of: `task_act`, `task_conn`, `rest_conn`. Must match the corresponding type in the proc template. |
| `task_label` | str | Yes | — | Which task this analysis operates on. Must match a `task_label` defined in the `tasks` section. |
| `fd_threshold` | float | Yes | — | Framewise displacement threshold in mm for this analysis. Must be a positive number. Different analyses can use different thresholds. |
| `post_id_out_pre` | str | Yes | — | Suffix appended to the session prefix to form `out_file_pre`. For subject `NDARABC123`, session `00`: `out_file_pre = "sub-NDARABC123_ses-00A_{post_id_out_pre}"`. |
| `post_id_extract_pre` | str | Conditional | — | Required when the matching proc template analysis has an `extraction` block. Forms `extraction.extract_out_file_pre` using the same prefix pattern. |
| `post_id_conn_pre` | str | Conditional | — | Required for `task_conn` and `rest_conn` types. Forms `connectivity.conn_out_file_pre` using the same prefix pattern. |

**Prefix construction:**

For subject `NDARABC123`, session `00`, with `post_id_out_pre: "nback"`:
- `out_file_pre` = `sub-NDARABC123_ses-00A_nback`
- `extract_out_file_pre` = `sub-NDARABC123_ses-00A_nback_Shen368` (if `post_id_extract_pre: "nback_Shen368"`)
- `conn_out_file_pre` = `sub-NDARABC123_ses-00A_nback_Shen368` (if `post_id_conn_pre: "nback_Shen368"`)

**Deprecated fields (ignored by orchestrator, should be in proc template):**
`hrf_model`, `custom_hrf`, `include_motion_derivs`, `cond_labels`, `cond_beta_labels`, `contrasts`, `bandpass`, `motion_deriv_degree`, `template_path`, `average_type`, `extraction`, `connectivity`, `extract_out_file_suffix`, `conn_out_file_suffix`

**Validation rules:**
- `analyses` must be a non-empty list
- Each analysis must have `name`, `type`, `task_label`, `fd_threshold`, and `post_id_out_pre`
- `type` must be one of `{task_act, task_conn, rest_conn}`
- `task_label` must reference an existing entry in the `tasks` section
- `fd_threshold` must be a positive number
- `post_id_conn_pre` is required for `task_conn` and `rest_conn` types
- `post_id_extract_pre` is required when the proc template has an `extraction` block for this analysis

### 2.4 `smoothing` Block (Optional)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | bool | No | `false` | Whether to apply spatial smoothing. |
| `method` | str | Conditional | — | Required if `enabled: true`. Must be `"3dmerge"` (Gaussian blur) or `"3dBlurToFWHM"` (iterative blur to target FWHM within mask). |
| `fwhm` | float | Conditional | — | Required if `enabled: true`. Target FWHM in mm. Must be a positive number. |

**Validation rules:**
- If `enabled: true`, both `method` and `fwhm` must be present
- `method` must be `"3dmerge"` or `"3dBlurToFWHM"`
- `fwhm` must be a positive number

### 2.5 `qc` Block (Optional)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `preproc.enabled` | bool | `false` | Enable preprocessing QC (per-run metrics) |
| `preproc.tsnr` | bool | `true` | Compute temporal signal-to-noise ratio |
| `preproc.carpet_plots` | bool | `true` | Generate carpet plots (FD/DVARS traces + voxel heatmap) |
| `preproc.registration_quality` | bool | `true` | Compute Dice coefficient between functional and anatomical brain masks |
| `first_level.enabled` | bool | `false` | Enable first-level analysis QC |

If the `qc` section is omitted entirely, both preproc and first_level QC default to disabled.

### 2.6 `s3` Block (Optional)

| Field | Type | Required | Default | Constraints |
|-------|------|----------|---------|-------------|
| `enabled` | bool | No | `false` | — |
| `bucket` | str | If enabled | — | Bucket name only. Must NOT start with `s3://`. |
| `fmriprep_s3_prefix` | str | If enabled | — | S3 key prefix for fMRIPrep archives. No leading or trailing `/`. |
| `mmps_mproc_s3_prefix` | str | If enabled | — | S3 key prefix for events files. No leading or trailing `/`. |
| `upload_prefix` | str | If enabled | — | S3 key prefix for uploading results. No leading or trailing `/`. |
| `cleanup_after_upload` | bool | No | `true` | If true, delete downloaded/extracted files after successful upload. |
| `available_sessions` | list of str | If enabled | `[]` | Pool of session codes to probe (e.g., `["00", "02", "04", "06"]`). Must be a non-empty list of strings when S3 is enabled. |

**Validation rules:**
- If `enabled: true`, `bucket`, `fmriprep_s3_prefix`, `mmps_mproc_s3_prefix`, and `upload_prefix` are all required and must be non-null
- `bucket` must not start with `s3://`
- All prefix fields must not start or end with `/`
- `available_sessions` must be a non-empty list of strings

---

## 3. Proc Template Config (fmri_first_level_proc)

The proc template config (passed via `--proc_config`) is a standard `fmri_first_level_proc` config file. The orchestrator uses it as a template: analysis-level settings are preserved verbatim, and only subject-specific fields are overridden.

### Fields Overridden by the Orchestrator

| Field | What It's Set To |
|-------|-----------------|
| `analyses[].out_dir` | `{output_dir}/sub-{ID}/ses-{session}A/first_level_out/{analysis_name}` |
| `analyses[].out_file_pre` | `sub-{ID}_ses-{session}A_{post_id_out_pre}` |
| `analyses[].paths.*` | Resolved from preprocessed files (see below) |
| `analyses[].extraction.extract_out_file_pre` | `sub-{ID}_ses-{session}A_{post_id_extract_pre}` (if extraction block exists) |
| `analyses[].connectivity.conn_out_file_pre` | `sub-{ID}_ses-{session}A_{post_id_conn_pre}` (if connectivity block exists) |

**Path fields set per analysis type:**

For `task_act` and `task_conn`:
- `paths.scan_path` — concatenated BOLD
- `paths.task_timing_path` — concatenated timing CSV
- `paths.motion_path` — concatenated motion regressors
- `paths.censor_path` — concatenated censor file (for this analysis's FD threshold)

For `rest_conn`:
- `paths.scan_paths` — list of per-run BOLDs
- `paths.motion_paths` — list of per-run motion regressors
- `paths.censor_paths` — list of per-run censor files (for this analysis's FD threshold)
- `paths.CSF_paths` — list of per-run CSF signal files
- `paths.WM_paths` — list of per-run white matter signal files
- `paths.GS_paths` — list of per-run global signal files (or null)

### Everything Else

All other fields in the proc template are passed through verbatim to the generated first-level config. This includes:
- HRF model settings
- Contrast definitions
- Bandpass filter parameters
- Motion derivative degree
- Atlas/template paths
- Extraction parameters (ROI definitions, average type, etc.)
- Connectivity parameters (methods, thresholds, etc.)

### Cross-Validation Rules (`validate_proc_template()`)

The orchestrator validates the proc template against the orchestrator config at startup:

1. Proc template must have `analyses` as a non-empty list
2. Every proc template analysis block must have a `name` field
3. Every orchestrator analysis `name` must have a matching entry in the proc template
4. `type` fields must match for each name-matched pair
5. `post_id_out_pre` must be present in every orchestrator analysis
6. `post_id_extract_pre` is required when the matching proc template analysis has an `extraction` block
7. `post_id_conn_pre` is required for `task_conn` and `rest_conn` types
8. Proc template analyses not referenced by the orchestrator config produce a warning and are removed from the generated config

For the full proc template specification, see the upstream [fmri_first_level_proc INPUT_SPECIFICATION.md](https://github.com/tjkeding/fmri_first_level_proc/blob/main/INPUT_SPECIFICATION.md).

---

## 4. Subject ID and Subject List

### Subject ID

Any string passed via `--subj_id`. For the ABCD study, the typical format is `NDAR` followed by 8 alphanumeric characters (e.g., `NDARABC12345`). The orchestrator does not enforce a specific format — the ID is used as-is in file path construction.

### Subject List (Batch Mode)

A plain-text file passed via `--subject-list` to `run_orchestrator.py`:

- One subject ID per line
- Blank lines are ignored
- Lines starting with `#` are treated as comments
- Duplicate IDs produce a warning and are skipped
- Leading/trailing whitespace is stripped

Example:
```
# ABCD wave 1 subjects
NDARABC12345
NDARDEF67890

# Reprocessing these from wave 2
NDARGHI11111
NDARJKL22222
```

---

## 5. S3 Data Requirements

### fMRIPrep Archive

**S3 key pattern:**
```
{fmriprep_s3_prefix}/sub-{ID}/ses-{session}A/sub-{ID}_ses-{session}A_fmriprep-output.tar.gz
```

Example:
```
derivatives/fmriprep/sub-NDARABC123/ses-00A/sub-NDARABC123_ses-00A_fmriprep-output.tar.gz
```

Session discovery uses S3 HEAD requests against this exact key pattern for each code in `available_sessions`.

### Events Files

**S3 key pattern:**
```
{mmps_mproc_s3_prefix}/sub-{ID}/ses-{session}A/func/sub-{ID}_ses-{session}A_task-{task}_run-0{N}_events.tsv
```

Where `N` ranges from 1 to 9. Run numbers use the format `run-0{N}` (zero-padded single digit). The orchestrator probes sequentially starting from run 1 and stops at the first missing run number.

Example:
```
mmps_mproc/sub-NDARABC123/ses-00A/func/sub-NDARABC123_ses-00A_task-nback_run-01_events.tsv
mmps_mproc/sub-NDARABC123/ses-00A/func/sub-NDARABC123_ses-00A_task-nback_run-02_events.tsv
```

Events files are only downloaded for non-rest tasks.

### Upload Target

**S3 key pattern:**
```
{upload_prefix}/sub-{ID}/ses-{session}A/first_level_out.tar.gz
```

---

## 6. fMRIPrep Archive Contents

### Required Files Per Task/Run

| File Pattern | Description |
|-------------|-------------|
| `func/sub-{ID}_ses-{session}A_task-{task}_run-{run}_space-{space}_desc-preproc_bold.nii.gz` | Preprocessed BOLD timeseries |
| `func/sub-{ID}_ses-{session}A_task-{task}_run-{run}_desc-confounds_timeseries.tsv` | Confounds file (motion, FD, tissue signals, NSS) |
| `func/sub-{ID}_ses-{session}A_task-{task}_run-{run}_space-{space}_desc-brain_mask.nii.gz` | Brain mask for masking BOLD data |

All three files must be present for a run to be processed. Missing files cause the run to be skipped with a warning.

The `space` entity in filenames must exactly match `study.space` in the orchestrator config (e.g., `MNI152NLin2009cAsym`).

### Optional Files

| File Pattern | Description | Used For |
|-------------|-------------|----------|
| `anat/sub-{ID}_ses-{session}A_run-01_space-{space}_desc-brain_mask.nii.gz` | Anatomical brain mask | Registration QC (Dice coefficient) |

---

## 7. fMRIPrep Confounds TSV

The confounds timeseries file output by fMRIPrep. Every row corresponds to one TR.

| Column | Used For | Required | Fallback |
|--------|----------|----------|----------|
| `trans_x` | Motion regressor (translation X) | Yes | Error if missing |
| `trans_y` | Motion regressor (translation Y) | Yes | Error if missing |
| `trans_z` | Motion regressor (translation Z) | Yes | Error if missing |
| `rot_x` | Motion regressor (rotation X) | Yes | Error if missing |
| `rot_y` | Motion regressor (rotation Y) | Yes | Error if missing |
| `rot_z` | Motion regressor (rotation Z) | Yes | Error if missing |
| `trans_x_derivative1` | First temporal derivative of trans_x | No | Computed numerically via finite differences |
| `trans_y_derivative1` | First temporal derivative of trans_y | No | Computed numerically via finite differences |
| `trans_z_derivative1` | First temporal derivative of trans_z | No | Computed numerically via finite differences |
| `rot_x_derivative1` | First temporal derivative of rot_x | No | Computed numerically via finite differences |
| `rot_y_derivative1` | First temporal derivative of rot_y | No | Computed numerically via finite differences |
| `rot_z_derivative1` | First temporal derivative of rot_z | No | Computed numerically via finite differences |
| `framewise_displacement` | Censor file generation, motion QC | Yes | Error if missing |
| `dvars` | QC carpet plots, DVARS metrics | No | Omitted from QC if missing |
| `non_steady_state_outlier_*` | Detecting non-steady-state TRs | No | 0 TRs removed if no columns present |
| `csf` | CSF nuisance regressor (rest only) | Conditional | Error if missing for rest tasks |
| `white_matter` | WM nuisance regressor (rest only) | Conditional | Error if missing for rest tasks |
| `global_signal` | Global signal regressor (rest only) | Conditional | Error if missing for rest tasks |

**Notes:**
- The 6 base motion columns (`trans_x/y/z`, `rot_x/y/z`) must not be entirely NaN
- `framewise_displacement` first row is typically NaN and is always treated as "include" (censor = 1)
- `non_steady_state_outlier_*` columns: each column corresponds to one NSS TR (e.g., 3 columns = 3 TRs to remove)
- Derivative columns (e.g., `trans_x_derivative1`) are used if present; otherwise the orchestrator computes derivatives numerically
- NaN values in motion derivatives are replaced with 0.0

---

## 8. Task Events TSV

BIDS-format task events files, downloaded from the mmps_mproc location on S3.

### Required Columns

| Column | Type | Description |
|--------|------|-------------|
| `onset` | float | Event onset time in seconds from scan start |
| `duration` | float | Event duration in seconds |
| `{condition_column}` | str | Condition label (column name set by `tasks[].condition_column`, default: `trial_type`) |

### N-Back Specific Behavior

When `fix_nback_cues: true` is set for a task, the orchestrator relabels generic "cue" (or "Cue") trial types. The ABCD n-back task has this structure:

```
onset     duration  trial_type
0.000     6.400     "dummy"
6.410     2.890     "cue"          ← relabeled to "posface"
9.410     1.900     "0_back_posface"
11.910    1.900     "0_back_posface"
...
34.470    2.890     "cue"          ← relabeled to "place"
37.470    1.900     "2_back_place"
```

The relabeling algorithm:
1. For each row where the condition is "cue" or "Cue"
2. Look ahead to find the next row matching the pattern `{digit}_back_{condition}`
3. Extract the `{condition}` portion (e.g., "posface", "place", "neutface", "negface")
4. Replace the cue's trial type with the extracted condition

The n-back level prefix (`0_back_` / `2_back_`) is stripped from cue labels because the cue is a passive viewing event — the n-back manipulation only applies to subsequent recall trials.

### Onset Adjustment

After non-steady-state TR removal, event onsets are adjusted:
- `adjusted_onset = original_onset - (n_remove * TR)`
- Events with `adjusted_onset < 0` are dropped with a warning

### Condition Filtering

- `conditions_exclude: ["dummy"]` — removes rows where the condition matches any value in the list
- `conditions_exclude: null` — no filtering (all conditions kept)

---

## 9. Validation Rules Summary

All validation checks performed at startup, with error message patterns and source functions.

### Orchestrator Config Validation (`load_orchestrator_config()`)

| Check | Error Message Pattern | Fatal? |
|-------|----------------------|--------|
| Config file exists | `Orchestrator config not found: {path}` | Yes |
| Valid YAML | `YAML parse error in {path}: {error}` | Yes |
| Non-empty config | `Config file is empty: {path}` | Yes |
| Required sections present | `Config missing required section '{section}'.` | Yes |
| Required study keys present | `Config study section missing required key '{key}'.` | Yes |
| TR is positive number | `study.TR must be a positive number, got: {value}` | Yes |
| Tasks is non-empty list | `'tasks' must be a non-empty list.` | Yes |
| Each task has task_label | `tasks[{i}] missing required key 'task_label'.` | Yes |
| Analyses is non-empty list | `'analyses' must be a non-empty list.` | Yes |
| Analysis type valid | `[{name}] Invalid type '{type}'. Must be one of {valid_types}.` | Yes |
| Analysis task_label exists | `[{name}] task_label '{label}' not defined in tasks section.` | Yes |
| Analysis has post_id_out_pre | `[{name}] Missing required key 'post_id_out_pre'.` | Yes |
| Analysis has fd_threshold | `[{name}] Missing required key 'fd_threshold'.` | Yes |
| fd_threshold is positive | `[{name}] fd_threshold must be a positive number, got: {value}` | Yes |
| Connectivity types have post_id_conn_pre | `[{name}] {type} analysis requires 'post_id_conn_pre'.` | Yes |
| Smoothing method valid | `smoothing.method must be '3dmerge' or '3dBlurToFWHM', got '{method}'.` | Yes |
| Smoothing FWHM positive | `smoothing.fwhm must be a positive number, got: {value}` | Yes |
| S3 required fields present | `s3.enabled is true but required field 's3.{field}' is null or missing.` | Yes |
| S3 bucket no prefix | `s3.bucket must be the bucket name only (no 's3://' prefix), got: {value}` | Yes |
| S3 prefix no slashes | `s3.{field} must not have a leading or trailing '/', got: {value}` | Yes |
| S3 sessions non-empty list | `s3.available_sessions must be a non-empty list of session codes` | Yes |
| S3 sessions are strings | `s3.available_sessions entries must be strings, got: {value}` | Yes |
| calc_n_motion_derivs valid | `study.calc_n_motion_derivs must be a non-negative integer, got: {value}` | Yes |
| Deprecated fields in analysis | `[{name}] Field '{field}' in orchestrator analysis block is deprecated.` | Warning |

### Proc Template Cross-Validation (`validate_proc_template()`)

| Check | Error Message Pattern | Fatal? |
|-------|----------------------|--------|
| Template not null | `Proc template config is empty (null).` | Yes |
| Template has analyses list | `Proc template must have 'analyses' as a non-empty list.` | Yes |
| Template analyses have names | `Proc template has an analysis block without a 'name' field.` | Yes |
| Name match exists | `Orchestrator analysis '{name}' has no matching entry in proc template.` | Yes |
| Type match | `Type mismatch for analysis '{name}': orchestrator has '{type1}', proc template has '{type2}'.` | Yes |
| post_id_out_pre present | `[{name}] Missing required key 'post_id_out_pre'.` | Yes |
| post_id_extract_pre when needed | `[{name}] Proc template has an 'extraction' block but orchestrator analysis is missing 'post_id_extract_pre'.` | Yes |
| post_id_conn_pre for conn types | `[{name}] {type} analysis requires 'post_id_conn_pre'.` | Yes |
| Unreferenced template analyses | `Proc template contains {n} analysis block(s) not referenced by orchestrator config` | Warning |

---

## 10. Output File Schemas

### 10.1 Preprocessing QC JSON

One file per task/run: `{sub_id}_{run_label}_preproc_qc.json`

```json
{
  "sub_id": "sub-NDARABC123",
  "session": "00",
  "task": "nback",
  "run": 1,
  "non_steady_state_trs": 3,
  "motion": {
    "mean_fd": 0.182,
    "max_fd": 1.456,
    "median_fd": 0.134
  },
  "censor": {
    "fd_threshold_mm": 0.9,
    "n_total_trs": 380,
    "n_censored_trs": 12,
    "n_clean_trs": 368,
    "pct_censored": 3.16,
    "clean_time_seconds": 294.4
  },
  "dvars": {
    "mean": 25.34,
    "max": 89.12
  },
  "tsnr": {
    "median_brain": 45.67
  },
  "brain_mask": {
    "n_voxels": 168432,
    "volume_mm3": 1347456.0
  },
  "carpet_plot_path": "/path/to/qc/preproc/sub-NDARABC123_ses-00A_task-nback_run-1_carpet.png",
  "registration": {
    "dice": 0.912,
    "anat_mask": "/path/to/anat/mask.nii.gz"
  }
}
```

**Key fields for group-level exclusion decisions:**
- `censor.clean_time_seconds` — total clean (uncensored) scan time
- `censor.pct_censored` — percentage of volumes exceeding FD threshold
- `motion.mean_fd` — average framewise displacement

### 10.2 First-Level QC JSON

One file per analysis: `{sub_id}_{ses_label}_{analysis_name}_first_level_qc.json`

```json
{
  "sub_id": "sub-NDARABC123",
  "session": "ses-00A",
  "analysis_name": "nback_act",
  "type": "task_act",
  "pct_censored": 3.16,
  "completed_successfully": true,
  "error": null,
  "n_nifti_outputs": 4,
  "output_files": [
    "sub-NDARABC123_ses-00A_nback_stats.nii.gz",
    "sub-NDARABC123_ses-00A_nback_bucket.nii.gz",
    "sub-NDARABC123_ses-00A_nback_fitts.nii.gz",
    "sub-NDARABC123_ses-00A_nback_errts.nii.gz"
  ]
}
```

For failed analyses:
```json
{
  "sub_id": "sub-NDARABC123",
  "session": "ses-00A",
  "analysis_name": "nback_act",
  "type": "task_act",
  "pct_censored": 78.42,
  "completed_successfully": false,
  "error": "AFNI exited with code 1",
  "n_nifti_outputs": 0,
  "output_files": []
}
```

### 10.3 Batch Summary CSV

Written by `run_orchestrator.py` after all subjects complete (or on Ctrl+C).

| Column | Type | Description |
|--------|------|-------------|
| `sub_id` | str | Subject ID |
| `status` | str | One of: `success`, `failed`, `cancelled` |
| `runtime_seconds` | float | Wall-clock time for this subject (rounded to 2 decimals) |
| `error_message` | str | Error summary (last stderr line for failures, empty for success) |
| `log_file` | str | Path to the per-subject log file |

Default filename: `{log_dir}/run_summary_{YYYYMMDD_HHMMSS}.csv`

### 10.4 Internal Data Structure: `processed_files`

This dict is not written to disk but is the central data structure passing information between pipeline steps. Documented here for developers.

**Task (concatenated) format:**
```python
processed_files[task_label] = {
    "bold": str,        # Path to concatenated BOLD
    "motion": str,      # Path to concatenated motion regressors
    "censor": {         # Dict: fd_threshold (float) -> path (str)
        0.9: str,       # Path to concatenated censor file at this threshold
    },
    "timing": str,      # Path to concatenated timing CSV, or None if no events
}
```

**Rest (per-run) format:**
```python
processed_files[task_label] = {
    "bolds": [str, ...],       # List of per-run BOLD paths
    "motions": [str, ...],     # List of per-run motion regressor paths
    "censors": {               # Dict: fd_threshold (float) -> list of paths
        0.4: [str, ...],       # Per-run censor file paths at this threshold
    },
    "csf": [str, ...],        # List of per-run CSF signal paths
    "wm": [str, ...],         # List of per-run WM signal paths
    "gs": [str, ...] or None, # List of per-run global signal paths, or None
}
```

Key differences between the two formats:
- Task format uses singular keys (`bold`, `motion`, `censor`, `timing`) with single paths
- Rest format uses plural keys (`bolds`, `motions`, `censors`, `csf`, `wm`, `gs`) with lists of paths
- `censor` (task) maps threshold -> single path; `censors` (rest) maps threshold -> list of paths
- Only rest format has tissue signal fields (`csf`, `wm`, `gs`)
- Only task format has `timing`
