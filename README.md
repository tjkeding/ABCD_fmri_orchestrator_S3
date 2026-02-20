# ABCD_fmri_orchestrator_S3

A session-centric orchestrator for first-level fMRI processing on AWS EC2/S3, purpose-built for the Adolescent Brain and Cognitive Development (ABCD) study (~11,000 subjects, up to 4 timepoints, 2 functional imaging modalities). It wraps [fmri_first_level_proc](https://github.com/tjkeding/fmri_first_level_proc) and handles the full lifecycle of downloading fMRIPrep derivatives from S3, preprocessing, running first-level analyses, and uploading results — one session at a time to minimize disk usage on EC2 instances.

Two entry points are provided:
- **`orchestrate_first_level.py`** — processes a single subject (all available sessions)
- **`run_orchestrator.py`** — parallel batch runner for processing many subjects concurrently

## Architecture

```
run_orchestrator.py
│
│  Reads subject list, launches ThreadPoolExecutor
│  Each thread runs orchestrate_first_level.py as a subprocess
│
└──► orchestrate_first_level.py  [per subject]
     │
     │  Step 0: Discover available sessions (S3 HEAD requests)
     │
     └──► _process_session()  [per session]
          │
          ├── Step 1:  Download session data from S3 (archive + events)
          ├── Step 2:  Extract fMRIPrep archive
          ├── Step 3:  Discover files per task (glob func/ directory)
          │
          │   ┌── Per task, per run ──────────────────────────────┐
          ├── │ Step 4:  Decompress if needed (.bz2/.gz/.tar.gz) │
          │   │ Step 5:  Apply brain mask (3dcalc)               │
          │   │ Step 6:  Preprocessing QC (tSNR, carpet plots)   │
          │   │ Step 7:  Detect & remove non-steady-state TRs    │
          │   │ Step 8:  Generate censor files (per FD threshold) │
          │   │ Step 9:  Extract motion regressors                │
          │   │ Step 10: Extract tissue signals (rest only)       │
          │   │ Step 11: Format task timing (task only)           │
          │   └──────────────────────────────────────────────────┘
          │
          ├── Step 12: Concatenate runs (task) or collect per-run (rest)
          │             + optional spatial smoothing
          ├── Step 13: Build first-level config & run analyses
          └── Step 14: Compress outputs → upload to S3 → cleanup
```

## Prerequisites and Installation

### Software Requirements

- **Python** >= 3.8
- **AFNI** — must be installed and on `PATH` ([installation guide](https://afni.nimh.nih.gov/pub/dist/doc/htmldoc/background_install/main_toc.html))
- **AWS credentials** — configured via environment variables, `~/.aws/credentials`, or EC2 instance role
- **conda** or **pip** (conda recommended)

### Installation

```bash
# Clone the repository
git clone https://github.com/tjkeding/ABCD_fmri_orchestrator_S3.git
cd ABCD_fmri_orchestration_S3

# Create and activate the conda environment
conda env create -f environment.yaml
conda activate ABCD_fmri_orchestrator_S3

# Verify AFNI is available
3dinfo -ver
```

The conda environment installs `fmri_first_level_proc` directly from GitHub via pip. See `environment.yaml` for the full dependency list.

## Quick Start

### Single Subject

```bash
# Full run
python orchestrate_first_level.py \
  --orchestrate_config study.yaml \
  --proc_config example_config.yaml \
  --subj_id NDARABC123 \
  --log-file logs/NDARABC123.log

# Dry run — validate config and print processing plan
python orchestrate_first_level.py \
  --orchestrate_config study.yaml \
  --proc_config example_config.yaml \
  --subj_id NDARABC123 \
  --dry-run
```

### Batch Processing

```bash
# Full run, 8 parallel workers
python run_orchestrator.py \
  --orchestrate_config study.yaml \
  --proc_config example_config.yaml \
  --subject-list subjects.txt \
  --n-jobs 8 \
  --log-dir logs/

# Dry run
python run_orchestrator.py \
  --orchestrate_config study.yaml \
  --proc_config example_config.yaml \
  --subject-list subjects.txt \
  --n-jobs 2 \
  --log-dir logs/ \
  --dry-run
```

## CLI Reference

### `orchestrate_first_level.py`

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--orchestrate_config` | Yes | — | Path to the orchestrator YAML config file |
| `--proc_config` | Yes | — | Path to the fmri_first_level_proc YAML template config |
| `--subj_id` | Yes | — | Participant ID (e.g. `NDARABC123`) |
| `--session` | No | `None` | Process only this session code (e.g. `00`). Useful for reprocessing a failed session. |
| `--dry-run` | No | `False` | Validate config and print plan without executing |
| `--log-file` | No | `None` | Path to a log file (logs to stdout if not set) |
| `--skip-qc` | No | `False` | Skip all QC computations |
| `--skip-first-level` | No | `False` | Run preprocessing only, skip first-level analyses |

### `run_orchestrator.py`

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--orchestrate_config` | Yes | — | Path to the orchestrator YAML config file |
| `--proc_config` | Yes | — | Path to the fmri_first_level_proc YAML template config |
| `--subject-list` | Yes | — | Path to plain-text subject list file |
| `--n-jobs` | No | `1` | Number of subjects to process in parallel |
| `--log-dir` | Yes | — | Directory for per-subject log files |
| `--dry-run` | No | `False` | Pass `--dry-run` to each subprocess |
| `--skip-qc` | No | `False` | Pass `--skip-qc` to each subprocess |
| `--skip-first-level` | No | `False` | Pass `--skip-first-level` to each subprocess |
| `--session` | No | `None` | Pass `--session` to each subprocess |
| `--summary-file` | No | `{log_dir}/run_summary_{timestamp}.csv` | Path to output summary CSV |

### Exit Codes (Batch Runner)

| Code | Meaning |
|------|---------|
| `0` | All subjects succeeded |
| `1` | One or more subjects failed |
| `130` | Interrupted by Ctrl+C |

### Subject List Format

Plain text, one subject ID per line. Blank lines and lines starting with `#` are ignored. Duplicate IDs produce a warning and are skipped.

```
# ABCD subjects — wave 1
NDARABC12345
NDARDEF67890

# Wave 2
NDARGHI11111
```

## Pipeline Steps Explained

### Step 0: Discover Available Sessions

When S3 is enabled, the orchestrator probes S3 for each session code in `s3.available_sessions` using HEAD requests against the expected archive key. Only sessions where an archive exists are processed. When S3 is disabled (local mode), the `available_sessions` list is used directly. The `--session` flag can further filter to a single session.

### Step 1: Download Session Data from S3

Downloads two types of files: (1) the fMRIPrep archive (`.tar.gz` containing BOLD, confounds, masks, and anatomical files) and (2) task events files from the `mmps_mproc` prefix. Events files are probed for runs 1–9 per non-rest task; download stops at the first missing run number. Already-present local files are skipped (idempotent).

### Step 2: Extract fMRIPrep Archive

Extracts the `.tar.gz` archive with safety checks: disk space verification (requires 10x archive size free) and path traversal protection (rejects tar members that resolve outside the target directory). The extractor searches for the `func/` subdirectory, which may be nested inside `sub-{ID}/ses-{session}A/` within the archive.

### Step 3: Discover Files Per Task

Globs the extracted `func/` directory for BOLD files matching each task label and template space. For each discovered run, the corresponding confounds TSV, brain mask, and events file (matched by run number, not position) must all exist for the run to be included. Missing files produce warnings and the run is skipped. An anatomical brain mask is also discovered for optional registration QC.

### Step 4: Decompress If Needed

Handles files that may exist in compressed form (`.bz2`, `.gz`, or inside a `.tar.gz`). Most fMRIPrep outputs are `.nii.gz` and pass through unchanged. This step ensures confounds TSVs and other files are available in their expected format.

### Step 5: Apply Brain Mask

Uses AFNI `3dcalc` with expression `a*step(b)` to zero out non-brain voxels. Produces `{prefix}_masked.nii.gz`. Idempotent — skips if output already exists.

### Step 6: Preprocessing QC

Computes per-run quality metrics: motion statistics (mean/max/median FD), censor counts, DVARS, brain mask coverage, and optionally tSNR (median within-brain temporal signal-to-noise), carpet plots (FD + DVARS traces over voxel-by-time heatmap), and registration quality (Dice coefficient between functional and anatomical brain masks). Metrics are saved as JSON for group-level aggregation. Skipped if `--skip-qc` is set or `qc.preproc.enabled` is false.

### Step 7: Detect and Remove Non-Steady-State TRs

Counts `non_steady_state_outlier_*` columns in the fMRIPrep confounds file to determine how many initial TRs to remove. Uses AFNI `3dTcat` to trim the BOLD timeseries. If no non-steady-state TRs are detected, the file passes through unchanged.

### Step 8: Generate Censor Files

Creates binary censor vectors (1 = include, 0 = exclude) based on framewise displacement. One censor file is generated per unique FD threshold needed by the analyses referencing this task. For example, if two analyses use the same task but different FD thresholds (0.4 and 0.9), two censor files are produced. The first volume (NaN FD) is always included. Warnings are issued at >25% and >50% censored, but **no motion gating is applied** — the pipeline always attempts first-level analysis, leaving exclusion decisions to post-hoc group-level analysis.

### Step 9: Extract Motion Regressors

Extracts the 6 base motion parameters (trans_x/y/z, rot_x/y/z) from the confounds TSV. Temporal derivatives are included based on `study.calc_n_motion_derivs`: fMRIPrep-computed derivatives are used if available, otherwise computed numerically via finite differences. Total output columns = `6 * (1 + calc_n_motion_derivs)`. Remaining NaN values (e.g., first-row derivatives) are replaced with 0.0.

### Step 10: Extract Tissue Signals (Rest Only)

For resting-state tasks, extracts CSF, white matter, and global signal timeseries from the confounds TSV for use as nuisance regressors in rest_conn analyses.

### Step 11: Format Task Timing (Task Only)

Converts BIDS events TSVs to the first-level timing CSV format (CONDITION, ONSET, DURATION). Adjusts onsets for removed non-steady-state TRs and drops events that fall before time 0 after adjustment. For n-back tasks with `fix_nback_cues: true`, generic "cue" trial types are relabeled with the stimulus condition inferred from the subsequent block of trials (e.g., "posface", "place").

### Step 12: Concatenate Runs

For tasks with `concatenate_runs: true` (default for non-rest tasks): concatenates BOLD files (AFNI `3dTcat`), motion regressors, censor vectors, and task timing (with onset adjustment for cumulative run lengths). For single-run tasks, files are copied rather than concatenated. For rest tasks (`concatenate_runs: false`): per-run files are collected as lists. Optional spatial smoothing is applied after concatenation (or per-run for rest).

### Step 13: Build Config and Run First-Level Analyses

Deep-copies the proc template config and overrides only subject-specific fields (paths, output directories, session-aware prefixes). The orchestrator changes the working directory to the session output directory before running AFNI to prevent `3dDeconvolve.err` file collisions between concurrent sessions. Each analysis runs independently — if one fails, others continue. First-level QC is computed per analysis (completion check, censor proportion, error tracking).

### Step 14: Compress, Upload, Cleanup

When S3 is enabled: (a) compresses `first_level_out/` into a `.tar.gz` archive using atomic writes (temp file + rename), (b) uploads to S3 with size verification, (c) cleans up downloaded and extracted files if `s3.cleanup_after_upload` is true. Cleanup also runs on session failure to free disk space.

## Output Directory Structure

```
{output_dir}/sub-{ID}/ses-{session}A/
├── preproc/                                              # Per-run intermediate files
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_masked.nii.gz
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_trimmed.nii.gz
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_motion.1D
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_censor_fd{threshold}.1D
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_timing.csv
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_events_fixed.tsv  (nback only)
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_csf.1D            (rest only)
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_wm.1D             (rest only)
│   └── sub-{ID}_ses-{session}A_task-{task}_run-{N}_gs.1D             (rest only)
│
├── concat/                                               # Concatenated files (task only)
│   ├── sub-{ID}_ses-{session}A_task-{task}_concat_bold.nii.gz
│   ├── sub-{ID}_ses-{session}A_task-{task}_concat_motion.1D
│   ├── sub-{ID}_ses-{session}A_task-{task}_concat_censor_fd{threshold}.1D
│   └── sub-{ID}_ses-{session}A_task-{task}_concat_timing.csv
│
├── first_level_out/                                      # Analysis outputs (uploaded to S3)
│   ├── {analysis_name}/
│   │   └── (analysis-specific outputs: stat buckets, beta series, etc.)
│   └── ...
│
├── qc/
│   ├── preproc/                                          # Per-run QC JSONs + carpet plots
│   │   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_preproc_qc.json
│   │   └── sub-{ID}_ses-{session}A_task-{task}_run-{N}_carpet.png
│   └── first_level/                                      # Per-analysis QC JSONs
│       └── sub-{ID}_ses-{session}A_{analysis_name}_first_level_qc.json
│
├── sub-{ID}_ses-{session}A_first_level_config.yaml       # Generated proc config
└── first_level_out.tar.gz                                # Compressed archive for S3 upload
```

Only `first_level_out/` is included in the uploaded S3 archive. Preprocessing intermediates, QC files, and the generated config remain on the local filesystem.

## Quality Control

### Preprocessing QC

Per-run JSON files containing:

- **Motion**: mean, max, and median framewise displacement
- **Censor**: total TRs, censored TRs, clean TRs, percent censored, clean time in seconds (based on the minimum FD threshold for the task)
- **DVARS**: mean and max
- **tSNR**: median within-brain temporal signal-to-noise ratio (optional, via `qc.preproc.tsnr`)
- **Brain mask**: voxel count and volume in mm^3
- **Carpet plots**: FD + DVARS traces above voxel-by-time heatmap (optional, via `qc.preproc.carpet_plots`)
- **Registration quality**: Dice coefficient between functional and anatomical brain masks (optional, via `qc.preproc.registration_quality`)

### First-Level QC

Per-analysis JSON files containing:

- **completed_successfully**: whether at least one non-empty NIfTI output was produced and no error was raised
- **pct_censored**: percentage of volumes censored at this analysis's FD threshold
- **error**: error message if the analysis failed, else null
- **n_nifti_outputs**: count of NIfTI files produced
- **output_files**: list of all files in the analysis output directory

### Group-Level Aggregation

QC JSONs are designed for easy aggregation with pandas:

```python
import json, glob
import pandas as pd

# Preprocessing QC
qc_files = glob.glob("*/ses-*/qc/preproc/*_preproc_qc.json")
df = pd.json_normalize([json.load(open(f)) for f in qc_files])

# Key fields for exclusion decisions:
#   censor.clean_time_seconds  — minimum clean data duration
#   censor.pct_censored        — maximum censoring rate
#   motion.mean_fd             — mean head motion
```

## S3 Data Structure

### Source Data Patterns

| Data | S3 Key Pattern |
|------|----------------|
| fMRIPrep archive | `{fmriprep_s3_prefix}/sub-{ID}/ses-{session}A/sub-{ID}_ses-{session}A_fmriprep-output.tar.gz` |
| Events files | `{mmps_mproc_s3_prefix}/sub-{ID}/ses-{session}A/func/sub-{ID}_ses-{session}A_task-{task}_run-0{N}_events.tsv` |
| Upload target | `{upload_prefix}/sub-{ID}/ses-{session}A/first_level_out.tar.gz` |

Session labels follow the format `ses-{code}A` where `{code}` is a session code from `s3.available_sessions` (e.g., `ses-00A`, `ses-02A`, `ses-04A`, `ses-06A`). The "A" suffix is a study convention for the ABCD dataset.

### Config Fields Controlling S3 Paths

- `s3.bucket` — S3 bucket name (e.g., `abcd-v6`)
- `s3.fmriprep_s3_prefix` — prefix for fMRIPrep archives (e.g., `derivatives/fmriprep`)
- `s3.mmps_mproc_s3_prefix` — prefix for events files (e.g., `mmps_mproc`)
- `s3.upload_prefix` — prefix for uploading results (e.g., `derivatives/fmriprep`)
- `s3.available_sessions` — pool of session codes to probe (e.g., `["00", "02", "04", "06"]`)

## Design Decisions

### Session-Centric Architecture

The outer loop iterates over sessions (not tasks or processing steps). Each session is fully processed — download through upload and cleanup — before the next session begins. This minimizes peak disk usage on EC2, which is critical when processing ~11,000 subjects with multiple sessions each. Only one session's worth of fMRIPrep data needs to be on disk at any time.

### Per-Analysis FD Thresholds

FD thresholds are specified per analysis, not per study. This allows resting-state analyses to use stricter thresholds (e.g., 0.4 mm) while task analyses use more lenient ones (e.g., 0.9 mm). Censor files are generated per unique (task, threshold) combination and named with the threshold in the filename (e.g., `_censor_fd0.4.1D`).

### No Motion-Based Gating

The pipeline always attempts first-level analysis for every subject, regardless of motion severity. Warnings are logged at >25% and >50% censored volumes, but no subjects are automatically excluded. Motion-based exclusion is a post-hoc research decision made at the group level using the QC metrics (particularly `censor.clean_time_seconds` and `censor.pct_censored`).

### Deep-Copy Proc Template

The first-level config is built by deep-copying the proc template and overriding only subject-specific fields (paths, output directories, prefixes). All analysis-level settings (HRF model, contrasts, bandpass, etc.) are preserved verbatim. This enforces separation of concerns: the orchestrator handles data logistics while the proc template controls analysis parameters.

### Partial Success Model

Within a session, each analysis runs independently. If one analysis fails (e.g., AFNI error, insufficient data), other analyses for the same session continue. Across sessions, each session is processed independently — a failed session does not prevent other sessions from being processed. A subject is only marked as fully failed if all sessions fail.

### Idempotent Outputs

Every processing step checks whether its output file already exists before running. This makes the pipeline safe to re-run after a partial failure — it picks up where it left off without re-doing completed work.

### Censor File Naming

Censor files include the FD threshold in the filename (`_censor_fd0.9.1D`) to prevent collisions when multiple analyses use different thresholds for the same task, and to make the threshold traceable from the filename alone.

## Edge Case Behavior

| Scenario | Behavior |
|----------|----------|
| Subject has only 1 session on S3 | Processes that session only; summary shows 1 success |
| Subject has no sessions on S3 | Raises `OrchestratorError` (fatal for that subject) |
| Missing events file for a task run | Run is skipped with a warning; other runs proceed |
| Missing confounds or mask for a run | Run is skipped with a warning; other runs proceed |
| All runs fail for a task | Task is skipped; other tasks proceed |
| All tasks fail for a session | Session raises `OrchestratorError`; other sessions may still succeed |
| One analysis fails (e.g., AFNI error) | Other analyses continue; QC JSON records the error |
| All sessions fail for a subject | Subject-level `OrchestratorError` raised; exit code 1 in batch runner |
| Single-run task with `concatenate_runs: true` | File is copied (not concatenated) — no 3dTcat overhead |
| >50% volumes censored | Warning logged; analysis still attempted |
| Non-steady-state TRs detected | Trimmed from BOLD, confounds, and timing; onsets adjusted |
| Events with onset < 0 after NSS trim | Events dropped with warning |
| Generic "cue" labels in n-back events | Relabeled using condition of subsequent trial block (when `fix_nback_cues: true`) |
| Archive extraction finds path traversal | Unsafe members skipped with warning |
| Insufficient disk space for extraction | `OrchestratorError` raised (requires 10x archive size free) |
| Already-existing output files | Skipped (idempotent); pipeline picks up where it left off |
| `--session` filter for missing session | `OrchestratorError` raised |
| Ctrl+C during batch run | In-flight analyses finish; pending jobs cancelled; summary CSV written; exit 130 |

## Troubleshooting

| Error Message | Likely Cause | Resolution |
|---------------|-------------|------------|
| `AFNI not found on PATH` | AFNI not installed or not in environment | Install AFNI and ensure it's on `PATH`; try `3dinfo -ver` |
| `AWS credentials not found` | No valid AWS credential chain | Configure `~/.aws/credentials`, environment variables, or EC2 instance role |
| `No sessions found on S3 for sub-{ID}` | Subject has no fMRIPrep archives on S3 | Verify subject ID; check S3 bucket/prefix in config |
| `Config missing required section '{section}'` | YAML config is missing `study`, `tasks`, or `analyses` | Check config file against `example_orchestrator_config.yaml` |
| `Insufficient disk space for extraction` | Less than 10x archive size free on disk | Free disk space or use smaller batch sizes |
| `Analysis '{name}' not found in proc template` | Orchestrator analysis name doesn't match proc template | Ensure analysis `name` fields match between configs |
| `Type mismatch for analysis '{name}'` | Analysis type differs between orchestrator and proc template configs | Align `type` fields in both configs |
| `Column 'framewise_displacement' not found` | fMRIPrep confounds TSV missing expected column | Verify fMRIPrep output version/completeness |
| `Missing base motion columns` | fMRIPrep confounds TSV is missing trans/rot columns | Check fMRIPrep output; may indicate corrupted confounds |
| `No task files found for sub-{ID}` | Archive extracted but no BOLD files match task/space | Verify `study.space` matches fMRIPrep output space entity |
| `Exit code 130` (batch runner) | Ctrl+C interruption | Normal interruption; check summary CSV for partial results |

## Development Notes

### File Map

| File | Purpose |
|------|---------|
| `orchestrate_first_level.py` | Main per-subject pipeline: CLI, session loop, pipeline steps 0–14 |
| `run_orchestrator.py` | Parallel batch runner: subject list parsing, ThreadPoolExecutor, progress display, summary CSV |
| `orchestrator_utils.py` | All utility functions: S3 operations, file discovery, preprocessing, QC, config building, validation |
| `example_orchestrator_config.yaml` | Annotated orchestrator config example for ABCD |
| `environment.yaml` | Conda environment specification |

### `orchestrator_utils.py` Section Index

| Section | Functions |
|---------|-----------|
| A: S3 Operations | `_get_s3_client`, `discover_available_sessions`, `download_session_data`, `extract_session_archive`, `upload_to_s3` |
| B: AFNI Check | `verify_afni_installation` |
| C: File Discovery | `discover_session_files` |
| D: Decompression | `decompress_if_needed` |
| E: Brain Masking | `apply_brain_mask` |
| F: Non-Steady-State TR Handling | `detect_non_steady_state_trs`, `remove_initial_trs_bold`, `remove_initial_trs_tabular` |
| G: Confounds Extraction | `extract_motion_regressors`, `generate_censor_file`, `extract_tissue_signals` |
| H: Task Timing | `fix_nback_cue_labels`, `format_task_timing` |
| I: Run Concatenation | `concatenate_bolds`, `concatenate_tabular_files`, `concatenate_task_timing` |
| J: Smoothing | `apply_smoothing` |
| K: QC — Preprocessing | `compute_tsnr`, `generate_carpet_plot`, `compute_registration_quality`, `compute_preproc_qc`, `save_qc_json`, `compute_first_level_qc` |
| L: Config Building | `build_first_level_config`, `write_temp_config` |
| M: Config Validation | `load_orchestrator_config`, `validate_proc_template` |
| N: Output Compression/Cleanup | `compress_session_outputs`, `cleanup_local_inputs` |

### Function-to-Pipeline-Step Mapping

| Pipeline Step | Primary Function(s) |
|---------------|-------------------|
| Step 0: Session discovery | `discover_available_sessions` |
| Step 1: S3 download | `download_session_data` |
| Step 2: Archive extraction | `extract_session_archive` |
| Step 3: File discovery | `discover_session_files` |
| Step 4: Decompression | `decompress_if_needed` |
| Step 5: Brain mask | `apply_brain_mask` |
| Step 6: Preprocessing QC | `compute_preproc_qc`, `compute_tsnr`, `generate_carpet_plot`, `compute_registration_quality` |
| Step 7: NSS TR removal | `detect_non_steady_state_trs`, `remove_initial_trs_bold` |
| Step 8: Censor generation | `generate_censor_file` |
| Step 9: Motion extraction | `extract_motion_regressors` |
| Step 10: Tissue signals | `extract_tissue_signals` |
| Step 11: Task timing | `fix_nback_cue_labels`, `format_task_timing` |
| Step 12: Concatenation | `concatenate_bolds`, `concatenate_tabular_files`, `concatenate_task_timing`, `apply_smoothing` |
| Step 13: First-level analysis | `build_first_level_config`, `write_temp_config`, `compute_first_level_qc` |
| Step 14: Compress/upload/cleanup | `compress_session_outputs`, `upload_to_s3`, `cleanup_local_inputs` |

### Internal Data Structure: `processed_files`

The `processed_files` dict is the central data structure passed between pipeline steps. Its schema differs by task type:

**Task (concatenated):**
```python
processed_files["nback"] = {
    "bold": "/path/to/concat_bold.nii.gz",           # str
    "motion": "/path/to/concat_motion.1D",            # str
    "censor": {                                        # dict: fd_threshold -> path
        0.9: "/path/to/concat_censor_fd0.9.1D",
    },
    "timing": "/path/to/concat_timing.csv",           # str or None
}
```

**Rest (per-run):**
```python
processed_files["rest"] = {
    "bolds": ["/path/run1.nii.gz", "/path/run2.nii.gz"],     # list of str
    "motions": ["/path/run1_motion.1D", "/path/run2_motion.1D"],
    "censors": {                                               # dict: fd_threshold -> list of paths
        0.4: ["/path/run1_censor_fd0.4.1D", "/path/run2_censor_fd0.4.1D"],
    },
    "csf": ["/path/run1_csf.1D", "/path/run2_csf.1D"],
    "wm": ["/path/run1_wm.1D", "/path/run2_wm.1D"],
    "gs": ["/path/run1_gs.1D", "/path/run2_gs.1D"],          # list or None
}
```

### Note on Testing

No automated test suite currently exists for this project. Validation is performed through `--dry-run` mode, per-run QC metrics, first-level QC checks, and log inspection.

## Author

Taylor J. Keding, Ph.D.
