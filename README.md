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
          ├── Step 1:  Download session data from S3 (archive + events + motion)
          ├── Step 2:  Extract fMRIPrep archive
          ├── Step 3:  Discover files per task (glob func/ + match motion files)
          │
          │   ┌── Per task, per run ──────────────────────────────┐
          ├── │ Step 4:  Decompress if needed (.bz2/.gz/.tar.gz) │
          │   │ Step 5:  Apply brain mask (3dcalc)               │
          │   │ Step 6:  Preprocessing QC (FD from raw motion)    │
          │   │ Step 7:  Detect & remove non-steady-state TRs    │
          │   │ Step 8:  Extract motion regressors (from raw TSV) │
          │   │ Step 9:  Extract tissue signals (rest only)       │
          │   │ Step 10: Format task timing (task only)           │
          │   └──────────────────────────────────────────────────┘
          │
          ├── Step 11: Concatenate runs (task) or collect per-run (rest)
          │             + optional spatial smoothing
          ├── Step 12: Build first-level config & run analyses
          └── Step 13: Upload outputs (per-file) to S3 → write _COMPLETE sentinel → cleanup
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
cd ABCD_fmri_orchestrator_S3

# Create and activate the conda environment
conda env create -f environment.yaml
conda activate ABCD_fmri_orchestrator_S3

# Verify AFNI is available
3dinfo -ver
```

The conda environment installs `fmri_first_level_proc` directly from GitHub via pip. **Requires `fmri_first_level_proc` >= 2.5.0.** See `environment.yaml` for the full dependency list.

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

Downloads three types of files: (1) the fMRIPrep archive (`.tar.gz` containing BOLD, confounds, masks, and anatomical files), (2) task events files from the `mmps_mproc` prefix (non-rest tasks only), and (3) raw motion parameter files from `mmps_mproc` (ALL tasks including rest). Events and motion files are probed for runs 1–9 per task; download stops at the first missing run number. Already-present local files are skipped (idempotent).

### Step 2: Extract fMRIPrep Archive

Extracts the `.tar.gz` archive with safety checks: disk space verification (requires 10x archive size free) and path traversal protection (rejects tar members that resolve outside the target directory). The extractor searches for the `func/` subdirectory, which may be nested inside `sub-{ID}/ses-{session}A/` within the archive.

### Step 3: Discover Files Per Task

Globs the extracted `func/` directory for BOLD files matching each task label and template space. For each discovered run, the corresponding confounds TSV, brain mask, raw motion file, and events file (matched by run number, not position) must all exist for the run to be included. Missing files produce warnings and the run is skipped. An anatomical brain mask is also discovered for optional registration QC.

### Step 4: Decompress If Needed

Handles files that may exist in compressed form (`.bz2`, `.gz`, or inside a `.tar.gz`). Most fMRIPrep outputs are `.nii.gz` and pass through unchanged. This step ensures confounds TSVs and other files are available in their expected format.

### Step 5: Apply Brain Mask

Uses AFNI `3dcalc` with expression `a*step(b)` to zero out non-brain voxels. Produces `{prefix}_masked.nii.gz`. Idempotent — skips if output already exists.

### Step 6: Preprocessing QC

Computes per-run, non-motion quality metrics. DVARS (from fMRIPrep confounds), brain mask coverage (voxel count and volume in mm^3), and optionally tSNR (median within-brain temporal signal-to-noise ratio), carpet plots (DVARS trace above voxel-by-time heatmap), and registration quality (Dice coefficient between functional and anatomical brain masks) are computed. Motion metrics (FD, censor counts) are not computed here; they are sourced from upstream `enorm.1D`/`censor.1D` files produced by `fmri_first_level_proc` and reported in the per-analysis section of the consolidated session QC JSON. Skipped if `--skip-qc` is set or `qc.preproc.enabled` is false.

### Step 7: Detect and Remove Non-Steady-State TRs

Counts `non_steady_state_outlier_*` columns in the fMRIPrep confounds file to determine how many initial TRs to remove. Uses AFNI `3dTcat` to trim the BOLD timeseries. If no non-steady-state TRs are detected, the file passes through unchanged.

### Step 8: Extract Motion Regressors

Extracts the 6 base motion parameters (trans_x/y/z, rot_x/y/z) from the raw motion.tsv file (not fMRIPrep confounds). Rotations remain in degrees per the `fmri_first_level_proc` >= 2.5.0 input contract; no unit conversion is applied. Temporal derivatives are always computed numerically via finite differences (padded with 0.0 at the first row). Total output columns = `6 * (1 + calc_n_motion_derivs)`. NaN values indicate motion tracking failures and are imputed to 999.0, guaranteeing those TRs exceed any reasonable FD threshold and are censored by upstream `1d_tool.py`.

### Step 9: Extract Tissue Signals (Rest Only)

For resting-state tasks, extracts CSF, white matter, and global signal timeseries from the confounds TSV for use as nuisance regressors in rest_conn analyses.

### Step 10: Format Task Timing (Task Only)

Converts BIDS events TSVs to the first-level timing CSV format (CONDITION, ONSET, DURATION). Adjusts onsets for removed non-steady-state TRs and drops events that fall before time 0 after adjustment. For n-back tasks with `fix_nback_cues: true`, generic "cue" trial types are relabeled based on the n-back level of the subsequent block: 0-back cues become the bare stimulus condition (e.g., "posface", "place") because they are passive viewing events, while 2-back cues become "instruction" because they are instruction screens preceding the recall task.

### Step 11: Concatenate Runs

For tasks with `concatenate_runs: true` (default for non-rest tasks): concatenates BOLD files (AFNI `3dTcat`), motion regressors, and task timing (with onset adjustment for cumulative run lengths). For single-run tasks, files are copied rather than concatenated. For rest tasks (`concatenate_runs: false`): per-run files are collected as lists. Optional spatial smoothing is applied after concatenation (or per-run for rest).

### Step 12: Build Config and Run First-Level Analyses

Deep-copies the proc template config and overrides only subject-specific fields (paths, output directories, session-aware prefixes). Injects `global.tr` from `study.TR` if not already present (validates match if present). Injects per-analysis `fd_threshold` and `censor_prev_tr` fields; censoring is handled automatically by `fmri_first_level_proc`. The orchestrator changes the working directory to the session output directory before running AFNI to prevent `3dDeconvolve.err` file collisions between concurrent sessions. Each analysis runs independently — if one fails, others continue. First-level QC reads the upstream QC summary JSON produced by `fmri_first_level_proc` for censor statistics and other metrics. After all analyses complete, a consolidated session-level QC JSON (`orchestrator_qc.json`) is written, combining pre-analysis preprocessing metrics with per-analysis status and upstream motion metrics.

### Step 13: Upload (Per-File), Sentinel, Cleanup

When S3 is enabled, output files are uploaded in parallel under the per-session prefix; files already on S3 with matching size are skipped. A zero-byte `_COMPLETE` sentinel is written after verification. Local cleanup follows if `s3.cleanup_after_upload` is true (also runs on session failure).

Subsequent invocations skip sessions with a `_COMPLETE` sentinel; partial uploads resume by re-uploading only missing or size-mismatched files.

## Output Directory Structure

```
{output_dir}/sub-{ID}/ses-{session}A/
├── preproc/                                              # Per-run intermediate files
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_masked.nii.gz
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_trimmed.nii.gz
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_motion.1D
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_timing.csv
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_events_fixed.tsv  (nback only)
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_csf.1D            (rest only)
│   ├── sub-{ID}_ses-{session}A_task-{task}_run-{N}_wm.1D             (rest only)
│   └── sub-{ID}_ses-{session}A_task-{task}_run-{N}_gs.1D             (rest only)
│
├── concat/                                               # Concatenated files (task only)
│   ├── sub-{ID}_ses-{session}A_task-{task}_concat_bold.nii.gz
│   ├── sub-{ID}_ses-{session}A_task-{task}_concat_motion.1D
│   ├── sub-{ID}_ses-{session}A_task-{task}_concat_mask.nii.gz   (intersection of per-run masks)
│   └── sub-{ID}_ses-{session}A_task-{task}_concat_timing.csv
│
├── first_level_out/                                      # Analysis outputs (uploaded to S3)
│   ├── {analysis_name}/
│   │   └── (analysis-specific outputs: stat buckets, beta series, etc.)
│   └── ...
│
├── qc/
│   ├── preproc/                                          # Per-run carpet plot images
│   │   └── sub-{ID}_ses-{session}A_task-{task}_run-{N}_carpet.png
│   └── sub-{ID}_ses-{session}A_orchestrator_qc.json      # Consolidated session QC JSON
│
└── sub-{ID}_ses-{session}A_first_level_config.yaml       # Generated proc config
```

The per-file S3 upload mirrors this local tree under `s3://{bucket}/{upload_prefix}/sub-{ID}/ses-{session}A/`, excluding large intermediate BOLD NIfTI files (regenerable from fMRIPrep). The `_COMPLETE` sentinel is written directly to S3 at the per-session prefix root after upload verification.

Legacy `first_level_out.tar.gz` archives from prior versions are auto-migrated (see below).

## Quality Control

### Consolidated Session QC

One JSON file per session (`sub-{ID}_ses-{session}A_orchestrator_qc.json`) combining:

**Provenance block:**
- `orchestrator_version`, `fmri_first_level_proc_version`, `afni_version`, `timestamp_utc`

**Per-run preprocessing metrics (non-motion, computed by the orchestrator):**
- **DVARS**: mean and max (from fMRIPrep confounds)
- **tSNR**: median within-brain temporal signal-to-noise ratio (optional, via `qc.preproc.tsnr`)
- **Brain mask**: voxel count and volume in mm^3
- **Carpet plots**: DVARS trace above voxel-by-time heatmap (optional, via `qc.preproc.carpet_plots`); saved as PNG to `qc/preproc/`
- **Registration quality**: Dice coefficient between functional and anatomical brain masks (optional, via `qc.preproc.registration_quality`)

**Per-analysis metrics (motion and status, sourced from upstream):**
- **completed_successfully**: whether at least one non-empty NIfTI output was produced
- **pct_censored**: percentage of volumes censored (read from upstream QC summary JSON produced by `fmri_first_level_proc`)
- **upstream_qc**: full upstream QC dict (censor stats, DOF, trial counts, etc.) or null if not available
- **error**: error message if the analysis failed, else null
- **wall_time_seconds**: analysis wall time

**Session summary:**
- **status**: qualified session status (`success`, `partial`, or `failed`)
- **n_analyses_attempted**, **n_analyses_succeeded**, **wall_time_seconds**

Motion metrics (FD, censor counts) are not computed by the orchestrator. They are sourced from the upstream `enorm.1D` and `censor.1D` files produced by `fmri_first_level_proc` and reported via the `upstream_qc` block.

### Group-Level Aggregation

Consolidated QC JSONs are designed for easy aggregation with pandas:

```python
import json, glob
import pandas as pd

# Consolidated session QC
qc_files = glob.glob("*/ses-*/qc/sub-*_orchestrator_qc.json")
sessions = [json.load(open(f)) for f in qc_files]

# Key fields for exclusion decisions:
#   analyses[name].upstream_qc.pct_censored  — censoring rate per analysis
#   session.status                           — success / partial / failed
```

## S3 Data Structure

### Source Data Patterns

| Data | S3 Key Pattern |
|------|----------------|
| fMRIPrep archive | `{fmriprep_s3_prefix}/sub-{ID}/ses-{session}A/sub-{ID}_ses-{session}A_fmriprep-output.tar.gz` |
| Events files | `{mmps_mproc_s3_prefix}/sub-{ID}/ses-{session}A/func/sub-{ID}_ses-{session}A_task-{task}_run-0{N}_events.tsv` |
| Upload target (per-file) | `{upload_prefix}/sub-{ID}/ses-{session}A/{first_level_out,qc,preproc,concat}/...` |
| Completion sentinel | `{upload_prefix}/sub-{ID}/ses-{session}A/_COMPLETE` |

Session labels follow the format `ses-{code}A` where `{code}` is a session code from `s3.available_sessions` (e.g., `ses-00A`, `ses-02A`, `ses-04A`, `ses-06A`). The "A" suffix is a study convention for the ABCD dataset.

### Config Fields Controlling S3 Paths

- `s3.bucket` — S3 bucket name (e.g., `abcd-v6`)
- `s3.fmriprep_s3_prefix` — prefix for fMRIPrep archives (e.g., `derivatives/fmriprep`)
- `s3.mmps_mproc_s3_prefix` — prefix for events files (e.g., `mmps_mproc`)
- `s3.upload_prefix` — prefix for uploading results (e.g., `derivatives/fmriprep`)
- `s3.available_sessions` — pool of session codes to probe (e.g., `["00", "02", "04", "06"]`)
- `s3.upload_max_workers` — number of concurrent worker threads for per-file S3 upload (default `8`, valid range `[1, 64]`, integer); boto3 S3 client is thread-safe and one client is shared across all workers

### Legacy Archive Auto-Migration

Legacy `first_level_out.tar.gz` archives are auto-detected and rehosted in the per-file layout on next re-touch (no reprocessing; byte-equivalent outputs preserved). The source tarball is deleted from S3 after successful migration, and a `_COMPLETE` sentinel is written to the per-session prefix.

Migrated sessions carry a top-level `migration` block in the orchestrator QC JSON (source ETag, UTC timestamp, orchestrator version) for provenance auditing.

## Design Decisions

### Session-Centric Architecture

The outer loop iterates over sessions (not tasks or processing steps). Each session is fully processed — download through upload and cleanup — before the next session begins. This minimizes peak disk usage on EC2, which is critical when processing ~11,000 subjects with multiple sessions each. Only one session's worth of fMRIPrep data needs to be on disk at any time.

### Per-Analysis FD Thresholds

FD thresholds are specified per analysis, not per study. This allows resting-state analyses to use stricter thresholds (e.g., 0.4 mm) while task analyses use more lenient ones (e.g., 0.9 mm). The `fd_threshold` and `censor_prev_tr` fields are injected into each analysis block of the generated config and passed to `fmri_first_level_proc`, which handles censor file generation automatically.

### No Motion-Based Gating

The pipeline always attempts first-level analysis for every subject, regardless of motion severity. No subjects are automatically excluded. Motion-based exclusion is a post-hoc research decision made at the group level using the QC metrics (particularly `analyses.{name}.upstream_qc.pct_censored` and `session.status` from the consolidated session QC JSON).

### Deep-Copy Proc Template

The first-level config is built by deep-copying the proc template and overriding only subject-specific fields (paths, output directories, prefixes). All analysis-level settings (HRF model, contrasts, bandpass, etc.) are preserved verbatim. This enforces separation of concerns: the orchestrator handles data logistics while the proc template controls analysis parameters.

### Partial Success Model

Within a session, each analysis runs independently. If one analysis fails (e.g., AFNI error, insufficient data), other analyses for the same session continue. Across sessions, each session is processed independently — a failed session does not prevent other sessions from being processed. A subject is only marked as fully failed if all sessions fail.

### Motion Data from Raw Files

All motion parameters, framewise displacement (FD), and motion derivatives are sourced from raw motion.tsv files (mmps_mproc), NOT from fMRIPrep's confounds_timeseries.tsv. This is because fMRIPrep's motion parameters in the confounds file do not include motion correction information. Rotations remain in degrees in the output `.1D` file per the `fmri_first_level_proc` >= 2.5.0 input contract; FD computation and radian conversion are handled exclusively by `fmri_first_level_proc`. DVARS, tissue signals, and non-steady-state detection remain sourced from confounds (derived from the BOLD signal, unaffected by motion source).

### Idempotent Outputs

Every processing step checks whether its output file already exists before running. This makes the pipeline safe to re-run after a partial failure — it picks up where it left off without re-doing completed work.

## Edge Case Behavior

| Scenario | Behavior |
|----------|----------|
| Subject has only 1 session on S3 | Processes that session only; summary shows 1 success |
| Subject has no sessions on S3 | Raises `OrchestratorError` (fatal for that subject) |
| Missing events file for a task run | Run is skipped with a warning; other runs proceed |
| Missing motion file for a run | Run is skipped with a warning; other runs proceed |
| Missing confounds or mask for a run | Run is skipped with a warning; other runs proceed |
| All runs fail for a task | Task is skipped; other tasks proceed |
| All tasks fail for a session | Session raises `OrchestratorError`; other sessions may still succeed |
| One analysis fails (e.g., AFNI error) | Other analyses continue; QC JSON records the error |
| All sessions fail for a subject | Subject-level `OrchestratorError` raised; exit code 1 in batch runner |
| Single-run task with `concatenate_runs: true` | File is copied (not concatenated) — no 3dTcat overhead |
| >50% volumes censored | Warning from upstream; analysis still attempted |
| Non-steady-state TRs detected | Trimmed from BOLD, confounds, and timing; onsets adjusted |
| Events with onset < 0 after NSS trim | Events dropped with warning |
| Generic "cue" labels in n-back events | 0-back cues relabeled with bare stimulus condition; 2-back cues relabeled as "instruction" (when `fix_nback_cues: true`) |
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
| `orchestrate_first_level.py` | Main per-subject pipeline: CLI, session loop, pipeline steps 0–13 |
| `run_orchestrator.py` | Parallel batch runner: subject list parsing, ThreadPoolExecutor, progress display, summary CSV |
| `orchestrator_utils.py` | All utility functions: S3 operations, file discovery, preprocessing, QC, config building, validation |
| `example_orchestrator_config.yaml` | Annotated orchestrator config example for ABCD |
| `environment.yaml` | Conda environment specification |

### `orchestrator_utils.py` Section Index

| Section | Functions |
|---------|-----------|
| A: S3 Operations | `_get_s3_client`, `enumerate_upload_targets`, `check_session_complete`, `delete_session_sentinel`, `determine_session_routing`, `discover_available_sessions`, `download_session_data`, `discover_local_mmps_files`, `extract_session_archive`, `upload_session_to_s3`, `migrate_session_from_archive` |
| B: AFNI Check | `verify_afni_installation` |
| C: File Discovery | `discover_session_files` |
| D: Decompression | `decompress_if_needed` |
| E: Brain Masking | `apply_brain_mask` |
| F: Non-Steady-State TR Handling | `detect_non_steady_state_trs`, `remove_initial_trs_bold`, `remove_initial_trs_tabular` |
| G: Confounds Extraction | `extract_motion_regressors`, `extract_tissue_signals` |
| H: Task Timing | `fix_nback_cue_labels`, `format_task_timing` |
| I: Run Concatenation | `concatenate_bolds`, `concatenate_tabular_files`, `concatenate_task_timing` |
| J: Smoothing | `apply_smoothing` |
| J2: Mask Intersection | `compute_mask_intersection` |
| K: QC — Preprocessing | `compute_tsnr`, `generate_carpet_plot`, `compute_registration_quality`, `compute_preproc_qc`, `save_qc_json`, `compute_first_level_qc`, `consolidate_session_qc` |
| L: Config Building | `build_first_level_config`, `write_temp_config` |
| M: Config Validation | `load_orchestrator_config`, `validate_proc_template` |
| N: Output Cleanup | `cleanup_local_inputs` |

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
| Step 8: Motion extraction | `extract_motion_regressors` |
| Step 9: Tissue signals | `extract_tissue_signals` |
| Step 10: Task timing | `fix_nback_cue_labels`, `format_task_timing` |
| Step 11: Concatenation | `concatenate_bolds`, `concatenate_tabular_files`, `concatenate_task_timing`, `compute_mask_intersection`, `apply_smoothing` |
| Step 12: First-level analysis | `build_first_level_config`, `write_temp_config`, `compute_first_level_qc`, `consolidate_session_qc` |
| Step 13: Upload/sentinel/cleanup | `enumerate_upload_targets`, `upload_session_to_s3`, `cleanup_local_inputs` |

### Internal Data Structure: `processed_files`

The `processed_files` dict is the central data structure passed between pipeline steps. Its schema differs by task type:

**Task (concatenated):**
```python
processed_files["nback"] = {
    "bold": "/path/to/concat_bold.nii.gz",           # str
    "motion": "/path/to/concat_motion.1D",            # str
    "timing": "/path/to/concat_timing.csv",           # str or None
}
```

**Rest (per-run):**
```python
processed_files["rest"] = {
    "bolds": ["/path/run1.nii.gz", "/path/run2.nii.gz"],     # list of str
    "motions": ["/path/run1_motion.1D", "/path/run2_motion.1D"],
    "csf": ["/path/run1_csf.1D", "/path/run2_csf.1D"],
    "wm": ["/path/run1_wm.1D", "/path/run2_wm.1D"],
    "gs": ["/path/run1_gs.1D", "/path/run2_gs.1D"],          # list or None
}
```

### Testing

The `tests/` directory contains pytest test suites for validating orchestrator utility functions. Run with:

```bash
python -m pytest tests/ -v
```

Test outputs are written to a sandbox directory and cleaned up after each test run.

## Author

Taylor J. Keding, Ph.D.
