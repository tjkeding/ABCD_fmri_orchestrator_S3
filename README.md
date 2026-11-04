# ABCD_fmri_orchestrator_S3

A session-centric orchestrator for first-level fMRI analysis of the Adolescent Brain Cognitive Development (ABCD) Study on AWS. For each participant it discovers the functional preprocessing outputs stored per file on S3, prepares per-run inputs (non-steady-state trimming, motion and tissue regressors, task timing), runs first-level analyses with [fmri-first-level-proc](https://github.com/tjkeding/fmri-first-level-proc), and uploads the results per file with a completion sentinel. Sessions are processed one at a time, so local disk use is bounded by a single session.

The orchestrator is specific to the ABCD emotional n-back task (`nback`) and resting-state (`rest`) scans. All analysis settings live in the fmri-first-level-proc template config; the orchestrator fills in only participant-, session- and run-specific fields.

Two entry points are provided:

- `orchestrate_first_level.py` processes one participant (all available sessions, or one session with `--session`).
- `run_orchestrator.py` runs `orchestrate_first_level.py` over a list of participants in parallel.

Every configuration field, input format, validation rule and output schema is specified in [INPUT_SPECIFICATION.md](INPUT_SPECIFICATION.md).

## Pipeline overview

For each participant, `orchestrate_first_level.py`:

1. Verifies that the installed fmri-first-level-proc is >= 4.0.0 and < 5.0.0, checks the AFNI installation, and validates the two config files against each other.
2. Lists the participant's input files on S3 once and builds the session, task and run inventory from the filenames. A session is used only when the producer's completion manifest (`_complete.json`) is present and every file it lists is present.
3. Checks, before anything is downloaded, that every run has a brain mask and a confounds file and that each session has the CSF and WM masks required by the proc config.

Then, for each session:

4. Skips the session when its `_COMPLETE` sentinel already exists (unless `study.force_recompute` is true); otherwise downloads the session's inputs.
5. Prepares each run: detects and trims non-steady-state volumes, computes preprocessing QC, writes the motion regressors after checking the rotation units against the producer's framewise displacement, computes CSF and WM regressors from the session tissue masks, and converts the n-back events to run-local timing. A run that cannot be prepared is excluded with a recorded reason, and the session continues with the remaining runs.
6. Skips any analysis that has no usable runs or, for task analyses, whose required conditions are absent from every surviving run.
7. Builds the session's first-level config from the proc template and runs each analysis with fmri-first-level-proc. A failed analysis does not stop the others.
8. Writes a consolidated session QC JSON and classifies the session outcome as complete, failed with a known cause, or failed with an unexplained cause.
9. Uploads the outputs per file, verifies them, removes stale keys left by earlier attempts, writes the `_COMPLETE` sentinel unless the failure is unexplained, and deletes the local files.

The participant's exit code is the most severe session outcome.

## Requirements

- The conda environment in `environment.yaml` (Python 3.14 with numpy, pandas, PyYAML, matplotlib, boto3 and packaging). It installs fmri-first-level-proc from GitHub.
- fmri-first-level-proc >= 4.0.0 and < 5.0.0. The version window is checked at startup.
- [AFNI](https://afni.nimh.nih.gov/pub/dist/doc/htmldoc/background_install/main_toc.html) on `PATH`, at or above the minimum version recorded in fmri-first-level-proc's tool lock file. AFNI is checked at startup (except under `--dry-run`).
- AWS credentials (environment variables, a credentials file or an instance role) with read access to the input prefix and read, write and delete access to the upload prefix.

## Installation

```bash
git clone https://github.com/tjkeding/ABCD_fmri_orchestrator_S3.git
cd ABCD_fmri_orchestrator_S3
conda env create -f environment.yaml
conda activate ABCD_fmri_orchestrator_S3
3dinfo -ver    # AFNI must be on PATH
```

## Quick start

The example configs contain placeholder paths and a placeholder bucket name. Copy them and set `study.input_dir`, `study.output_dir` and `s3.bucket` (orchestrator config) and `global.template_path` (proc config) before use.

```bash
# Validate both configs, run discovery and print the processing plan
# (no downloads, no processing; S3 is still listed, so credentials are needed)
python orchestrate_first_level.py \
  --orchestrate_config example_orchestrator_config.yaml \
  --proc_config example_proc_config.yaml \
  --subj_id SUBJ01 --dry-run

# Process one participant
python orchestrate_first_level.py \
  --orchestrate_config example_orchestrator_config.yaml \
  --proc_config example_proc_config.yaml \
  --subj_id SUBJ01 --log-file logs/SUBJ01.log

# Process a subject list, 8 participants at a time
python run_orchestrator.py \
  --orchestrate_config example_orchestrator_config.yaml \
  --proc_config example_proc_config.yaml \
  --subject-list subjects.txt --n-jobs 8 --log-dir logs/
```

## Command-line reference

### `orchestrate_first_level.py`

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--orchestrate_config` | Yes | | Orchestrator YAML config |
| `--proc_config` | Yes | | fmri-first-level-proc YAML template config |
| `--subj_id` | Yes | | Participant ID, used verbatim in filenames and S3 keys (e.g. `SUBJ01`) |
| `--session` | No | all sessions | Process only this session code (e.g. `00`) |
| `--dry-run` | No | off | Validate configs, run discovery and the input-completeness check, and log the plan; no downloads, processing or uploads |
| `--log-file` | No | none | Also write the log to this file |
| `--skip-qc` | No | off | Skip preprocessing QC, first-level QC and the session QC JSON |
| `--skip-first-level` | No | off | Prepare runs only; no analyses are run and no sentinel is written |

### `run_orchestrator.py`

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--orchestrate_config` | Yes | | Passed to every participant run |
| `--proc_config` | Yes | | Passed to every participant run |
| `--subject-list` | Yes | | Plain-text subject list |
| `--log-dir` | Yes | | Directory for per-participant logs (`{log_dir}/{sub_id}.log`) |
| `--n-jobs` | No | `1` | Participants processed in parallel |
| `--dry-run`, `--skip-qc`, `--skip-first-level`, `--session` | No | | Forwarded to every participant run |
| `--summary-file` | No | `{log_dir}/run_summary_{YYYYMMDD_HHMMSS}.csv` | Batch summary CSV |

The subject list holds one participant ID per line. Blank lines and lines starting with `#` are ignored, and duplicate IDs are skipped with a warning.

```
# example subject list
SUBJ01
SUBJ02
SUBJ03
```

### Exit codes

| Code | `orchestrate_first_level.py` | `run_orchestrator.py` |
|------|------------------------------|-----------------------|
| `0` | Every session complete | Every participant exited 0 |
| `1` | At least one session failed with an unexplained cause, or a fatal error stopped the participant (invalid config, version or AFNI check, no sessions discovered, input-completeness failure) | At least one participant exited 1, exited with any other nonzero code, or crashed |
| `2` | At least one session failed with a known cause (an analysis raised a `ModelError`, such as insufficient degrees of freedom) and none failed unexplained | At least one participant exited 2 and none exited 1 |
| `130` | | Interrupted with Ctrl+C (in-flight participants finish, pending ones are cancelled, the summary CSV is still written) |

## Configuration

Two YAML files configure a run.

**Orchestrator config** (`--orchestrate_config`, see `example_orchestrator_config.yaml`):

| Section | Contents |
|---------|----------|
| `study` | `input_dir` and `output_dir` (absolute paths), `space` (template space entity of the input files), `TR` (seconds), `force_recompute` |
| `tasks` | One entry per task label (`nback`, `rest`): events condition column, conditions to exclude, n-back cue relabeling |
| `qc` | Switches for preprocessing QC (tSNR, carpet plots) and first-level QC |
| `analyses` | One entry per analysis: `name` and `type` matching a proc-config block, `task_label`, and the filename prefixes appended to `sub-{ID}_ses-{NN}A_` |
| `s3` | `enabled` (must be `true`), `bucket`, input prefix templates with `{subject}` and `{session}`, `upload_prefix`, `cleanup_after_upload`, `upload_max_workers` |

**Proc config** (`--proc_config`, see `example_proc_config.yaml`): the fmri-first-level-proc analysis template. HRF model, contrasts, censoring thresholds, bandpass, nuisance model, extraction and connectivity settings are used exactly as written. The orchestrator fills in only each analysis's `out_dir`, output prefixes, per-run path lists and `run_labels`, and drops template blocks that the orchestrator config does not reference. See the fmri-first-level-proc repository for the full template schema.

The example configs define three analyses:

| Analysis | Type | Task | Key settings |
|----------|------|------|--------------|
| `nback_act` | `task_act` | `nback` | GAM HRF, 10 condition regressors (8 stimulus-by-load conditions, `target_0back`, `instruction`), 29 contrasts, FD censoring at 0.9 mm, percent signal change scaling |
| `nback_conn` | `task_conn` | `nback` | Beta series for the 8 stimulus-by-load conditions, parcellated connectivity with Fisher z, the same 29 contrasts, FD censoring at 0.9 mm |
| `rest_conn` | `rest_conn` | `rest` | CSF and WM regressors with derivatives, no global signal regression, 0.01 to 0.1 Hz bandpass, 0.31 to 0.43 Hz notch filter before censoring, FD censoring at 0.4 mm including the preceding volume, parcellated connectivity with Fisher z |

All three use the Schaefer 400-parcel, 7-network cortical atlas combined with the Tian S4 subcortical atlas in MNI152NLin2009cAsym space at 2 mm (Schaefer et al., 2018; Tian et al., 2020).

## Inputs

Each session directory (`{func_prefix_template}` on S3, for example `derivatives/func/sub-{ID}/ses-{NN}A/`) must contain the producer's per-file outputs:

| File | Purpose |
|------|---------|
| `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}_space-{space}_bold.nii.gz` | Preprocessed BOLD run (defines the run) |
| `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}_space-{space}_brainmask.nii.gz` | Run brain mask |
| `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}_desc-confounds_timeseries.tsv` | Confounds: motion parameters, framewise displacement, DVARS, non-steady-state columns |
| `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}_events.tsv` | Task events (n-back runs) |
| `sub-{ID}_ses-{NN}A_space-{space}[_res-{res}]_label-CSF_mask.nii.gz` | Session CSF mask |
| `sub-{ID}_ses-{NN}A_space-{space}[_res-{res}]_label-WM_mask.nii.gz` | Session WM mask |
| `_complete.json` | Producer completion manifest, written last |

Session directories end in `A` (`ses-00A`); the session code is the numeric part (`00`). Other producer files (reference volumes, segmentations, probability maps, GM masks) are ignored.

## Outputs

```
{output_dir}/sub-{ID}/ses-{NN}A/
├── sub-{ID}_ses-{NN}A_first_level_config.yaml     rendered fmri-first-level-proc config
├── preproc/
│   ├── {run_prefix}_trimmed.nii.gz                only when volumes were trimmed (not uploaded)
│   ├── {run_prefix}_motion.1D
│   ├── {run_prefix}_csf.1D, _wm.1D, _gs.1D        when the proc config requests them
│   ├── {run_prefix}_events_fixed.tsv              n-back runs with cue relabeling
│   └── {run_prefix}_timing.tsv                    task runs
├── qc/
│   ├── sub-{ID}_ses-{NN}A_orchestrator_qc.json    consolidated session QC
│   └── preproc/
│       ├── sub-{ID}_{run_label}_tsnr.nii.gz
│       └── sub-{ID}_{run_label}_carpet.png
└── first_level_out/{analysis_name}/               fmri-first-level-proc outputs
```

`{run_prefix}` is `sub-{ID}_ses-{NN}A_task-{task}_run-{RR}` and `{run_label}` is `ses-{NN}A_task-{task}_run-{R}` (run number without zero padding).

The session is mirrored to `s3://{bucket}/{upload_prefix}/sub-{ID}/ses-{NN}A/`: the files each analysis reports as outputs (plus its ledger, or its timing-error and QC-summary files when it failed), everything under `qc/`, the files directly under `preproc/` except NIfTI images, and the rendered config. A zero-byte `{upload_prefix}/sub-{ID}/ses-{NN}A/_COMPLETE` object is written last. With `s3.cleanup_after_upload: true` (the default) the downloaded inputs and the local session tree are deleted afterwards.

## Session outcomes and the completion sentinel

| Outcome | Exit category | `_COMPLETE` |
|---------|---------------|-------------|
| Every analysis succeeded or was skipped for a documented reason, and no run was excluded for an unexplained reason | `0` complete | Written |
| No unexplained failure, and at least one analysis failed with a `ModelError` (a data-quality floor; rerunning cannot change it) | `2` known cause | Written |
| An analysis failed with any other error, a run was excluded for an unexpected error or a rotation-unit mismatch, or the session raised an error | `1` unexplained | Not written, so the next invocation reprocesses the session |

With `--skip-first-level` the sentinel is never written. A session whose sentinel exists is skipped on later invocations unless `study.force_recompute` is true, in which case the sentinel is deleted and the session is recomputed from an empty local output directory.

Documented run exclusions (`events_missing`, `motion_non_finite`, `tissue_mask_empty`, `events_invalid_values`, `events_onset_out_of_range`) and documented analysis skips (`no_usable_runs`, `events_missing_conditions`) are terminal and leave the session complete. Run exclusions for `producer_fd_inconsistent` or `unexpected_error` make the session unexplained.

## Quality control

Each session's `qc/sub-{ID}_ses-{NN}A_orchestrator_qc.json` contains:

- `provenance`: orchestrator, fmri-first-level-proc and AFNI versions, timestamp, participant, session, and the producer manifest's `written_at` and `schema_version`.
- `preprocessing`: per run, the number of trimmed non-steady-state volumes, DVARS mean and maximum, median brain tSNR, brain-mask voxel count and volume, carpet-plot path, the producer's framewise displacement (informational; censoring uses the FD computed by fmri-first-level-proc), the rotation-unit check result, tissue-mask voxel counts, and event counts (dropped, written, conditions present).
- `preproc_excluded_runs`: every excluded run with its reason and message.
- `analyses`: per analysis, the status, skip reason, fmri-first-level-proc result status, a `qc_flagged` flag (fmri-first-level-proc returned a warning status), error details, graded warnings, wall time, run labels, whether the outcome is terminal, and a `qc` block with `pct_censored`, output counts and the full fmri-first-level-proc QC summary.
- `session`: status (`success`, `partial` or `failed`), counts of attempted, succeeded, skipped and flagged analyses, `all_terminal`, peak memory, and summary flags (`preproc_excluded_runs`, `analysis_skipped`, `analysis_failed`, `upstream_warning`).

No participant is excluded for motion by the orchestrator. Censoring is performed by fmri-first-level-proc at each analysis's FD threshold, and exclusion decisions are made at the group level from the QC JSON, for example:

```python
import glob, json
import pandas as pd

rows = []
for path in glob.glob("outputs/sub-*/ses-*A/qc/*_orchestrator_qc.json"):
    with open(path) as f:
        qc = json.load(f)
    for name, a in qc["analyses"].items():
        rows.append({
            "sub_id": qc["provenance"]["sub_id"],
            "session": qc["provenance"]["session"],
            "analysis": name,
            "status": a["status"],
            "qc_flagged": a["qc_flagged"],
            "pct_censored": (a["qc"] or {}).get("pct_censored"),
        })
qc_table = pd.DataFrame(rows)
```

## Design notes

- **Session-centric processing.** Each session is downloaded, prepared, analyzed, uploaded and cleaned up before the next one starts, which bounds local disk use to one session.
- **Proc-config authority.** Analysis settings are never injected or overridden by the orchestrator. `global.tr` must equal `study.TR`; a mismatch is an error rather than a silent correction.
- **Rotation-unit check.** fmri-first-level-proc expects rotations in degrees. For every run the orchestrator recomputes Power framewise displacement (Power et al., 2012) from the six motion parameters with rotations taken as degrees on a 50 mm sphere and compares it with the producer's `framewise_displacement` column (absolute tolerance 1e-6 mm). A degree/radian mismatch changes FD by orders of magnitude more than the tolerance, so a disagreement excludes the run and marks the session unexplained.
- **Event timing.** Onsets are shifted by the trimmed duration. Events whose shifted onset is negative are dropped, including events that straddle the cutoff, because a clipped event would be modeled late and would shorten its condition's mean duration.
- **Strict inputs.** A session without a completion manifest is skipped; a manifest that lists missing files, or a session lacking required masks, stops the participant with an error that lists every missing item. With `--session`, only that session's manifest is read.
- **Verified per-file upload.** Only missing or size-mismatched files are uploaded, every expected key is verified afterwards, stale keys under the session prefix are deleted, and the sentinel is written last.
- **S3 only.** Inputs are listed and downloaded from S3, each session is recomputed from an empty local output directory, and outputs are uploaded back to S3; `study.input_dir` and `study.output_dir` are local working directories.

## Troubleshooting

| Message | Likely cause | Resolution |
|---------|--------------|------------|
| `fmri_first_level_proc ... requires >= 4.0.0` or `requires < 5.0.0` | Wrong upstream version | Recreate the environment from `environment.yaml` |
| `AFNI preflight failed` or `AFNI version check failed` | AFNI missing from `PATH` or older than the upstream minimum | Install or update AFNI |
| `AWS credentials not found` | No credential source available | Configure AWS credentials for the account that holds the bucket |
| `No sessions discovered for sub-{ID}` | Wrong ID or prefix template, or no session has a completion manifest yet | Check `s3.func_prefix_template` and the participant's S3 listing; sessions without `_complete.json` are skipped |
| `_complete.json lists file(s) absent from the listing` | Producer output for that session is incomplete | Regenerate or repair the session's producer outputs |
| `Subject input-completeness gate failed: ...` | Brain mask, confounds or a required tissue mask is missing | The message lists every missing item |
| `global.tr in proc template (...) does not match study.TR (...)` | The two configs disagree | Make the values equal |
| `Orchestrator analysis '{name}' has no matching entry in proc template` or `Type mismatch for analysis '{name}'` | Analysis names or types differ between the configs | Align `name` and `type` |
| `Stale sentinel present at upload start` | A `_COMPLETE` object appeared under the session prefix while the session was being processed | Ensure only one process handles a participant at a time, then rerun |
| `Post-upload verification failed` | Keys missing or size-mismatched after upload | Rerun; only the gap is uploaded |
| Run excluded with reason `producer_fd_inconsistent` | Producer rotations are not in degrees, or its FD is computed differently | Inspect that run's confounds file |

## Repository contents

| File | Purpose |
|------|---------|
| `orchestrate_first_level.py` | Per-participant entry point: command line, session loop, run preparation, analysis dispatch, QC, upload |
| `orchestrator_utils.py` | Discovery, download, run preparation, QC, config building and validation, outcome classification, S3 upload |
| `run_orchestrator.py` | Parallel batch runner with progress display and summary CSV |
| `example_orchestrator_config.yaml` | Example orchestrator config (placeholder paths and bucket) |
| `example_proc_config.yaml` | Example fmri-first-level-proc template config (placeholder atlas path) |
| `environment.yaml` | Conda environment |
| `INPUT_SPECIFICATION.md` | Complete input, validation and output reference |
| `LICENSE` | MIT license |

## References

- Casey, B. J., et al. (2018). The Adolescent Brain Cognitive Development (ABCD) study: Imaging acquisition across 21 sites. *Developmental Cognitive Neuroscience*, 32, 43-54.
- Cox, R. W. (1996). AFNI: Software for analysis and visualization of functional magnetic resonance neuroimages. *Computers and Biomedical Research*, 29(3), 162-173.
- Power, J. D., Barnes, K. A., Snyder, A. Z., Schlaggar, B. L., & Petersen, S. E. (2012). Spurious but systematic correlations in functional connectivity MRI networks arise from subject motion. *NeuroImage*, 59(3), 2142-2154.
- Schaefer, A., et al. (2018). Local-global parcellation of the human cerebral cortex from intrinsic functional connectivity MRI. *Cerebral Cortex*, 28(9), 3095-3114.
- Tian, Y., Margulies, D. S., Breakspear, M., & Zalesky, A. (2020). Topographic organization of the human subcortex unveiled with functional connectivity gradients. *Nature Neuroscience*, 23(11), 1421-1432.

## License

Released under the MIT License. See [LICENSE](LICENSE).

## Author

Taylor J. Keding, Ph.D.
