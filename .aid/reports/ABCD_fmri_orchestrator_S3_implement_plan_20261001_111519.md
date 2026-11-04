# ABCD_fmri_orchestrator_S3: Implementation Plan (CR 2026-10-01 harmonization fixes)

## Assumptions and decisions made without explicit approval

Each item below is resolved to a directive in the change list; none is a gate.

1. **Legacy concat upload walk retained.** The `concat/` walk in `enumerate_upload_targets` and the `concat` subdirectory mapping in `migrate_session_from_archive` stay in place, so legacy archives (produced by orchestrator < 4.0) migrate with full fidelity. A comment marks the walk as legacy-only. Evidence: legacy tarballs contain `concat/` members (`tests/test_legacy_routing_migration.py::_build_legacy_tarball`), and removing the walk would silently drop those files during migration.
2. **Dead per-run counters removed.** `per_run_tr_counts` and `per_run_timing_tr_counts` are removed from `_process_session`. Their only consumer was `concatenate_task_timing`, which this plan retires. The `n_trs <= 0` guard is kept.
3. **Missing events file for a task run raises inside the per-run try.** A non-rest run whose `events_path` is `None` raises `FileNotFoundError` inside the per-run try, so the run is skipped and the per-run lists stay aligned. This path is currently unreachable, because discovery already excludes such runs. The guard makes the D6 alignment invariant hold by construction rather than by upstream filtering.
4. **Placeholder paths converted to list form.** The `paths` placeholders in `example_proc_config.yaml` and `proc_config_final.yaml` become list-form keys (`scan_paths`, `task_timing_paths`, `motion_paths`), as a direct consequence of D1.
5. **`remove_previous` comments annotated.** The comments in the example and final proc configs state that the orchestrator forces `remove_previous: true` at render time.
6. **Test edits are in scope; tests are not run.** Test-file edits fall within this implementation, because the CR action items target `implement` and prior builds edited `tests/` fixtures. Per the implement skill contract, tests are not run. New test cases for newly introduced behaviors are routed to `/test`.
7. **Header metadata updated.** The header of `orchestrate_first_level.py` gets `Version: 4.0`, consistent with `__version__ = "4.0"`. Its pipeline description replaces "run concatenation" with "per-run collection".
8. **`[FLAGGED]` tag in the session summary log.** The session summary log line gets a `[FLAGGED]` tag for analyses whose upstream status is `"warning"`, mirroring the new `qc_flagged` field.
9. **Fallback QC path uses the D4 completion criterion.** `compute_first_level_qc` (the failure-path QC builder) uses the same `completed_successfully` criterion as the success path, so the two QC paths cannot disagree for the same output directory. The criterion is that the primary output file exists with size > 0, per `PRIMARY_OUTPUT_SUFFIX`.
10. **`upstream_status` vocabulary in tests.** The field already passes through to the consolidated QC. Test fixtures that set it use the real upstream values `"success"` or `"warning"` (FACT1) instead of placeholder strings.

## Upstream contract this plan depends on (fmri_first_level_proc >= 3.2.0)

### Per-run inputs (task_act and task_conn)

- The analysis `paths` block accepts three per-run lists: `scan_paths`, `motion_paths` and `task_timing_paths`.
- The lists are run-aligned and of equal length.
- Timing onsets are run-local: each run's onsets are relative to that run's first retained volume.

### Primary output filenames (frozen)

| Analysis | Primary output file |
|---|---|
| task_act | `{out_file_pre}_concat_bucket_stats.nii.gz` |
| task_conn | `{out_file_pre}_concat_LSS.nii.gz` |
| rest_conn | `{out_file_pre}_concat_residual_dtseries.nii.gz` |

### Run-level outputs

- Per-run PSC is applied upstream before regression.
- PSC intermediates end in `_psc.nii.gz`.
- `FirstLevelResult.status` is in {`"success"`, `"warning"`}.

### Invariant (verbatim, carried into README and the upstream contract)

"For multi-run task_act and task_conn analyses, upstream must concatenate all runs into a single time series and fit exactly one 3dDeconvolve model per analysis (and, for task_conn, exactly one 3dLSS fit on the concatenated series). Task and condition regressors are shared across runs. Only the baseline polynomial terms are run-specific, via `-concat`. Run-wise operations are restricted to stimulus-independent preprocessing (per-run PSC scaling). Per-run regression is prohibited."

```xml
<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-10-01T15:15:19Z" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_cr_20261001_142121.md" mode="cr" key_items="16" />
  </input_reports>
  <locked_decisions>
    <decision id="D1">Task_act and task_conn proc-config blocks receive per-run lists scan_paths, motion_paths, task_timing_paths (run-local onsets). MIN_UPSTREAM_VERSION = "3.2.0". environment.yaml pins @v3.2.0.</decision>
    <decision id="D2">Retire orchestrator-side concatenation: remove concatenate_bolds, concatenate_tabular_files, concatenate_task_timing, compute_mask_intersection, their imports and tests, and creation of the concat/ directory. Reject the orchestrator-config key concatenate_runs with a migration error. Update README, INPUT_SPECIFICATION, and example orchestrator config.</decision>
    <decision id="D3">No runtime PSC warning. scale_to_psc stays "(recommended)" with per-run wording. README Step 12 carries the invariant, a note that per-run PSC and run-boundary baselines are applied upstream (>= 3.2.0), and a regeneration advisory for multi-run task PSC outputs produced by orchestrator &lt; 4.0.</decision>
    <decision id="D4">PRIMARY_OUTPUT_SUFFIX map; completed_successfully is true iff {out_dir}/{out_file_pre}{suffix} exists with size &gt; 0; build_first_level_qc_from_result gains out_dir and out_file_pre parameters.</decision>
    <decision id="D5">Upstream-import guard at the top of process_participant before Step 0; both guard message and main() message interpolate MIN_UPSTREAM_VERSION.</decision>
    <decision id="D6">Commit-on-success per run (outputs held in locals, appended to all per-run lists together after the last per-run step succeeds); equal-length assertion across populated per-run lists before processed_files is built, raising OrchestratorError on mismatch.</decision>
    <decision id="FACT1">qc_flagged = (result is not None and result.status == "warning"). _derive_session_status is unchanged.</decision>
  </locked_decisions>
  <changes>

    <change id="C1" priority="P0" source_item="F6 (P0: packaging.version), D1">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Raise the minimum upstream version to 3.2.0 and replace the regex version parser with PEP 440 comparison via packaging.version, so pre-release and dev builds (e.g. 3.2.0rc1, 3.2.0.dev0) no longer satisfy the minimum.</description>
      <spec>
        Section B (lines ~1150-1210):
        1. Add at module import block (after the third-party imports `import pandas as pd`): `from packaging.version import Version, InvalidVersion`.
        2. Change `MIN_UPSTREAM_VERSION = "3.1.0"` to `MIN_UPSTREAM_VERSION = "3.2.0"`.
        3. Delete the helper `_parse_version` (lines ~1159-1170) in its entirety.
        4. In `verify_upstream_version(logger, min_version=MIN_UPSTREAM_VERSION)`, replace the tuple comparison (lines ~1194-1200) with:
           ```python
           try:
               installed = Version(str(version_str))
               required = Version(str(min_version))
           except InvalidVersion as e:
               raise OrchestratorError(
                   f"Could not parse fmri_first_level_proc version "
                   f"'{version_str}' (minimum required: {min_version}): {e}"
               ) from e
           if installed < required:
               raise OrchestratorError(
                   <existing below-minimum message, unchanged wording>
               )
           ```
           Keep the existing not-installed and missing-__version__ branches and the return value (the installed version string) unchanged.
        5. Update the function docstring: comparison is PEP 440 (packaging.version), pre-releases sort below their release, unparseable versions raise OrchestratorError.
        6. Update the two in-code contract comments at lines ~1740 and ~1826 from "fmri-first-level-proc >= 3.1.0" to "fmri-first-level-proc >= 3.2.0".
        7. Retain `import re` (used by discovery helpers at lines ~1280, ~1288).
      </spec>
      <dependencies>none</dependencies>
      <risk>low - isolated function; packaging is a declared dependency after C2 and is vendored by setuptools in the existing env.</risk>
      <rollback>Restore _parse_version and the tuple comparison from git (git checkout -p orchestrator_utils.py for Section B hunks).</rollback>
    </change>

    <change id="C2" priority="P0" source_item="F6 (P0: packaging dependency), D1">
      <file path="environment.yaml" action="modify" />
      <description>Declare packaging as an explicit dependency and pin the upstream package to v3.2.0.</description>
      <spec>
        1. Add `- packaging` to the conda `dependencies` list (alphabetical position consistent with surrounding entries).
        2. Change the pip line `"git+https://github.com/tjkeding/fmri_first_level_proc.git@v3.1.0"` to `@v3.2.0`.
        3. Change the preceding comment to "pinned to the minimum supported release (v3.2.0)".
        No environment install or update is executed by this change.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - declarative only; the v3.2.0 tag must exist upstream before the env is rebuilt (delivered by the upstream implementation of this CR).</risk>
      <rollback>git checkout environment.yaml.</rollback>
    </change>

    <change id="C3" priority="P1" source_item="F6 (P1: import guard), D5">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Fail fast with a clear error when guarded upstream imports failed, before any per-session work references the guarded exception names; interpolate the minimum version in both error messages; update header metadata.</description>
      <spec>
        1. Add `MIN_UPSTREAM_VERSION` to the `from orchestrator_utils import (...)` list (lines ~55-92).
        2. In `process_participant` (line ~131), immediately after the docstring and before the "Step 0" block (line ~170), insert:
           ```python
           if _UPSTREAM_IMPORT_ERROR is not None:
               raise OrchestratorError(
                   f"Failed to import fmri_first_level_proc >= {MIN_UPSTREAM_VERSION} "
                   f"modules: {_UPSTREAM_IMPORT_ERROR}"
               )
           ```
        3. In `main()` (line ~1040), change the literal "Failed to import fmri_first_level_proc v3.1.0+ modules: " to an f-string "Failed to import fmri_first_level_proc >= {MIN_UPSTREAM_VERSION} modules: ".
        4. Update the comment above the guarded import block (line ~41) from "v3.1.0+ upstream modules" to "upstream modules (>= MIN_UPSTREAM_VERSION)".
        5. Header (lines 1-25): replace "run concatenation" (line 13) with "per-run collection"; change "Version: 3.1" to "Version: 4.0"; change "Last updated: 03/16/26" to "Last updated: 10/01/26".
        Leave the unguarded imports (lines 37-39) and the guarded try/except (lines 41-51) structurally unchanged.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - additive guard; existing main() guard behaviour preserved.</risk>
      <rollback>Remove the inserted guard and revert the two message strings.</rollback>
    </change>

    <change id="C4" priority="P0" source_item="F5 (P0 completed_successfully; P1 _psc exclusion), F8 (P0 inner rename), D4">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Ground completed_successfully in existence of the frozen primary upstream output for both QC builders, exclude PSC intermediates from NIfTI counts, and rename the inner upstream summary key to upstream_qc_summary.</description>
      <spec>
        1. Add a module constant at the head of the first-level QC section (immediately above `def compute_first_level_qc`, line ~2844):
           ```python
           # Primary GLM output per analysis type; names are part of the
           # fmri_first_level_proc >= 3.2.0 output contract.
           PRIMARY_OUTPUT_SUFFIX = {
               "task_act": "_concat_bucket_stats.nii.gz",
               "task_conn": "_concat_LSS.nii.gz",
               "rest_conn": "_concat_residual_dtseries.nii.gz",
           }
           ```
        2. Add a private helper directly below the constant:
           ```python
           def _primary_output_ok(analysis_type, out_dir, out_file_pre):
               """Return True iff the primary upstream output exists and is non-empty."""
               suffix = PRIMARY_OUTPUT_SUFFIX.get(analysis_type)
               if suffix is None:
                   return False
               path = os.path.join(str(out_dir), f"{out_file_pre}{suffix}")
               return os.path.isfile(path) and os.path.getsize(path) > 0
           ```
        3. `compute_first_level_qc(analysis_name, analysis_type, out_dir, sub_id, out_file_pre, logger, error_msg=None)` (lines ~2844-2948), signature unchanged:
           a. NIfTI glob filter: count only files ending ".nii.gz" that do NOT end with "_psc.nii.gz" and have size &gt; 0; use this filtered list for n_nifti_outputs.
           b. completed_successfully = `_primary_output_ok(analysis_type, out_dir, out_file_pre)` (replacing the any-non-empty-NIfTI criterion).
           c. Rename the dict key "upstream_qc" to "upstream_qc_summary" (value unchanged: parsed qc_summary.json or None).
           d. Docstring: replace the "Expected outputs by type" block with the three primary filenames from PRIMARY_OUTPUT_SUFFIX and state that PSC intermediates are excluded from n_nifti_outputs.
        4. `build_first_level_qc_from_result` (lines ~2951-2973), new signature:
           `def build_first_level_qc_from_result(analysis_name, analysis_type, sub_id, result, out_dir, out_file_pre):`
           a. completed_successfully = `_primary_output_ok(analysis_type, out_dir, out_file_pre)`.
           b. n_nifti_outputs = count of `result.output_files` entries whose str() ends with ".nii.gz" and not with "_psc.nii.gz".
           c. output_files listing: unchanged (sorted basenames of all result.output_files).
           d. Rename "upstream_qc" key to "upstream_qc_summary" (value: dict(result.qc_summary) as currently).
           e. Docstring: document the two new parameters and the completion criterion.
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - changes QC semantics; a renamed or relocated upstream primary file would mark analyses incomplete (intended fail-closed behaviour; names frozen in upstream contract).</risk>
      <rollback>git checkout -p orchestrator_utils.py for the QC section hunks.</rollback>
    </change>

    <change id="C5" priority="P0" source_item="F8 (P0 outer rename), F1 (P1 qc_flagged)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Rename the per-analysis outer QC key to qc and expose the upstream warning channel as qc_flagged with session-level aggregates.</description>
      <spec>
        In `consolidate_session_qc` (lines ~2980-3076):
        1. Per-analysis entry (line ~3072): replace `"upstream_qc": outcome.get("fl_qc")` with `"qc": outcome.get("qc")`.
        2. Add per-analysis field `"qc_flagged": bool(outcome.get("qc_flagged", False))`, placed after "upstream_status".
        3. Session block (lines ~3051-3058): add
           `"n_analyses_flagged": sum(1 for o in analysis_outcomes if o.get("qc_flagged")),`
           `"any_analysis_flagged": any(o.get("qc_flagged") for o in analysis_outcomes),`
           after "n_analyses_succeeded" (use the same iterable name the function already uses for outcomes).
        4. Docstring (lines ~3002-3004): replace fl_qc with qc in the list of consumed outcome keys; add qc_flagged; describe the qc.upstream_qc_summary nesting and the two session aggregates.
        Session status derivation is untouched.
      </spec>
      <dependencies>C4</dependencies>
      <risk>medium - schema change of the consolidated QC JSON consumed downstream; documented in C16/C17 as a v4.0 schema change.</risk>
      <rollback>Revert the key name and remove the three added fields.</rollback>
    </change>

    <change id="C6" priority="P0" source_item="F5 (P0 try/except), F1, F8 (P2 local rename), F9, F10 (orchestrator comment)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Harden Step 12 per-analysis dispatch: dispatch lookup inside the try, qc_flagged on the outcome, guarded QC builder with the new signature, schema-consistent outcome key, chdir constraint comment, flagged tag in the summary log.</description>
      <spec>
        In `_process_session` Step 12 (lines ~804-917):
        1. Move `run_fn = DISPATCH[atype]` (line ~850) inside the per-analysis `try:` (line ~856) as its first statement, so an unknown type is captured by the existing exception handling and recorded as a failed analysis rather than aborting the session loop.
        2. At the `os.chdir(...)` call (lines ~834-835) add the comment:
           `# os.chdir mutates process-global state: the orchestrator must run analyses`
           `# sequentially in a single process (no thread-level parallelism over analyses).`
        3. Outcome dict (lines ~885-895): add `"qc_flagged": (result is not None and result.status == "warning"),`.
        4. QC block (lines ~897-913): rename local `fl_qc` to `qc`. On the success path replace the builder call with:
           ```python
           try:
               qc = build_first_level_qc_from_result(
                   name, atype, f"sub-{sub_id}", result, cfg.out_dir, cfg.out_file_pre
               )
           except Exception as e:
               logger.error("QC builder failed for analysis '%s': %s", name, e)
               qc = {
                   "analysis_name": name,
                   "analysis_type": atype,
                   "completed_successfully": False,
                   "error": "QC builder failed",
               }
           ```
           (`cfg` is the ResolvedConfig returned by the build/load step already in scope at line ~826; use the identifier already bound there.)
           On the failure path keep `compute_first_level_qc(name, atype, fl_out_dir, f"sub-{sub_id}", <out_file_pre>, logger, error_msg=...)` with its existing arguments.
        5. Replace `outcome["fl_qc"] = fl_qc` with `outcome["qc"] = qc`.
        6. `process_participant` session summary (lines ~283-309): for each analysis outcome with `o.get("qc_flagged")`, append " [FLAGGED]" to the per-analysis log line after the [OK]/[FAILED] tag.
        `_derive_session_status` (lines 95-128) is unchanged.
      </spec>
      <dependencies>C4, C5</dependencies>
      <risk>low - localized; behaviour for successful analyses unchanged apart from schema keys.</risk>
      <rollback>git checkout -p orchestrate_first_level.py for Step 12 and summary hunks.</rollback>
    </change>

    <change id="C7" priority="P0" source_item="F2 (P0 orchestrator), D2">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Retire orchestrator-side run concatenation and mask intersection; upstream owns concatenation for multi-run task GLMs under the invariant.</description>
      <spec>
        orchestrator_utils.py:
        1. Delete Section I (lines ~2136-2284): `concatenate_bolds`, `concatenate_tabular_files`, `concatenate_task_timing`, including the section banner.
        2. Delete Section J2 (lines ~2350-2392): `compute_mask_intersection`, including its banner.
        3. Keep Section J `apply_smoothing` (line ~2286) unchanged.
        4. Remove any module-docstring or section-index lines that enumerate the deleted functions.
        orchestrate_first_level.py:
        5. Remove `concatenate_bolds`, `concatenate_tabular_files`, `concatenate_task_timing`, `compute_mask_intersection` from the `from orchestrator_utils import (...)` list (lines ~82-86).
      </spec>
      <dependencies>C8 (call sites removed in the same build pass; apply C8 before C7 deletes the definitions)</dependencies>
      <risk>medium - removes public helpers; any external caller breaks (none known; tests updated in C18).</risk>
      <rollback>Restore the deleted sections and imports from git.</rollback>
    </change>

    <change id="C8" priority="P0" source_item="F2 (P0 orchestrator), D1, D2, D6">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Refactor per-run processing and Step 11 so all tasks emit run-aligned per-run lists with commit-on-success semantics and an equal-length assertion; no concat directory.</description>
      <spec>
        In `_process_session`:
        1. Docstring: replace step "5. Concatenate or collect runs" with "5. Collect per-run files (concatenation for multi-run task GLMs is performed upstream)".
        2. Remove `concat_dir` definition (line ~378) and its entry in the makedirs list (line ~383).
        3. Remove `should_concat = task_def.get("concatenate_runs", not is_rest)` (line ~535).
        4. Remove `per_run_tr_counts` and `per_run_timing_tr_counts` (lines ~557-558) and their appends (lines ~609, ~677). Keep the `n_trs &lt;= 0` raise (lines ~603-608).
        5. Commit-on-success inside the per-run `try` (lines ~567-681):
           a. Initialize run-local holders at the top of the try: `run_motion = run_csf = run_wm = run_gs = run_timing = None`.
           b. Replace `per_run_motions.append(motion_path)` with `run_motion = motion_path`; replace the csf/wm/gs appends with `run_csf = csf_path`, `run_wm = wm_path`, `run_gs = gs_path`; replace `per_run_timings.append(timing_path)` with `run_timing = timing_path`.
           c. Step 10 condition: change `if not is_rest and rd["events_path"] is not None:` to `if not is_rest:` and, as its first statement, `if rd["events_path"] is None: raise FileNotFoundError(f"No events file for {run_label}")`.
           d. After the Step 10 block and still inside the try, commit all outputs together:
              ```python
              per_run_bolds.append(trimmed_bold)
              per_run_motions.append(run_motion)
              if is_rest:
                  per_run_csf.append(run_csf)
                  per_run_wm.append(run_wm)
                  per_run_gs.append(run_gs)
              else:
                  per_run_timings.append(run_timing)
              per_run_masks.append(rd["mask_path"])
              per_run_labels.append(run_label)
              ```
           The `preproc_qc_by_run` writes are unchanged. The except branches (lines ~683-696) are unchanged.
        6. Step 11 (lines ~716-795): delete the concatenation branch (lines ~721-769) and the if/else; the collection path applies to every task:
           a. Banner comment: "Step 11: Collect per-run files".
           b. Equal-length assertion before smoothing:
              ```python
              if is_rest:
                  aligned = {"bolds": per_run_bolds, "motions": per_run_motions,
                             "csf": per_run_csf, "wm": per_run_wm, "gs": per_run_gs,
                             "masks": per_run_masks, "labels": per_run_labels}
              else:
                  aligned = {"bolds": per_run_bolds, "motions": per_run_motions,
                             "timings": per_run_timings,
                             "masks": per_run_masks, "labels": per_run_labels}
              lengths = {k: len(v) for k, v in aligned.items()}
              if len(set(lengths.values())) != 1:
                  raise OrchestratorError(
                      f"Per-run file lists are misaligned for task '{task_label}' "
                      f"sub-{sub_id} {ses_label}: {lengths}"
                  )
              ```
           c. Keep the existing per-run smoothing loop (lines ~776-787) unchanged.
           d. Build processed_files:
              ```python
              if is_rest:
                  processed_files[task_label] = {
                      "bolds": per_run_bolds,
                      "motions": per_run_motions,
                      "csf": per_run_csf,
                      "wm": per_run_wm,
                      "gs": per_run_gs if per_run_gs else None,
                  }
              else:
                  processed_files[task_label] = {
                      "bolds": per_run_bolds,
                      "motions": per_run_motions,
                      "timings": per_run_timings,
                  }
              ```
           e. Log line: `logger.info("Step 11: Collecting per-run files for task '%s' (%d run(s)).", task_label, len(per_run_bolds))`.
        7. Remove the `task_prefix` assignment (line ~719); its only consumers are in the deleted concatenation branch.
      </spec>
      <dependencies>none</dependencies>
      <risk>high - core data-flow change; correctness depends on C9 rendering list paths and on upstream >= 3.2.0 concatenating runs per the invariant.</risk>
      <rollback>git checkout -p orchestrate_first_level.py for the per-run loop and Step 11 hunks.</rollback>
    </change>

    <change id="C9" priority="P0" source_item="F2 (P0 orchestrator), F4, D1">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Render per-run list paths for task analyses and force remove_previous so stale outputs cannot satisfy the completion criterion.</description>
      <spec>
        In `build_first_level_config` (lines ~3083-3217):
        1. Directly after `block["out_dir"] = ...` (line ~3168) add `block["remove_previous"] = True`.
        2. Task branch (lines ~3177-3182): set
           ```python
           block["paths"] = {
               "scan_paths": list(pf["bolds"]),
               "motion_paths": list(pf["motions"]),
               "task_timing_paths": list(pf["timings"]),
           }
           ```
           (use the existing variable name for the task's processed_files entry).
        3. Rest branch (lines ~3184-3191): unchanged.
        4. Docstring (lines ~3101-3107): state that task analyses receive run-aligned per-run lists with run-local onsets; that upstream (>= 3.2.0) concatenates runs and fits a single model per the invariant; that remove_previous is forced to true.
        5. Docstring processed_files description (line ~3104) for task entries becomes {"bolds": [paths], "motions": [paths], "timings": [paths]}.
      </spec>
      <dependencies>C8</dependencies>
      <risk>medium - interface change with upstream; requires upstream v3.2.0.</risk>
      <rollback>Revert the paths block and remove the remove_previous line.</rollback>
    </change>

    <change id="C10" priority="P0" source_item="D2">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Reject the retired orchestrator-config key concatenate_runs with an actionable migration error.</description>
      <spec>
        In `load_orchestrator_config` tasks loop (lines ~3321-3332), for each task definition:
        ```python
        if "concatenate_runs" in task_def:
            raise OrchestratorError(
                f"Task '{label}': 'concatenate_runs' is no longer supported "
                f"(removed in orchestrator 4.0). Runs are passed to "
                f"fmri_first_level_proc as per-run lists; multi-run task "
                f"analyses are concatenated upstream. Remove this key from "
                f"the orchestrator config."
            )
        ```
        Use the loop's existing task-label identifier. Update the function docstring's validation list.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - fail-fast validation; breaks configs that still carry the key (intended).</risk>
      <rollback>Remove the check.</rollback>
    </change>

    <change id="C11" priority="P0" source_item="F3 (P0 orchestrator)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Reject legacy extraction keys in proc templates so they cannot be silently ignored by upstream.</description>
      <spec>
        In `validate_proc_template` (line ~3491), within the per-analysis loop, adjacent to the censor-key rejection (lines ~3545-3559):
        ```python
        extraction = block.get("extraction")
        if isinstance(extraction, dict):
            legacy = sorted(k for k in ("extract", "extract_labels", "extract_stat") if k in extraction)
            if legacy:
                raise OrchestratorError(
                    f"Proc template analysis '{name}' uses legacy extraction keys "
                    f"{legacy}. These are not read by fmri_first_level_proc >= "
                    f"{MIN_UPSTREAM_VERSION}; specify outputs via 'stat_buckets'."
                )
        ```
        Use the loop's existing analysis-name and block identifiers. Add the rule to the docstring's checks list.
      </spec>
      <dependencies>C1</dependencies>
      <risk>low - fail-fast validation.</risk>
      <rollback>Remove the check.</rollback>
    </change>

    <change id="C12" priority="P2" source_item="F7 (P2 docstring), assumption 1">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Document the BIDS TSV timing output and mark the retained concat upload walk as legacy-only.</description>
      <spec>
        1. `format_task_timing` (line ~2040) docstring: add "Output is a BIDS-compliant tab-separated file with columns onset, duration, trial_type; onsets are run-local (relative to the first retained volume of the run)."
        2. `enumerate_upload_targets` concat walk (lines ~144-160): add comment "# Legacy layout (orchestrator &lt; 4.0): concat/ exists only in sessions migrated from pre-4.0 archives. Retained for migration fidelity."
        3. `migrate_session_from_archive` concat mapping (lines ~559, ~628): add the same one-line legacy comment.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - comments and docstrings only.</risk>
      <rollback>Remove the added lines.</rollback>
    </change>

    <change id="C13" priority="P0" source_item="F2 (P0 orchestrator), F4, D1, D3">
      <file path="example_proc_config.yaml" action="modify" />
      <description>Align the example proc template with list-form paths, per-run PSC wording, and forced remove_previous.</description>
      <spec>
        1. Task paths blocks (lines ~48-53 and ~100-105): replace scan_path / task_timing_path / motion_path with
           ```yaml
           paths:
             scan_paths: ["/placeholder/run-01_bold.nii.gz", "/placeholder/run-02_bold.nii.gz"]       # overridden by orchestrator (per-run)
             task_timing_paths: ["/placeholder/run-01_timing.tsv", "/placeholder/run-02_timing.tsv"]  # overridden; run-local onsets
             motion_paths: ["/placeholder/run-01_motion.1D", "/placeholder/run-02_motion.1D"]         # overridden by orchestrator (per-run)
           ```
           preserving the file's existing indentation and comment style.
        2. remove_previous lines (~63, ~115, ~180): comment "forced to true by the orchestrator at render time".
        3. scale_to_psc lines (~69, ~121, ~188): comment "scale each run to percent signal change (per-run mean) before regression (recommended)".
      </spec>
      <dependencies>C9</dependencies>
      <risk>low - example file.</risk>
      <rollback>git checkout example_proc_config.yaml.</rollback>
    </change>

    <change id="C14" priority="P0" source_item="D2">
      <file path="example_orchestrator_config.yaml" action="modify" />
      <description>Remove the retired concatenate_runs key and update the upstream version reference.</description>
      <spec>
        1. Delete the `concatenate_runs:` lines (~89, ~94) and any comment line describing that key.
        2. Line ~64: change "fmri_first_level_proc >= 3.1.0" to "fmri_first_level_proc >= 3.2.0".
      </spec>
      <dependencies>C10</dependencies>
      <risk>low - example file.</risk>
      <rollback>git checkout example_orchestrator_config.yaml.</rollback>
    </change>

    <change id="C15" priority="P2" source_item="F11, D1, D3, F4">
      <file path="proc_config_final.yaml" action="modify" />
      <description>Align the local real-world proc config with TSV timing, trial_type naming, list-form paths, per-run PSC wording, and forced remove_previous (local-only file, not published).</description>
      <spec>
        1. Task paths (lines ~49-51, ~156-158): list-form scan_paths / task_timing_paths / motion_paths placeholders as in C13, with ".tsv" timing extensions.
        2. Line ~59: "CONDITION column in timing CSV" becomes "trial_type column in timing TSV".
        3. scale_to_psc comments (~69, ~176, ~273): per-run wording as in C13.
        4. remove_previous comments (~63, ~170, ~265): as in C13.
      </spec>
      <dependencies>C13</dependencies>
      <risk>low - local config.</risk>
      <rollback>git checkout proc_config_final.yaml (or restore from local copy if untracked).</rollback>
    </change>

    <change id="C16" priority="P0" source_item="F2 (README), F7 (P1), F8 (P0 docs), D2, D3">
      <file path="README.md" action="modify" />
      <description>Bring README in line with per-run collection, the regression invariant, the v4.0 QC schema, and the timing format transition.</description>
      <spec>
        1. Pipeline tree (lines ~13, ~37): Step 11 becomes "Collect per-run files (multi-run concatenation performed upstream)".
        2. Timing format (line ~205): BIDS TSV with onset/duration/trial_type, run-local onsets.
        3. Step 11 section (lines ~207-209): rewrite as per-run collection; state that the orchestrator no longer concatenates runs and that concatenate_runs is rejected.
        4. Step 12 section (line ~211): add
           a. "Per-run PSC scaling and run-boundary baselines are applied by fmri_first_level_proc >= 3.2.0."
           b. The invariant, verbatim (see header of this plan).
           c. Regeneration advisory: "Multi-run task_act and task_conn outputs produced by orchestrator versions earlier than 4.0 with scale_to_psc enabled were scaled by the grand mean of the pre-concatenated series rather than per run, and should be regenerated."
        5. Output tree (lines ~229, ~235-239): `_timing.csv` becomes `_timing.tsv`; remove the concat/ subtree; add a note that concat/ appears only in sessions migrated from pre-4.0 archives.
        6. QC schema (lines ~275, ~277, ~285, ~300, ~344): analyses.{name}.upstream_qc becomes analyses.{name}.qc; inner key upstream_qc_summary; add qc_flagged per analysis and n_analyses_flagged / any_analysis_flagged per session; document completed_successfully as primary-output-based with the three frozen filenames.
        7. Upload targets (line ~312): mark concat/ as legacy-only.
        8. concatenate_runs row (line ~375): remove from the config table; add a one-line note that the key is rejected from 4.0.
        9. Function tables (lines ~426, ~449-450): remove the four retired functions; add PRIMARY_OUTPUT_SUFFIX; update build_first_level_qc_from_result signature.
        10. processed_files format (lines ~457-462): task entry becomes {"bolds": [...], "motions": [...], "timings": [...]}.
        11. Add a "Version 4.0 output changes" subsection containing:
            a. Timing transition: sessions processed before 4.0 contain `_timing.csv` (comma-delimited, CONDITION column); sessions from 4.0 contain `_timing.tsv` (tab-delimited, trial_type column). Deprecation warning: the upstream legacy CSV timing reader is scheduled for removal in fmri_first_level_proc 4.0.0; pre-4.0 timing CSVs should be regenerated or converted before use with that release.
            b. QC schema note: pre-4.0 consolidated QC JSON uses analyses.{name}.upstream_qc (with nested upstream_qc); 4.0 and later use analyses.{name}.qc with nested upstream_qc_summary, plus qc_flagged and session-level flag counts. Downstream parsers must branch on provenance.orchestrator_version.
        12. Update every remaining "3.1.0" upstream minimum reference to "3.2.0".
      </spec>
      <dependencies>C4, C5, C8, C9, C10</dependencies>
      <risk>low - documentation.</risk>
      <rollback>git checkout README.md.</rollback>
    </change>

    <change id="C17" priority="P0" source_item="F7 (P1), F8 (P0 docs), D2, F3">
      <file path="INPUT_SPECIFICATION.md" action="modify" />
      <description>Update input and output specification for list-form paths, rejected keys, the v4.0 QC schema, and the timing transition.</description>
      <spec>
        1. Section 2.2 tasks block (lines ~51, ~54): remove concatenate_runs from the field table; add a note that it is rejected.
        2. Section 3 overridden fields (lines ~166-168): task paths are scan_paths / motion_paths / task_timing_paths (lists, run-aligned, run-local onsets); remove_previous listed as forced true.
        3. Section 3 cross-validation rules (line ~200 list) and Section 9 proc-template table (lines ~528-545): add the legacy extraction-key rejection with its error message. Section 9 orchestrator table (lines ~497-527): add the concatenate_runs rejection with its error message.
        4. Section 5 upload target (line ~307): concat/ legacy-only.
        5. Section 8 / 8b: add the timing format note (BIDS TSV, run-local onsets) and the pre-4.0 CSV transition with the upstream deprecation warning, consistent with C16 item 11a.
        6. Section 10.1 (lines ~547-632): JSON example and key-field list use analyses.{name}.qc, qc.upstream_qc_summary, qc_flagged, session.n_analyses_flagged, session.any_analysis_flagged; provenance example orchestrator_version "4.0", fmri_first_level_proc_version "3.2.0"; the closing sentence about upstream_qc becomes qc.upstream_qc_summary; add the pre-4.0 schema note.
        7. Section 10.3 processed_files (lines ~647-656): task entry list form.
        8. Lines ~443, ~600, ~612, ~625, ~631: replace remaining upstream_qc references per item 6.
      </spec>
      <dependencies>C4, C5, C8, C9, C10, C11</dependencies>
      <risk>low - documentation.</risk>
      <rollback>git checkout INPUT_SPECIFICATION.md.</rollback>
    </change>

    <change id="C18" priority="P0" source_item="D2">
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <file path="tests/test_coverage_gaps_v2.py" action="modify" />
      <file path="tests/test_cr_implementation.py" action="modify" />
      <file path="tests/test_cr_implementation_extended.py" action="modify" />
      <file path="tests/test_motion_and_qc.py" action="modify" />
      <description>Remove tests and imports of the retired concatenation and mask-intersection helpers.</description>
      <spec>
        Delete the following classes/tests and the corresponding names from each file's import list and module docstring index:
        - test_coverage_gaps.py: TestConcatenateBolds (line ~458), TestConcatenateTabularFiles (line ~499); imports at lines ~13-14, ~59-61. Retain TestEnumerateUploadTargets.test_concat_excludes_nii_gz_single_level (legacy walk retained).
        - test_coverage_gaps_v2.py: TestConcatenateTabularFilesExtended (line ~734), TestComputeMaskIntersectionEdgeCases (line ~856); import at line ~15.
        - test_cr_implementation.py: TestComputeMaskIntersection (line ~409); imports at lines ~33, ~39-40.
        - test_cr_implementation_extended.py: TestComputeMaskIntersectionExtended (line ~813), TestForceRecomputeAFNI.test_concatenate_bolds_force_recompute, TestForceRecomputeNonAFNI.test_concatenate_tabular_files_force_recompute; imports at lines ~6, ~14, ~16, ~43-51.
        - test_motion_and_qc.py: TestConcatenateTaskTimingGuards (line ~489); import at line ~24.
        Retain TestEnumerateUploadTargets.test_targets_sorted_by_s3_key_suffix and all test_legacy_routing_migration.py tests unchanged.
      </spec>
      <dependencies>C7</dependencies>
      <risk>low - removes tests of deleted code only.</risk>
      <rollback>git checkout the listed test files.</rollback>
    </change>

    <change id="C19" priority="P1" source_item="F1 (mocks), F8 (P1 tests), F5, F6, D1, D2, D4">
      <file path="tests/conftest.py" action="modify" />
      <file path="tests/test_simulated_pipeline.py" action="modify" />
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <file path="tests/test_coverage_gaps_v2.py" action="modify" />
      <file path="tests/test_cr_implementation.py" action="modify" />
      <file path="tests/test_cr_implementation_extended.py" action="modify" />
      <file path="tests/test_motion_and_qc.py" action="modify" />
      <file path="tests/test_preprocessing.py" action="modify" />
      <description>Re-express fixtures and assertions to the new contracts (list paths, processed_files keys, QC schema, upstream status vocabulary, primary-output completion) without weakening any postcondition.</description>
      <spec>
        conftest.py:
        1. sample_orchestrator_config and sim_orchestrator_config: delete every "concatenate_runs" entry (lines ~177, ~181, ~301, ~306).
        2. sample_proc_template and sim_proc_template task paths (lines ~100-102, ~371-373, ~404-406): list-form scan_paths / task_timing_paths / motion_paths placeholders (".tsv" timing).
        3. sample_processed_files_task (line ~238): {"bolds": [two run paths], "motions": [two .1D paths], "timings": [two .tsv paths]}.
        test_simulated_pipeline.py:
        4. _make_mock_dispatch: return status "success"; write the primary output as `os.path.join(ns.out_dir, f"{ns.out_file_pre}{PRIMARY_OUTPUT_SUFFIX[analysis_type]}")` (import PRIMARY_OUTPUT_SUFFIX from orchestrator_utils) instead of `_stats.nii.gz`; update the helper docstring (status "success", primary filename).
        5. TestHappyPath.test_full_session_nback_and_rest: replace the concat isdir assertion with `assert not os.path.isdir(os.path.join(session_out, "concat"))`.
        6. TestHappyPath.test_nback_cue_relabeling_produces_13_conditions: read all `*task-nback*_timing.tsv` files from the session preproc dir (assert at least 2, one per run), take the union of trial_type values across runs, keep every existing condition assertion (no cue/dummy/Cue, instruction present, exactly 13).
        7. TestHappyPath.test_concatenated_timing_onset_offsets: rename to test_per_run_timing_onsets_are_run_local; for each per-run nback timing TSV assert onsets are sorted non-decreasing, min onset &gt;= 0, and max onset &lt; (number of retained TRs in that run) * TR, using the TR and NSS count available from the simulated fixture.
        8. TestEdgeCases.test_zero_nss_trs: read per-run `_timing.tsv` from the preproc dir (assert at least one exists), assert `timing_df["onset"].min() &gt;= 0` with sep="\t".
        9. TestEdgeCases.test_single_run_no_concatenation: rename to test_single_run_collected_per_run; assert the log contains "Step 11: Collecting per-run files" and that no concat directory exists under the session output.
        10. TestQCValidation.test_consolidated_qc_with_first_level: `aqc["upstream_qc"]["pct_censored"]` becomes `aqc["qc"]["pct_censored"]`; add `assert aqc["qc"]["upstream_qc_summary"]["pct_censored"] == 5.2`, `assert aqc["qc_flagged"] is False`, `assert aqc["qc"]["completed_successfully"] is True`.
        11. TestQCValidation.test_consolidated_qc_json_structure: update any task path keys to list form.
        test_coverage_gaps.py:
        12. TestBuildFirstLevelQcFromResult: call with the new signature (analysis_name, analysis_type, sub_id, result, out_dir, out_file_pre) using tmp_path; test_basic_fields writes the primary file and asserts completed_successfully True; test_n_nifti_outputs_counts_only_nii_gz includes a `_psc.nii.gz` entry and asserts it is excluded from the count; test_upstream_qc_passthrough asserts the "upstream_qc_summary" key.
        13. TestBuildFirstLevelConfigAdditional.test_task_conn_paths_populated: assert paths keys == {"scan_paths", "motion_paths", "task_timing_paths"} and values equal the input lists.
        14. TestVerifyUpstreamVersion: keep all cases unchanged (no test references _parse_version; "3.1" vs "3.1.0" compares equal under packaging, so test_differing_version_tuple_lengths_compare_correctly holds as written).
        test_coverage_gaps_v2.py:
        15. Line ~987 and TestBuildFirstLevelConfigEdgeCases.test_tr_mismatch_raises: processed_files task entries in list form {"bolds": [...], "motions": [...], "timings": [...]}.
        test_cr_implementation.py:
        16. TestConsolidateSessionQC (test_basic_structure, test_analysis_entries, test_empty_outcomes): outcome key "fl_qc" becomes "qc"; asserted entry key "upstream_qc" becomes "qc"; add assertions for qc_flagged per analysis and session n_analyses_flagged / any_analysis_flagged.
        17. TestStructuredRunLossWarning.test_warning_format_in_simulated_pipeline: delete concatenate_runs from its config.
        test_cr_implementation_extended.py:
        18. TestProcessParticipantSessionResults (3 tests): delete concatenate_runs; task path keys to list form.
        19. TestConsolidateSessionQCExtended.test_analysis_without_fl_qc_key: rename to test_analysis_without_qc_key; asserts entry["qc"] is None.
        20. TestConsolidateSessionQCExtended.test_upstream_status_propagated: use "warning" (real upstream value) and assert it propagates; test_upstream_status_defaults_to_none_when_absent unchanged apart from key names.
        test_motion_and_qc.py:
        21. TestDiscoverLocalMmpsFiles.TASK_DEFS (lines ~380-382): delete concatenate_runs.
        22. TestGoldenFilePhase2.test_processed_files_format_unchanged: assert task keys {"bolds","motions","timings"}, each a list, every motion path ends with ".1D"; rest assertions unchanged.
        test_preprocessing.py:
        23. orch_config_with_task_conn: delete concatenate_runs.
        24. proc_template_no_censor, proc_template_with_task_conn, processed_files_task_no_censor fixtures: list-form keys.
        25. TestBuildFirstLevelConfigRefactored.test_task_paths_exact_keys: exact key set {"scan_paths","motion_paths","task_timing_paths"}; add assertion that block["remove_previous"] is True.
        26. TestComputeFirstLevelQCRefactored (4 tests): "upstream_qc" becomes "upstream_qc_summary"; tests that asserted completed_successfully from an arbitrary NIfTI now write the primary-output filename for the analysis type.
        Tests are edited, not run. New coverage for: concatenate_runs rejection, legacy extraction-key rejection, InvalidVersion and pre-release ordering, D6 misalignment assertion, missing-events-file skip, QC-builder failure fallback, DISPATCH unknown-type capture, and [FLAGGED] summary tag is routed to /test.
      </spec>
      <dependencies>C4, C5, C6, C8, C9, C10, C18</dependencies>
      <risk>medium - large fixture surface; risk of assertion weakening is controlled by the explicit re-expressions above.</risk>
      <rollback>git checkout tests/.</rollback>
    </change>

  </changes>
  <execution_order>C1, C2, C3, C4, C5, C6, C8, C7, C9, C10, C11, C12, C13, C14, C15, C16, C17, C18, C19</execution_order>
</implement_plan>
```

## Routing note

The upstream items from this CR are handled by the `/implement` run in the fmri_first_level_proc project, not by this plan:

- per-run list input with per-run PSC and `-concat`, under the invariant
- unknown-key rejection in `load_and_validate`
- `cwd` parameter
- freezing the primary output filenames
- release of the v3.2.0 tag

This plan's runtime correctness depends on that upstream release.
