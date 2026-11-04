<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-09-30T14:21:02-04:00" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_brainstorm_20260930_120000.md" mode="brainstorm" key_items="8" />
  </input_reports>
  <resolved_decisions>
    <decision id="G1-Q1">scale_to_psc: true is set explicitly in every analysis block of example_proc_config.yaml (task_act, task_conn, rest_conn).</decision>
    <decision id="G1-Q2">Upstream FirstLevelResult.status "warning" maps to orchestrator outcome status "success". Outcome dicts gain upstream_status and warnings. _derive_session_status is unchanged. The run-summary [OK] line appends the high-severity warning count when it is greater than zero.</decision>
    <decision id="G1-Q3">Success-path fl_qc keeps the existing key set, populated from FirstLevelResult. Warnings are not duplicated into fl_qc. fl_qc gating (qc.first_level.enabled and not --skip-qc) is unchanged.</decision>
    <decision id="G1-Q4">Error path records error (str(e)), error_type (exception class name), and error_context (JSON-safe copy of e.context, or {} when the exception carries no context). No exit code is recorded. Every exception family is isolated to its analysis. GuardError logs at critical, other upstream families at error, the generic fallback via logger.exception. The SystemExit catch is removed.</decision>
    <decision id="G1-Q5">This build updates version and CLI references in code and config templates only. README.md and INPUT_SPECIFICATION.md are handled by /document.</decision>
    <decision id="G1-Q6">The local, gitignored proc_config_final.yaml receives the same schema migration as the template (stat_buckets replaces extract/extract_labels/extract_stat in task_act; scale_to_psc: true in all three blocks). All other content in that file is left unchanged. Overwriting this file is authorized.</decision>
    <decision id="G1-Q7">Orchestrator __version__ becomes "4.0". environment.yaml pins upstream to the v3.1.0 tag. A runtime upstream version check (minimum 3.1.0) runs at startup, including under --dry-run, and the new upstream imports are guarded so an older install produces that diagnostic rather than a bare ImportError.</decision>
    <decision id="G1-Q8">No further real-world validation run is planned; validation relies on the test suite.</decision>
    <decision id="A-1">Preflight (preflight_tool_environments) remains skipped under --dry-run, matching the current verify_afni_installation behavior. The version check is not skipped under --dry-run.</decision>
    <decision id="A-2">load_and_validate failures (ConfigError, InputError) are not caught inside the dispatch loop. They propagate to the existing session-level "except Exception" handler in process_participant, which marks the session failed. This matches the report's scope (S1 is a mechanical migration).</decision>
  </resolved_decisions>
  <changes>
    <change id="C1" priority="P0" source_item="S2, S3, G1-Q7 (7a, 7c)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Update module-level imports and version constant: add guarded upstream imports for the typed exception classes and the preflight function, drop the verify_afni_installation import, add the new orchestrator_utils helpers, and bump the orchestrator version.</description>
      <spec>
1. Line 41: change `__version__ = "3.1"` to `__version__ = "4.0"`.
2. Immediately after the existing three `from fmri_first_level_proc...` imports (lines 37-39), add a guarded import block:
   ```python
   # v3.1.0+ upstream modules. Guarded so that an older install reaches the
   # explicit version diagnostic in main() instead of failing at import time.
   try:
       from fmri_first_level_proc.exceptions import (
           ConfigError, InputError, ModelError, GuardError,
           ToolUnavailableError, ToolVersionError, ToolExecutionError,
       )
       from fmri_first_level_proc.runtime import preflight_tool_environments
       _UPSTREAM_IMPORT_ERROR = None
   except ImportError as _import_err:
       _UPSTREAM_IMPORT_ERROR = _import_err
   ```
3. In the `from orchestrator_utils import (...)` block: remove `verify_afni_installation`; add `verify_upstream_version`, `json_safe_context`, and `build_first_level_qc_from_result` (defined in C2).
      </spec>
      <dependencies>C2</dependencies>
      <risk>low - import-surface change only; the guard confines the failure mode of an old install to a controlled diagnostic.</risk>
      <rollback>git checkout orchestrate_first_level.py</rollback>
    </change>

    <change id="C2" priority="P0" source_item="S3, S4, S2 (C), S7, G1-Q2, G1-Q3, G1-Q4, G1-Q5, G1-Q7 (7c)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Remove the single-binary AFNI check, add the upstream version check and two QC helpers, extend the consolidated session QC schema, and update the two in-code version references.</description>
      <spec>
1. Remove `verify_afni_installation` (lines 1156-1186) in full. Rename the section header "Section B: AFNI Check" to "Section B: Upstream Compatibility" and place the new `verify_upstream_version` function there. Keep `import subprocess` (still used elsewhere in the module).

2. Add to Section B:
   ```python
   MIN_UPSTREAM_VERSION = "3.1.0"

   def _parse_version(version_str):
       """Parse a dotted version string into a tuple of ints.

       Each dot-separated component contributes its leading digits; a
       component without leading digits contributes 0 (e.g. "3.1.0.dev0"
       -> (3, 1, 0, 0)).
       """
       parts = []
       for comp in str(version_str).split("."):
           m = re.match(r"\d+", comp)
           parts.append(int(m.group()) if m else 0)
       return tuple(parts)

   def verify_upstream_version(logger, min_version=MIN_UPSTREAM_VERSION):
       """Verify the installed fmri_first_level_proc meets the minimum version.

       Raises
       ------
       OrchestratorError
           If fmri_first_level_proc is not importable, exposes no
           __version__, or is older than ``min_version``.
       """
   ```
   Body: `import fmri_first_level_proc` inside try/except ImportError, raising `OrchestratorError("fmri_first_level_proc is not installed. Requires >= {min_version}.")`. Read `found = getattr(fmri_first_level_proc, "__version__", None)`; when None, raise `OrchestratorError` stating the version could not be determined and naming the required minimum. Compare `_parse_version(found)` against `_parse_version(min_version)` after right-padding the shorter tuple with zeros to equal length. When older, raise `OrchestratorError(f"fmri_first_level_proc {found} is installed, but this orchestrator requires >= {min_version}. Update the environment (see environment.yaml).")`. On success, `logger.info("fmri_first_level_proc version: %s (required >= %s)", found, min_version)` and return `found`.

3. Add a new helper (place in Section K, directly before `compute_first_level_qc`):
   ```python
   def json_safe_context(context):
       """Return a JSON-serializable deep copy of an exception context dict.

       Non-serializable leaves (Path, numpy scalars, etc.) are converted via
       str(). Returns {} when ``context`` is None or not a dict.
       """
       if not isinstance(context, dict):
           return {}
       return json.loads(json.dumps(context, default=str))
   ```

4. Add a new function directly after `compute_first_level_qc`:
   ```python
   def build_first_level_qc_from_result(analysis_name, analysis_type, sub_id, result):
       """Build the per-analysis first-level QC dict from an upstream FirstLevelResult.

       Success-path counterpart to compute_first_level_qc(); returns the same
       key set without inspecting the output directory.
       """
   ```
   Returns:
   - `"sub_id"`: sub_id
   - `"analysis_name"`: analysis_name
   - `"type"`: analysis_type
   - `"pct_censored"`: `round(float(v), 2)` where `v = (result.qc_summary or {}).get("pct_censored")`, or None when v is None
   - `"completed_successfully"`: True
   - `"error"`: None
   - `"n_nifti_outputs"`: count of entries in `result.output_files` whose basename ends with ".nii.gz"
   - `"output_files"`: `sorted(os.path.basename(p) for p in result.output_files)`
   - `"upstream_qc"`: `result.qc_summary`

5. `consolidate_session_qc` (lines 2921-3012):
   - Docstring for `analysis_outcomes`: extend the key list to "name, type, status, error, error_type, error_context, upstream_status, warnings, wall_time_seconds, and optionally fl_qc".
   - Per-analysis entry becomes (insertion order as listed):
     ```python
     qc["analyses"][outcome["name"]] = {
         "type": outcome["type"],
         "status": outcome["status"],
         "upstream_status": outcome.get("upstream_status"),
         "error": outcome["error"],
         "error_type": outcome.get("error_type"),
         "error_context": outcome.get("error_context", {}),
         "warnings": outcome.get("warnings", []),
         "wall_time_seconds": outcome["wall_time_seconds"],
         "upstream_qc": outcome.get("fl_qc"),
     }
     ```
   - `n_analyses_succeeded` logic is unchanged (counts status == "success"; upstream "warning" outcomes carry status "success" per G1-Q2).

6. Version references:
   - Line 1718 docstring: "per fmri-first-level-proc >= 2.4.0 input contract" becomes "per fmri-first-level-proc >= 3.1.0 input contract".
   - Line 1804 comment: "per fmri-first-level-proc >= 2.4.0 contract" becomes "per fmri-first-level-proc >= 3.1.0 contract".
      </spec>
      <dependencies>none</dependencies>
      <risk>low - additive helpers plus removal of a function whose only caller is replaced in C5; consolidate_session_qc uses .get() defaults so outcome dicts lacking the new keys still serialize.</risk>
      <rollback>git checkout orchestrator_utils.py</rollback>
    </change>

    <change id="C3" priority="P1" source_item="S6">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Migrate task timing output from legacy CSV (CONDITION, ONSET, DURATION; comma-separated) to BIDS TSV (trial_type, onset, duration; tab-separated).</description>
      <spec>
1. `format_task_timing` (lines 2018-2111):
   - Build `timing_df` with columns `"trial_type"`, `"onset"`, `"duration"` (from `events_df[condition_column]`, `events_df["onset"]`, `events_df["duration"]`).
   - Replace every `timing_df["ONSET"]` reference with `timing_df["onset"]` (onset offset subtraction, negative-onset mask, sort_values key).
   - Write with `timing_df.to_csv(out_path, sep="\t", index=False)`.
   - Docstring: summary line becomes "Convert BIDS events.tsv to first-level timing TSV (BIDS columns: trial_type, onset, duration)."; `out_path` description becomes "Destination path for the output timing TSV."; Returns description "path to timing TSV".
2. `concatenate_task_timing` (lines 2208-2261):
   - Read each file with `pd.read_csv(tp, sep="\t")`.
   - Replace `df["ONSET"]` with `df["onset"]` and the `sort_values("ONSET")` key with `"onset"`.
   - Write with `concat_df.to_csv(out_path, sep="\t", index=False)`.
   - Docstring: "Concatenate task timing TSVs, adjusting onsets for cumulative run lengths."; `timing_paths` described as "Per-run timing TSV paths (trial_type, onset, duration)."; Returns "Path to the concatenated timing TSV."
   - The single-run `shutil.copy2` path is unchanged.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - upstream format detection is content-based (header columns); cached files from earlier runs use the .csv extension and therefore do not collide with the new .tsv paths set in C6.</risk>
      <rollback>git checkout orchestrator_utils.py (shared file with C2; revert the two functions' hunks only)</rollback>
    </change>

    <change id="C4" priority="P0" source_item="S1, S2, S4, G1-Q2, G1-Q3, G1-Q4">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Rewrite the per-analysis dispatch loop (lines 813-871) for the v3.1.0 programmatic API: ResolvedConfig attribute access, FirstLevelResult capture, typed exception handling, and the new outcome and fl_qc population rules.</description>
      <spec>
Replace lines 814-871 (inside the existing `try:` whose `finally:` restores the working directory) with logic equivalent to:

```python
configs = load_and_validate(config_path, logger)
logger.info("First-level config validated: %d analysis block(s).", len(configs))

for i, cfg in enumerate(configs):
    atype = cfg.analysis_type
    name = cfg.analysis_name
    logger.info("-" * 60)
    logger.info("Running analysis [%d]: %s (type: %s)", i, name, atype)
    logger.info("-" * 60)

    os.makedirs(cfg.out_dir, exist_ok=True)

    run_fn = DISPATCH[atype]
    block_start = time.time()
    result = None
    analysis_error = None
    error_type = None
    error_context = {}
    try:
        result = run_fn(cfg, logger)
    except GuardError as e:
        analysis_error = str(e)
        error_type = type(e).__name__
        error_context = json_safe_context(getattr(e, "context", None))
        logger.critical("Analysis '%s' aborted by guard (%s): %s", name, error_type, analysis_error)
    except (ConfigError, InputError, ModelError,
            ToolUnavailableError, ToolVersionError, ToolExecutionError) as e:
        analysis_error = str(e)
        error_type = type(e).__name__
        error_context = json_safe_context(getattr(e, "context", None))
        logger.error("Analysis '%s' failed (%s): %s", name, error_type, analysis_error)
    except Exception as e:
        analysis_error = str(e)
        error_type = type(e).__name__
        error_context = json_safe_context(getattr(e, "context", None))
        logger.exception("Analysis '%s' raised an unexpected error (%s): %s", name, error_type, analysis_error)

    elapsed = time.time() - block_start
    # (existing completion / "did not complete" log lines, unchanged)

    outcome = {
        "name": name,
        "type": atype,
        "status": "failed" if analysis_error else "success",
        "upstream_status": result.status if result is not None else None,
        "error": analysis_error,
        "error_type": error_type,
        "error_context": error_context,
        "warnings": list(result.warnings) if result is not None else [],
        "wall_time_seconds": round(elapsed, 2),
    }

    if not skip_qc and qc_cfg.get("first_level", {}).get("enabled", False):
        if analysis_error is None:
            fl_qc = build_first_level_qc_from_result(
                name, atype, f"sub-{sub_id}", result
            )
        else:
            orch_a = next((a for a in active_analyses if a["name"] == name), None)
            out_file_pre = (
                f"sub-{sub_id}_{ses_label}_{orch_a['post_id_out_pre']}"
                if orch_a else None
            )
            fl_qc = compute_first_level_qc(
                name, atype, fl_out_dir, f"sub-{sub_id}",
                out_file_pre, logger, error_msg=analysis_error
            )
        fl_qc["session"] = ses_label
        outcome["fl_qc"] = fl_qc

    analysis_outcomes.append(outcome)
```

Constraints:
- The `except SystemExit` branch is removed.
- The variable name `analysis_list` and the tuple unpacking are removed; all former `ns.` references use `cfg.`.
- Existing completion log lines (the "completed in %.2f seconds" info line and the "did not complete ... QC will reflect failure" warning) are retained verbatim.
- Upstream "warning" status yields outcome status "success" (status is driven solely by whether an exception was raised).
      </spec>
      <dependencies>C1, C2</dependencies>
      <risk>medium - central control-flow change for every analysis; mitigated by per-analysis isolation being preserved and by the error-path fallback remaining the prior implementation.</risk>
      <rollback>git checkout orchestrate_first_level.py</rollback>
    </change>

    <change id="C5" priority="P0" source_item="S3, G1-Q7 (7c), A-1">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Replace the startup AFNI check in main() with the upstream version check followed by the upstream 14-binary preflight.</description>
      <spec>
Replace lines 992-994:
```python
        # Verify AFNI installation
        if not args.dry_run:
            verify_afni_installation(logger)
```
with:
```python
        # Verify upstream package version (runs under --dry-run as well)
        verify_upstream_version(logger)
        if _UPSTREAM_IMPORT_ERROR is not None:
            raise OrchestratorError(
                f"Failed to import fmri_first_level_proc v3.1.0+ modules: "
                f"{_UPSTREAM_IMPORT_ERROR}"
            )

        # Verify required AFNI binaries via upstream preflight
        if not args.dry_run:
            try:
                preflight_tool_environments(logger=logger)
            except ToolUnavailableError as e:
                raise OrchestratorError(f"AFNI preflight failed: {e}")
```
Both checks sit inside the existing `try:` so failures route through the existing `except OrchestratorError` handler (log "Fatal: ..." and exit 1). `probe_tool_versions` is not called.
      </spec>
      <dependencies>C1, C2</dependencies>
      <risk>low - fail-fast checks at startup; the version check is pure Python and has no external tool dependency.</risk>
      <rollback>git checkout orchestrate_first_level.py</rollback>
    </change>

    <change id="C6" priority="P1" source_item="S6">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Update the two timing output paths to the .tsv extension.</description>
      <spec>
- Line 644: `f"{run_prefix}_timing.csv"` becomes `f"{run_prefix}_timing.tsv"`.
- Line 726: `f"{task_prefix}_concat_timing.csv"` becomes `f"{task_prefix}_concat_timing.tsv"`.
      </spec>
      <dependencies>C3</dependencies>
      <risk>low - path strings only; paired with C3.</risk>
      <rollback>git checkout orchestrate_first_level.py</rollback>
    </change>

    <change id="C7" priority="P1" source_item="G1-Q2">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Append the high-severity upstream warning count to the per-analysis [OK] line in the participant run summary.</description>
      <spec>
In the summary loop (lines 281-285), replace the `[OK]` branch with:
```python
            if outcome["status"] == "success":
                n_high = sum(
                    1 for w in outcome.get("warnings", [])
                    if isinstance(w, dict) and w.get("severity") == "high"
                )
                if n_high > 0:
                    logger.info(
                        "    [OK]     %s (%.2fs) [%d high-severity warning(s)]",
                        outcome["name"], outcome["wall_time_seconds"], n_high
                    )
                else:
                    logger.info("    [OK]     %s (%.2fs)", outcome["name"], outcome["wall_time_seconds"])
```
The `[FAILED]` branch and all session counters are unchanged. `_derive_session_status` is unchanged.
      </spec>
      <dependencies>C4</dependencies>
      <risk>low - logging only.</risk>
      <rollback>git checkout orchestrate_first_level.py</rollback>
    </change>

    <change id="C8" priority="P1" source_item="S5, S6, S7, G1-Q1, G1-Q5">
      <file path="example_proc_config.yaml" action="modify" />
      <description>Migrate the published proc template to the v3.1.0 schema: stat_buckets extraction API, explicit scale_to_psc, renamed CLI, and BIDS TSV timing references.</description>
      <spec>
1. Usage header (lines 6-8): replace `run-first-level` with `fmri-first-level-proc` on all three lines; argument text unchanged.
2. task_act block:
   - Line 50 and task_conn line 103: `task_timing_path: "/path/to/subj001_nback_timing.csv"` becomes `"/path/to/subj001_nback_timing.tsv"`.
   - Line 59 comment becomes `# -- Condition labels (must match trial_type column in timing TSV) --`.
   - After `use_tissue_derivs` (line 68), add: `    scale_to_psc: true           # true = scale BOLD to percent signal change before regression (recommended)`.
   - Extraction block (lines 81-87): delete the `extract`, `extract_labels`, and `extract_stat` lines; insert as the first key of the block: `      stat_buckets: ["Coef", "Tstat", "Zstat"]  # list of stat types to extract as multi-sub-brick NIfTIs; valid: "Coef", "Tstat", "Fstat", "R^2", "Zstat"; null = disabled`. Keep `extract_out_file_pre`, `extract_resids`, `extract_raw_ptseries` unchanged.
3. task_conn block: after `use_tissue_derivs` (line 121), add the same `scale_to_psc: true` line with the same comment.
4. rest_conn block: after `use_tissue_derivs` (line 187), add the same `scale_to_psc: true` line with the same comment.
5. Column alignment of inline comments follows the surrounding lines in each block.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - template file; validated at runtime by upstream load_and_validate (stat_buckets requires extract_out_file_pre, which is retained).</risk>
      <rollback>git checkout example_proc_config.yaml</rollback>
    </change>

    <change id="C9" priority="P1" source_item="S7, G1-Q5">
      <file path="example_orchestrator_config.yaml" action="modify" />
      <description>Update the upstream version floor in the motion-units comment.</description>
      <spec>Line 64: "per the fmri_first_level_proc >= 2.4.0 input" becomes "per the fmri_first_level_proc >= 3.1.0 input". Surrounding comment text unchanged.</spec>
      <dependencies>none</dependencies>
      <risk>low - comment only.</risk>
      <rollback>git checkout example_orchestrator_config.yaml</rollback>
    </change>

    <change id="C10" priority="P1" source_item="G1-Q7 (7b)">
      <file path="environment.yaml" action="modify" />
      <description>Pin the upstream dependency to the v3.1.0 release tag for reproducible environments.</description>
      <spec>
- Line 29: `"git+https://github.com/tjkeding/fmri_first_level_proc.git"` becomes `"git+https://github.com/tjkeding/fmri_first_level_proc.git@v3.1.0"`.
- Line 28 comment becomes `# Install the first-level package from GitHub, pinned to the minimum supported release (v3.1.0)`.
No environment is created, updated, or installed by this change.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - file edit only; upstream tag v3.1.0 exists (commit 0ad0445).</risk>
      <rollback>git checkout environment.yaml</rollback>
    </change>

    <change id="C11" priority="P1" source_item="G1-Q6">
      <file path="proc_config_final.yaml" action="modify" />
      <description>Apply the v3.1.0 schema migration to the local (gitignored) production proc config so task_act extraction is not silently dropped.</description>
      <spec>
1. task_act block (`name: "nback_act"`), extraction (lines 136-142): delete lines 137-139 (`extract`, `extract_labels` with its 12 labels, `extract_stat`). Insert as the first key of the extraction block: `      stat_buckets: ["Coef", "Tstat", "Zstat"]  # list of stat types to extract as multi-sub-brick NIfTIs; valid: "Coef", "Tstat", "Fstat", "R^2", "Zstat"; null = disabled`. Keep `extract_out_file_pre`, `extract_resids: true`, `extract_raw_ptseries: true` unchanged.
2. In each of the three analysis blocks (nback_act, nback_conn, rest_conn), add `scale_to_psc: true           # true = scale BOLD to percent signal change before regression (recommended)` directly after that block's `use_tissue_derivs` line (lines 68, 176, and 272 respectively), indented to the block's key level.
3. No other line in the file is modified. This file is gitignored and is not published.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - overwrite of a local config, explicitly authorized; the file is not version-controlled, so the build copies it to the session sandbox before editing.</risk>
      <rollback>Restore from the pre-edit copy at <local_path>/proc_config_final.yaml.pre_v3_1_0 (created by the build immediately before the edit).</rollback>
    </change>
  </changes>
  <downstream_handoffs>
    <handoff target_mode="test">Update test fixtures and golden configs for the v3.1.0 schema (ResolvedConfig objects from load_and_validate, FirstLevelResult return values, typed upstream exceptions, stat_buckets, scale_to_psc, BIDS TSV timing) and add coverage for verify_upstream_version, json_safe_context, build_first_level_qc_from_result, the extended consolidate_session_qc schema, and the [OK] warning-count summary line.</handoff>
    <handoff target_mode="document">README.md and INPUT_SPECIFICATION.md: update upstream version floors to >= 3.1.0 (README.md:67; INPUT_SPECIFICATION.md:191, 296, 449) and the example provenance value (INPUT_SPECIFICATION.md:563); document the stat_buckets template schema, scale_to_psc (including a methods statement that per-voxel percent-signal-change scaling precedes parcel averaging and therefore equalizes voxel weights within each parcel), BIDS TSV timing outputs, the per-analysis QC JSON fields (upstream_status, error_type, error_context, warnings), the upstream version check, and the pinned environment. AID_LOG.md historical entries remain unchanged.</handoff>
  </downstream_handoffs>
  <execution_order>C2, C3, C1, C4, C5, C6, C7, C8, C9, C10, C11</execution_order>
</implement_plan>
