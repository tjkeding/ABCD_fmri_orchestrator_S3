# Brainstorm Report: Orchestrator v2.4.0 Alignment with fmri-first-level-proc v2.4.0

**Date:** 2026-04-03  
**Scope:** Identify all orchestrator changes required for compatibility with fmri-first-level-proc v2.3.1 → v2.4.0

---

## Sources Examined

| Source | Description |
|--------|-------------|
| `orchestrator_motion_changes_v2.4.0.md` | Pre-identified changes (user-authored) |
| fmri-first-level-proc GitHub (v2.3.1, v2.4.0) | Commits, code diffs, config changes |
| `orchestrator_utils.py` (lines 1044–1188) | `extract_motion_regressors()` current implementation |
| `orchestrate_first_level.py` | S3 upload/archive pipeline |
| `example_proc_config.yaml` | Local orchestrator config template |
| Upstream `example_config.yaml` | fmri-first-level-proc config template (v2.4.0) |

---

## Upstream Changes Since v2.3.0

### v2.3.1 (Bug Fix)

| Change | Details |
|--------|---------|
| `polort` regression fix | `3dTproject` in `rest_conn_first_level.py` changed from `polort 2` to `polort -1`. Bandpass filtering subsumes polynomial detrending; `polort 2` was double-detrending and wasting 3 DOF. |
| DOF pre-flight inconsistency | DOF calculation still counts 3 polynomial regressors (`n_regressors = use_cols + 3`) despite `polort -1` adding zero. Overly conservative — may cause false run rejections. Documented in `upstream_polort_dof_issue.md`. |

### v2.4.0 (Features)

| Change | Details |
|--------|---------|
| Motion file contract | Explicit input specification: columns `[tx, ty, tz, rx, ry, rz, ...]`, translations in mm, rotations in **degrees**. No unit conversion applied by the pipeline. |
| Mandatory min-outlier EPI | `gen_min_outlier_epi()` called in all three pipeline scripts. Produces `*_min_outlier_epi.nii.gz` per analysis (per run for rest_conn). Not configurable — always runs. |
| `extract_raw_ptseries` | New optional boolean (default `false`) under each analysis type's `extraction` block. Extracts pre-regression parcellated time series. |
| `notch_filter_band` | New optional parameter for rest_conn only. Applies respiration notch filter to motion parameters before FD-based censoring (Fair et al., 2020). |
| Enhanced docstrings | `prepare_motion_file()` and `INPUT_SPECIFICATION.md` now document column order and unit requirements. |

---

## Consolidated Orchestrator Change List

### Critical (Motion Contract Alignment)

| # | File | Location | Change | Rationale |
|---|------|----------|--------|-----------|
| 1 | `orchestrator_utils.py` | `extract_motion_regressors()` ~L1135–1137 | **Remove `np.deg2rad()` call.** Rotations must remain in degrees. | fmri-first-level-proc v2.4.0 expects degrees; AFNI's `1d_tool.py` treats 1 degree ≈ 1 mm arc at ~57.3 mm head radius. Prior deg2rad conversion underweighted rotational FD by ~57×. |
| 2 | `orchestrator_utils.py` | `extract_motion_regressors()` ~L1087 | **Verify column order** `[trans_x, trans_y, trans_z, rot_x, rot_y, rot_z]` is preserved in output. | Upstream contract requires `[tx, ty, tz, rx, ry, rz, ...]`. Current code already selects in this order — verify, no code change expected. |
| 3 | `orchestrator_utils.py` | `extract_motion_regressors()` ~L1139–1152 | **Verify derivatives use degree-valued array.** | Automatic after Change 1: `motion_array` will contain degrees, and `np.diff` operates on degree-valued data. No code change expected. |
| 4 | `orchestrator_utils.py` | `extract_motion_regressors()` ~L1106–1133 | **Update rotation detection messaging.** Remove reference to "Proceeding with deg2rad conversion"; state that values pass through without conversion. | Detection logic is unchanged; only the consequence and messaging differ. |

### Required (Code + Documentation)

| # | File | Location | Change | Rationale |
|---|------|----------|--------|-----------|
| 5 | `orchestrator_utils.py` | `extract_motion_regressors()` docstring ~L1046–1051 | **Update docstring.** Remove "Rotations are converted from degrees to radians" and "AFNI convention". State output is in degrees (translations mm, rotations degrees). Reference fmri-first-level-proc v2.4.0 input specification. | Docstring must reflect actual behavior. |
| 6 | `tests/` | `test_cr_implementation.py`, `test_coverage_gaps.py` | **Update rotation unit and integration test assertions.** Verify output retains degrees (not radians). Verify `rotation_unit_ambiguous` flag behavior. | Tests must match the new no-conversion contract. |

### Config Template Updates

| # | File | Change | Default |
|---|------|--------|---------|
| 7 | `example_proc_config.yaml` | Add `extract_raw_ptseries: true` to all three extraction blocks | `true` |
| 8 | `example_proc_config.yaml` | Add `notch_filter_band: null` to rest_conn block | `null` (disabled) |
| 9 | `example_proc_config.yaml` | Reorder `fd_threshold` and `censor_prev_tr` to match upstream layout (block-level, not separate section) | Already at block level in local config — verify parity |
| 10 | `example_proc_config.yaml` | Update header usage comments: `run-first-level` CLI entry point (upstream changed from `python run_first_level.py`) | — |

### Documentation

| # | File | Change |
|---|------|--------|
| 11 | `INPUT_SPECIFICATION.md` | Update provenance example from `"fmri_first_level_proc_version": "2.3.0"` to `"2.4.0"` |
| 12 | `README.md` | Note minimum required fmri-first-level-proc version (>= 2.4.0) |

### No Code Change Required (Verification Only)

| # | Item | Why No Change Needed |
|---|------|---------------------|
| A | Min-outlier EPI output files | `compress_session_outputs()` archives entire `first_level_out/` directory. New files auto-captured. Verify in end-to-end test. |
| B | `polort -1` behavioral change | Orchestrator does not set or pass `polort`. Hardcoded upstream. |
| C | Version provenance | `fmri_first_level_proc.__version__` is read dynamically at runtime (line 2219). Auto-updates after editable install is refreshed. |

### Upstream Issue (Not in Orchestrator Scope)

| # | Item | Document |
|---|------|----------|
| U1 | DOF pre-flight inconsistency (`polort -1` vs. `+ 3` regressors) | `upstream_polort_dof_issue.md` — to be transferred to fmri-first-level-proc project |

---

## Decisions Made

| Decision | Rationale |
|----------|-----------|
| `extract_raw_ptseries` default set to `true` | Pre-regression parcellated time series are scientifically useful for QC (temporal SNR, pre/post-regression signal comparison). |
| `notch_filter_band` default set to `null` | Respiratory notch filtering is dataset-specific; ABCD data may or may not benefit. Default disabled; user enables per study design. |
| No re-testing required | Motion contract change is well-understood; numerical impact is predictable (rotation values ×57.3 larger in output). Existing test infrastructure covers the code paths. |
| Independent orchestrator version scheme | Orchestrator documents which fmri-first-level-proc version it requires (>= 2.4.0) but uses its own semantic versioning. |

---

## Implementation Order

1. **orchestrator_utils.py** — Changes 1–5 (motion contract: deg2rad removal, messaging, docstring)
2. **example_proc_config.yaml** — Changes 7–10 (new parameters, layout parity)
3. **tests/** — Change 6 (update assertions)
4. **INPUT_SPECIFICATION.md** + **README.md** — Changes 11–12 (documentation)
5. **Verification** — Items A–C (confirm in test run)
