# Implementation Plan: CR Findings F1–F19

```xml
<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan"
        timestamp="2026-03-13T16:51:51Z" />

  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_brainstorm_20260313_100000.md"
            mode="brainstorm" key_items="10" />
    <report path="brainstorm_history/ABCD_fmri_orchestrator_S3_brainstorm_20260312_150000.md"
            mode="brainstorm" key_items="3 (F1, F2, QC enrichment)" />
  </input_reports>

  <!-- ================================================================ -->
  <!-- ASSUMPTIONS / DECISIONS MADE WITHOUT EXPLICIT APPROVAL           -->
  <!-- ================================================================ -->
  <!--
    1. F2 / QC architecture overhaul: The brainstorm specified removing
       compute_framewise_displacement() from the QC PIPELINE (i.e., from
       compute_preproc_qc() calls in orchestrate_first_level.py), not
       deleting the function itself from orchestrator_utils.py. The function
       is retained (it is referenced by unit tests). compute_preproc_qc()
       is revised to remove FD/censor computation and retain only non-motion
       metrics (tSNR, mask coverage, registration Dice, DVARS from confounds).

    2. Consolidated QC JSON (F2 QC enrichment): The brainstorm specifies
       a new function (e.g., consolidate_session_qc()) that assembles the
       final per-session JSON from: (a) Phase 1 preproc QC dicts, (b)
       per-analysis dicts already returned by compute_first_level_qc(),
       which read the upstream enorm.1D/censor.1D/qc_summary.json. This
       replaces the current approach of saving N separate preproc_qc.json
       and first_level_qc.json files with a single orchestrator_qc.json.
       The individual per-run and per-analysis JSON saves are REMOVED from
       _process_session() in favour of accumulation into the consolidated
       output.

    3. F1 per-analysis tracking: The brainstorm describes accumulating
       {name, status, error, wall_time} dicts inside _process_session() and
       passing them back. Because _process_session() currently has no return
       value, it will be refactored to return a list of analysis outcome
       dicts. process_participant() uses this list for session status
       determination (success / partial / failed) and for the consolidated
       QC JSON.

    4. force_recompute (F8): The flag will be threaded as a keyword argument
       to all caching functions. In orchestrate_first_level.py the flag is
       read from config at the top of _process_session() and passed down.
       No changes to the public API of load_orchestrator_config() are needed
       beyond adding the config default-set call.

    5. Mask intersection (F9): The intersection BOLD mask is written to
       concat_dir as {task_prefix}_concat_mask.nii.gz. Fall-back to
       per_run_masks[0] when only one run survived (no 3dmask_tool call
       needed).

    6. Version bump: orchestrate_first_level.py header version will be
       incremented from 3.0 to 3.1 to reflect this set of changes.
       orchestrator_utils.py header version bumped likewise.
  -->

  <changes>

    <!-- ============================================================== -->
    <!-- C1: Per-analysis outcome tracking + qualified session reporting -->
    <!--     (F1)                                                        -->
    <!-- ============================================================== -->
    <change id="C1" priority="P0" source_item="F1 — brainstorm_20260312">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Replace binary success/failed session status with qualified
        success/partial/failed based on per-analysis outcomes. Accumulate
        {name, status, error, wall_time_seconds} dicts in _process_session();
        return the list to process_participant(). SESSION SUMMARY log prints
        per-analysis status lines. Session status: "success" (all analyses OK),
        "partial" (some OK, some failed), "failed" (all failed or no analyses ran).
      </description>
      <spec>
        In _process_session():
          - Add `analysis_outcomes = []` before the analysis loop.
          - After each run_fn(ns, logger) call (inside the try/except), append:
              analysis_outcomes.append({
                  "name": name,
                  "type": atype,
                  "status": "failed" if analysis_error else "success",
                  "error": analysis_error,
                  "wall_time_seconds": round(elapsed, 2),
              })
          - Change return to: return analysis_outcomes

        In process_participant():
          - Replace `_process_session(...)` call (no return capture) with:
              analysis_outcomes = _process_session(...)
          - Derive session_status:
              if not analysis_outcomes:
                  session_status = "failed"
              elif all(o["status"] == "success" for o in analysis_outcomes):
                  session_status = "success"
              elif any(o["status"] == "success" for o in analysis_outcomes):
                  session_status = "partial"
              else:
                  session_status = "failed"
          - Store session_results[session] = {
                "status": session_status,
                "analyses": analysis_outcomes,
            }
          - SESSION SUMMARY: log session_status, then one line per analysis:
              "  [OK]     {name}" or "  [FAILED] {name}: {error}"
          - Participant-level raise condition: unchanged — raise only if ALL
            sessions have status "failed".

        In count logic, treat both "success" and "partial" as non-failed for
        the n_success/n_failed summary counts (or add n_partial separately).
        Recommended: three counters (n_success, n_partial, n_failed) for
        maximum transparency.
      </spec>
      <dependencies>none</dependencies>
      <risk>low — isolated to result accumulation and summary logging; no
      change to analysis dispatch logic</risk>
      <rollback>Revert return statement in _process_session() and the
      session_results dict construction in process_participant()</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C2: Remove orchestrator FD from QC; split Phase 1 / Phase 2   -->
    <!--     (F2)                                                        -->
    <!-- ============================================================== -->
    <change id="C2" priority="P0" source_item="F2 — brainstorm_20260312">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Revise compute_preproc_qc() to remove FD/censor computation from
        the Phase 1 (pre-analysis) QC path. The function retains: tSNR,
        brain mask coverage, registration Dice, DVARS from confounds, and
        non-steady-state TR count. All motion fields (mean_fd, max_fd,
        median_fd, censor stats) are REMOVED from compute_preproc_qc()
        output. Motion will come exclusively from upstream enorm.1D and
        censor.1D in compute_first_level_qc() / consolidate_session_qc().
      </description>
      <spec>
        In compute_preproc_qc() (line ~1779):
          - Remove the section "-- Motion metrics (from raw motion.tsv) --"
            (lines ~1820-1849): delete the motion_df read, compute_framewise_displacement()
            call, fd/censor computation, and the qc["motion"] and qc["censor"] blocks.
          - Remove the motion_tsv_path parameter from the function signature.
            (It was only used for FD computation in this function.)
          - Remove DVARS computation that relied on fd_full for carpet plot alignment;
            DVARS from fMRIPrep confounds_df can remain.
          - Retain carpet_plot call but change the fd_full argument to None
            (or a zeros array of length n_total_trs) since FD is no longer
            computed here. Update generate_carpet_plot() to handle None fd.
          - Update docstring to reflect Phase 1 scope: tSNR, mask coverage,
            Dice, DVARS only.

        Update all call sites in orchestrate_first_level.py:
          - Step 6 call to compute_preproc_qc(): remove motion_tsv_path
            argument and fd_threshold argument.
      </spec>
      <dependencies>none</dependencies>
      <risk>medium — removes a QC path; motion QC is now deferred to
      Phase 2 (consolidated JSON). Risk is acceptable because motion metrics
      are redundant with upstream enorm.1D/censor.1D</risk>
      <rollback>Restore motion block and original signature in
      compute_preproc_qc()</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C3: Consolidated session-level QC JSON                         -->
    <!--     (F2 QC enrichment — brainstorm_20260312)                   -->
    <!-- ============================================================== -->
    <change id="C3" priority="P0" source_item="F2 QC enrichment — brainstorm_20260312">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add consolidate_session_qc() function. Assembles single
        sub-{ID}_ses-{session}_orchestrator_qc.json from:
          - Provenance block (versions + timestamp)
          - Preprocessing block (per task, per run: tSNR, Dice, mask,
            n_total_trs, n_nss_removed) — from Phase 1 QC dicts
          - Analyses block (per analysis: status, error, wall_time,
            upstream QC embedded) — from compute_first_level_qc() dicts
          - Session-level block: session_status, session_wall_time_seconds
      </description>
      <spec>
        def consolidate_session_qc(
            sub_id: str,
            ses_label: str,
            session_status: str,
            session_wall_time: float,
            preproc_qc_by_run: dict,  # {run_label: qc_dict from compute_preproc_qc()}
            analysis_outcomes: list,  # [{name, type, status, error, wall_time, fl_qc}]
            out_path: str,
            logger,
        ) -> str:
            """Build and write the consolidated session QC JSON."""

        Provenance block:
            import subprocess
            afni_ver = subprocess.run(["afni", "--version"], ...).stdout.strip()
            from fmri_first_level_proc import __version__ as proc_ver
            orch_ver = "3.1"  # orchestrator version string (also set as module constant)
            from datetime import datetime, timezone
            timestamp = datetime.now(timezone.utc).isoformat()

        Structure:
            qc = {
                "provenance": {
                    "orchestrator_version": orch_ver,
                    "fmri_first_level_proc_version": proc_ver,
                    "afni_version": afni_ver,
                    "timestamp_utc": timestamp,
                    "sub_id": sub_id,
                    "session": ses_label,
                },
                "preprocessing": preproc_qc_by_run,
                "analyses": {o["name"]: {...} for o in analysis_outcomes},
                "session": {
                    "status": session_status,
                    "wall_time_seconds": round(session_wall_time, 2),
                    "n_analyses_attempted": len(analysis_outcomes),
                    "n_analyses_succeeded": sum(1 for o in analysis_outcomes
                                                if o["status"] == "success"),
                },
            }

        Each analysis entry in "analyses":
            {
                "status": o["status"],
                "error": o["error"],
                "wall_time_seconds": o["wall_time_seconds"],
                "upstream_qc": o.get("fl_qc"),  # from compute_first_level_qc()
            }

        Write via save_qc_json(). Return out_path.

        In _process_session():
          - Accumulate preproc QC dicts (don't write per-run JSON files anymore):
              preproc_qc_by_run = {}
              ...
              qc_metrics = compute_preproc_qc(...)
              preproc_qc_by_run[run_label] = qc_metrics
              # Remove: save_qc_json(qc_metrics, qc_out, logger)
          - After analysis loop, attach fl_qc to each analysis_outcome:
              fl_qc = compute_first_level_qc(...)
              # Remove: save_qc_json(fl_qc, ...) per-analysis write
              # Attach to the matching analysis_outcome dict:
              analysis_outcomes[-1]["fl_qc"] = fl_qc
          - After all analyses complete, call consolidate_session_qc():
              qc_out = os.path.join(
                  session_out, "qc",
                  f"sub-{sub_id}_{ses_label}_orchestrator_qc.json"
              )
              consolidate_session_qc(
                  sub_id, ses_label, session_status, session_elapsed,
                  preproc_qc_by_run, analysis_outcomes, qc_out, logger
              )
          Note: session_status must be derived from analysis_outcomes BEFORE
          consolidate_session_qc() is called, within _process_session().
          The return from _process_session() (for C1) carries the same
          analysis_outcomes list, which process_participant() uses for the
          log summary.
      </spec>
      <dependencies>C1, C2</dependencies>
      <risk>medium — significant restructuring of QC flow; existing per-run
      and per-analysis JSON files will no longer be written (breaking change
      for any downstream code reading those files). Risk is bounded: the
      current tests mock QC functions; the new consolidated format is
      strictly a superset.</risk>
      <rollback>Restore per-run save_qc_json() calls; remove consolidate_session_qc()</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C4: Structured run-loss warning (F3)                           -->
    <!-- ============================================================== -->
    <change id="C4" priority="P1" source_item="F3 — brainstorm_20260313">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Enhance the skipped-runs warning (lines 478-484) to explicitly state
        surviving vs. total run count and the fraction lost.
      </description>
      <spec>
        Replace the current warning at lines 478-484:
            if skipped_runs:
                logger.warning(
                    "Task '%s' sub-%s %s: skipped %d of %d run(s): %s",
                    task_label, sub_id, ses_label,
                    len(skipped_runs), len(run_dicts),
                    ", ".join(skipped_runs)
                )

        With:
            if skipped_runs:
                n_surviving = len(run_dicts) - len(skipped_runs)
                logger.warning(
                    "Task '%s' sub-%s %s: %d of %d run(s) failed preprocessing "
                    "(%d surviving). Skipped: %s. Downstream analyses will use "
                    "only surviving run(s).",
                    task_label, sub_id, ses_label,
                    len(skipped_runs), len(run_dicts), n_surviving,
                    ", ".join(skipped_runs)
                )
      </spec>
      <dependencies>none</dependencies>
      <risk>low — log-only change</risk>
      <rollback>Revert warning message string</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C5: Two-tier rotation unit check (F4)                          -->
    <!-- ============================================================== -->
    <change id="C5" priority="P1" source_item="F4 — brainstorm_20260313">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add definitive two-tier rotation unit validation in
        extract_motion_regressors() immediately after extracting base_data
        and before np.deg2rad(). Tier 1: max(abs(rot)) > 1.0 → definitively
        degrees, proceed. Tier 2: max(abs(rot)) &lt;= 1.0 → raise
        OrchestratorError (data may already be in radians; requires manual
        inspection).
      </description>
      <spec>
        Insert after line 1116 (after `base_data` check block), before line 1118:

            # --- Rotation unit validation (F4) ---
            # Physical constraint: head coil restricts rotation to ~+/-15-20 degrees.
            # 1.0 radian = 57.3 degrees — physically impossible in a head coil.
            # Therefore if max(abs(rotation)) > 1.0, data MUST be in degrees.
            # If max(abs(rotation)) <= 1.0 across all TRs and axes, data may already
            # be in radians (or is pathologically still), requiring manual inspection.
            rot_cols = motion_df[["rot_x", "rot_y", "rot_z"]].values
            max_rot = float(np.nanmax(np.abs(rot_cols)))
            if max_rot > 1.0:
                logger.debug(
                    "Rotation unit check PASSED (max abs rotation = %.4f > 1.0): "
                    "data is definitively in degrees. Proceeding with deg2rad.",
                    max_rot
                )
            else:
                raise OrchestratorError(
                    f"Rotation unit check FAILED for {motion_tsv_path}: "
                    f"max(abs(rotation)) = {max_rot:.6f} <= 1.0 across all TRs and axes. "
                    f"Data may already be in radians (1.0 rad = 57.3 deg is physically "
                    f"impossible in a head coil, but every real scan should show >1.0 "
                    f"degree of rotation somewhere). Manual inspection required. "
                    f"If confirmed to be in degrees, investigate the motion tracking "
                    f"output for this subject/run."
                )
            # --- End rotation unit validation ---
      </spec>
      <dependencies>none</dependencies>
      <risk>low — operates on rot_cols extracted before deg2rad; no change
      to downstream logic in normal operation; hard error only in the
      ambiguous case which is effectively impossible with real ABCD data</risk>
      <rollback>Remove the rotation unit validation block</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C6: force_recompute config flag (F8)                           -->
    <!-- ============================================================== -->
    <change id="C6" priority="P1" source_item="F8 — brainstorm_20260313">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="example_orchestrator_config.yaml" action="modify" />
      <description>
        Add force_recompute boolean to orchestrator config (default false).
        When true, bypass all os.path.isfile() early-exit checks in all
        caching preprocessing functions. Thread flag through function signatures.
      </description>
      <spec>
        1. orchestrator_utils.py — add force_recompute=False parameter to
           the following functions (all have os.path.isfile() early exits):
             - apply_brain_mask(bold_path, mask_path, out_dir, out_prefix,
                                logger, force_recompute=False)
             - remove_initial_trs_bold(bold_path, n_remove, out_dir,
                                       out_prefix, logger,
                                       force_recompute=False)
             - extract_motion_regressors(motion_tsv_path, n_remove,
                                         calc_n_motion_derivs, out_path,
                                         logger, force_recompute=False)
             - extract_tissue_signals(confounds_path, n_remove, tissue_type,
                                      out_path, logger, force_recompute=False)
             - fix_nback_cue_labels(events_path, condition_column, out_path,
                                    logger, force_recompute=False)
             - format_task_timing(..., logger, force_recompute=False)
             - concatenate_bolds(bold_paths, out_path, logger,
                                  force_recompute=False)
             - concatenate_tabular_files(file_paths, out_path, logger,
                                          force_recompute=False)
             - concatenate_task_timing(..., logger, force_recompute=False)
             - apply_smoothing(bold_path, mask_path, method, fwhm,
                               out_dir, out_prefix, logger,
                               force_recompute=False)

           For each: change `if os.path.isfile(out_path):` to
                             `if os.path.isfile(out_path) and not force_recompute:`

        2. load_orchestrator_config() — add after existing defaults block
           (near line 2416):
               config["study"].setdefault("force_recompute", False)
               if not isinstance(config["study"].get("force_recompute"), bool):
                   raise OrchestratorError(
                       "study.force_recompute must be a boolean, got: "
                       f"{config['study']['force_recompute']!r}"
                   )

        3. orchestrate_first_level.py — in _process_session(), extract flag:
               force_recompute = study.get("force_recompute", False)
           Pass force_recompute=force_recompute keyword to ALL 10 function
           calls listed above.

        4. example_orchestrator_config.yaml — add to study section with
           comment explaining caching behavior (default false; set true during
           iterative development to force recomputation of cached outputs).
      </spec>
      <dependencies>none</dependencies>
      <risk>low — keyword argument with default=False; zero behavioral change
      when unset; only affects early-exit cache checks</risk>
      <rollback>Remove force_recompute parameters and the config default line</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C7: NaN motion handling — "unknown = censor" (F10)             -->
    <!-- ============================================================== -->
    <change id="C7" priority="P1" source_item="F10 — brainstorm_20260313">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        In extract_motion_regressors(), replace np.nan_to_num(nan=0.0) with
        imputation using 999.0 (guarantees censoring). Log NaN count and
        affected TR indices and column names at WARNING level before imputation.
      </description>
      <spec>
        Replace lines 1143-1144 (current nan_to_num call):

        OLD:
            # Replace any remaining NaN with 0.0
            motion_data = np.nan_to_num(motion_data, nan=0.0)

        NEW:
            # NaN handling: "unknown = censor" policy (F10)
            # NaN in motion parameters indicates a tracking failure — the true
            # motion is unknown. Imputing 0.0 would make these TRs appear as
            # the stillest frames, guaranteeing they survive censoring when they
            # are the least trustworthy. Imputing 999.0 ensures they exceed any
            # reasonable FD threshold and are censored by upstream 1d_tool.py.
            nan_mask = np.isnan(motion_data)
            if nan_mask.any():
                nan_rows, nan_cols = np.where(nan_mask)
                col_names = (base_cols * (1 + calc_n_motion_derivs))[:motion_data.shape[1]]
                affected = [
                    f"TR {r} col '{col_names[c]}'"
                    for r, c in zip(nan_rows.tolist(), nan_cols.tolist())
                ]
                logger.warning(
                    "NaN motion values detected in %s: %d occurrence(s) across "
                    "%d unique TR(s). Imputing 999.0 (guarantees censoring). "
                    "Affected: %s",
                    motion_tsv_path,
                    int(nan_mask.sum()),
                    int(np.unique(nan_rows).size),
                    "; ".join(affected[:20]) + (" ..." if len(affected) > 20 else "")
                )
            motion_data = np.where(nan_mask, 999.0, motion_data)

        Note: `col_names` construction — base_cols has 6 names; for each
        derivative degree the same 6 column names repeat with a "d1_" etc.
        prefix for logging clarity. A simpler approach: use positional index
        labels ("col_0" through "col_N") if column-name expansion is complex.
        Simplest correct implementation: use positional labels.

        Revised col_names line:
            col_names = [f"col_{i}" for i in range(motion_data.shape[1])]
      </spec>
      <dependencies>none</dependencies>
      <risk>low — only activates when NaN values are present; default
      behavior (999.0 imputation) is strictly more conservative than the
      prior 0.0 imputation</risk>
      <rollback>Restore np.nan_to_num(nan=0.0) line</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C8: Strict task label whitelist (F11)                          -->
    <!-- ============================================================== -->
    <change id="C8" priority="P1" source_item="F11 — brainstorm_20260313">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Replace is_rest = task_label.lower().startswith("rest") with an
        exact equality check against a module-level whitelist constant
        VALID_TASK_LABELS = {"rest", "nback"}. Raise OrchestratorError on
        unrecognized task labels.
      </description>
      <spec>
        1. Add module-level constant after imports (before process_participant):
               VALID_TASK_LABELS = {"rest", "nback"}

        2. In the per-task loop (around line 324), replace:
               is_rest = task_label.lower().startswith("rest")

           With validation + exact check:
               if task_label not in VALID_TASK_LABELS:
                   raise OrchestratorError(
                       f"Unrecognized task label '{task_label}' for sub-{sub_id} "
                       f"{ses_label}. Valid labels for this orchestrator: "
                       f"{sorted(VALID_TASK_LABELS)}. Check the 'tasks' section "
                       f"of the orchestrator config."
                   )
               is_rest = (task_label == "rest")

        3. Also replace the is_rest check in download_session_data() and
           discover_local_mmps_files() in orchestrator_utils.py (lines ~196
           and ~373):
               is_rest = task_label.lower().startswith("rest")
           → is_rest = (task_label == "rest")
           (No whitelist error needed there — orchestrate_first_level.py
           validates labels first; utils just uses the flag.)
      </spec>
      <dependencies>none</dependencies>
      <risk>low — only changes behavior for malformed config task labels,
      which would otherwise cause unpredictable downstream failures</risk>
      <rollback>Remove VALID_TASK_LABELS constant; restore startswith() checks</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C9: S3 run discovery — probe all 9 indices (F12)               -->
    <!-- ============================================================== -->
    <change id="C9" priority="P1" source_item="F12 — brainstorm_20260313">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        In download_session_data(), replace `break` on 404/NoSuchKey for
        both events and motion file loops with `continue`, so all 9 run
        indices are probed regardless of gaps.
      </description>
      <spec>
        Events loop (around line 223):
        OLD:
            if code in ("404", "NoSuchKey"):
                # No more runs for this task
                break
        NEW:
            if code in ("404", "NoSuchKey"):
                # Continue probing — run indices may be non-contiguous
                continue

        Also remove the `break` in the download failure handler (line 241):
        OLD:
            except ClientError as e:
                logger.warning(...)
                break
        NEW:
            except ClientError as e:
                logger.warning(...)
                continue

        Motion loop (around line 282-300): apply identical changes.

        Docstring update: remove statement "probes for runs 1-9, stop at
        first missing run" → "probes all run indices 1-9 unconditionally".
      </spec>
      <dependencies>none</dependencies>
      <risk>low — adds at most ~9 additional HEAD requests per task;
      functionally equivalent for contiguous runs; strictly more complete
      for non-contiguous runs</risk>
      <rollback>Restore break statements</rollback>
    </change>

    <!-- ============================================================== -->
    <!-- C10: Mask intersection for concatenated tasks (F9)             -->
    <!-- ============================================================== -->
    <change id="C10" priority="P2" source_item="F9 — brainstorm_20260313">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Replace `concat_mask = per_run_masks[0]` with a mask intersection
        using 3dmask_tool -inter across all per-run masks when more than
        one run survived. Fall back to the single mask when only one run
        survived.
      </description>
      <spec>
        Add helper function compute_mask_intersection() to orchestrator_utils.py:

            def compute_mask_intersection(mask_paths, out_path, logger,
                                          force_recompute=False):
                """
                Compute the intersection of multiple brain masks using
                3dmask_tool -inter. Returns out_path if only one mask provided.
                """
                if len(mask_paths) == 1:
                    return mask_paths[0]

                if os.path.isfile(out_path) and not force_recompute:
                    logger.info("Mask intersection already exists: %s", out_path)
                    return out_path

                cmd = ["3dmask_tool", "-inter", "-prefix", out_path,
                       "-input"] + list(mask_paths)
                try:
                    subprocess.run(cmd, capture_output=True, text=True, check=True)
                except subprocess.CalledProcessError as e:
                    raise OrchestratorError(
                        f"3dmask_tool mask intersection failed: "
                        f"{e.stderr.strip() if e.stderr else str(e)}"
                    )
                if not os.path.isfile(out_path):
                    raise OrchestratorError(
                        f"3dmask_tool produced no output: {out_path}"
                    )
                logger.info(
                    "Mask intersection (%d masks) → %s", len(mask_paths), out_path
                )
                return out_path

        In orchestrate_first_level.py, import compute_mask_intersection.
        Replace (line 513):
            # For smoothing, use mask from first successfully processed run
            concat_mask = per_run_masks[0]

        With:
            # Compute intersection mask across all surviving runs (F9)
            concat_mask_path = os.path.join(
                concat_dir, f"{task_prefix}_concat_mask.nii.gz"
            )
            concat_mask = compute_mask_intersection(
                per_run_masks, concat_mask_path, logger,
                force_recompute=force_recompute
            )

        Also add compute_mask_intersection to the import block in
        orchestrate_first_level.py.
      </spec>
      <dependencies>C6 (for force_recompute threading)</dependencies>
      <risk>low — intersection equals any single mask when masks are
      identical (the normal case for fMRIPrep template-space outputs);
      no behavioral change in the common case</risk>
      <rollback>Remove compute_mask_intersection(); restore
      concat_mask = per_run_masks[0]</rollback>
    </change>

  </changes>

  <execution_order>
    C4, C5, C7, C8, C9 (independent, no cross-dependencies — implement first)
    C6 (force_recompute threading — prerequisite for C10)
    C10 (mask intersection — depends on C6 for force_recompute)
    C1 (per-analysis tracking — sets up return structure needed by C3)
    C2 (QC Phase 1 revision — prerequisite for C3)
    C3 (consolidated QC JSON — depends on C1 and C2)
  </execution_order>

</implement_plan>
```
