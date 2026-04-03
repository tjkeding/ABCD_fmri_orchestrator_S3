<?xml version="1.0" encoding="UTF-8"?>
<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-03-13T15:10:04Z" />

  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_brainstorm_20260313_192500.md" mode="brainstorm" key_items="8" />
    <report path="ABCD_fmri_orchestrator_S3_cr_20260313_192000.md" mode="cr" key_items="11 findings, 8 actioned (F3/F5/F11 deferred)" />
  </input_reports>

  <assumptions>
    - F3 (NaN derivative propagation), F5 (rotation unit check), F11 (test coverage gap) are explicitly deferred.
    - All proc_config and upstream fmri_first_level_proc changes are already applied manually — out of scope.
    - Changes are limited to orchestrate_first_level.py and orchestrator_utils.py.
    - VALID_TASK_LABELS is already defined in orchestrate_first_level.py at module level; it must be importable
      into orchestrator_utils.py for F10 (task label validation at load time). To avoid circular imports,
      the whitelist will be defined as a module-level constant in orchestrator_utils.py and imported into
      orchestrate_first_level.py from there.
    - __version__ (F8) will be defined in orchestrate_first_level.py (the entry point) and passed/imported
      into orchestrator_utils.consolidate_session_qc() via a module-level reference in orchestrator_utils.py.
    - For F9 (per-run smoothing index mismatch): a parallel list per_run_labels is already tracked implicitly
      via per_run_masks (both appended at the same point, line 514). The fix adds per_run_labels as an
      explicit parallel list to replace run_dicts[i]['run_label'] in the smoothing loop.
    - F7 (grid match logging): a debug-level log confirming grid dimensions is added before the 3dmask_tool
      call; this requires a 3dinfo subprocess call per mask. To keep the fix lightweight, dimensions are
      logged from the first mask only (the reference), with the assumption that AFNI itself will error if
      masks are mismatched.
  </assumptions>

  <changes>

    <!-- ============================================================ -->
    <!-- C1: F1 — Fix session_wall_time hardcoded to 0.0             -->
    <!-- ============================================================ -->
    <change id="C1" priority="P0" source_item="F1 (critical)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Track actual session wall time inside _process_session() and pass it
        to consolidate_session_qc() instead of the hardcoded literal 0.0.
      </description>
      <spec>
        1. At the top of _process_session(), immediately after the local
           variable assignments (after line ~287), add:
               session_start_time = time.time()
        2. At line 751 (the consolidate_session_qc() call), replace the
           hardcoded 0.0 with:
               session_wall_time = time.time() - session_start_time
           Then pass session_wall_time as the fourth positional argument.
        Note: time is already imported at module level.
      </spec>
      <dependencies>none</dependencies>
      <risk>low — purely additive; only affects the QC JSON wall_time_seconds field</risk>
      <rollback>revert the session_start_time assignment and restore 0.0 in the call</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C2: F2 — Fix force_recompute for AFNI-backed functions      -->
    <!-- ============================================================ -->
    <change id="C2" priority="P0" source_item="F2 (critical)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        In all 5 AFNI-backed caching functions, delete the existing output
        file before calling the AFNI command when force_recompute=True.
        Uses Option A from the CR recommendation (os.remove before AFNI call).

        Affected functions and their output path variables:
          - apply_brain_mask: out_path
          - remove_initial_trs_bold: out_path
          - concatenate_bolds: out_path
          - apply_smoothing: out_path
          - compute_mask_intersection: out_path

        Pattern to insert immediately before the subprocess.run (or cmd
        construction) in each function, after the cache-hit early return:
            if force_recompute and os.path.isfile(out_path):
                os.remove(out_path)
                logger.debug("force_recompute: removed existing %s", out_path)
      </spec>
      <dependencies>none</dependencies>
      <risk>low — only active when force_recompute=True (non-default); no effect on production default path</risk>
      <rollback>remove the os.remove block from each of the 5 functions</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C3: F4 — Extract _derive_session_status() helper            -->
    <!-- ============================================================ -->
    <change id="C3" priority="P1" source_item="F4 (major)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Extract the duplicated session status derivation logic into a single
        module-level helper function _derive_session_status(). Replace both
        inline implementations with calls to this helper.
      </description>
      <spec>
        1. Add module-level function immediately before process_participant():
               def _derive_session_status(analysis_outcomes):
                   """Derive qualified session status from per-analysis outcomes."""
                   if not analysis_outcomes:
                       return "failed"
                   elif all(o["status"] == "success" for o in analysis_outcomes):
                       return "success"
                   elif any(o["status"] == "success" for o in analysis_outcomes):
                       return "partial"
                   else:
                       return "failed"

        2. In process_participant() (lines 175-182), replace the inline
           if/elif/else block with:
               session_status = _derive_session_status(analysis_outcomes)

        3. In _process_session() (lines 737-744), replace the inline
           if/elif/else block with:
               qc_session_status = _derive_session_status(analysis_outcomes)
      </spec>
      <dependencies>none</dependencies>
      <risk>low — pure refactor; logic is identical, no behavioral change</risk>
      <rollback>inline the status derivation logic at both call sites</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C4: F9 — Fix per-run smoothing index mismatch               -->
    <!-- ============================================================ -->
    <change id="C4" priority="P1" source_item="F9 (minor)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Add a per_run_labels list (parallel to per_run_bolds and per_run_masks)
        that is populated only for successfully processed runs. Use per_run_masks[i]
        and per_run_labels[i] in the rest smoothing loop instead of run_dicts[i].
      </description>
      <spec>
        1. In the per-run variable initializations (~line 390), add:
               per_run_labels = []  # run labels for successfully processed runs

        2. At the same location where per_run_bolds and per_run_masks are
           appended (lines 513-514), also append:
               per_run_labels.append(run_label)

        3. In the rest smoothing loop (lines 612-620), replace:
               bold, run_dicts[i]["mask_path"]
               ...
               f"sub-{sub_id}_{run_dicts[i]['run_label']}"
           with:
               bold, per_run_masks[i]
               ...
               f"sub-{sub_id}_{per_run_labels[i]}"
      </spec>
      <dependencies>none</dependencies>
      <risk>low — per_run_labels populated in the same location as per_run_masks; index parity guaranteed</risk>
      <rollback>remove per_run_labels list and restore run_dicts[i] indexing</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C5: F6 — Consolidate detect_non_steady_state_trs() call     -->
    <!-- ============================================================ -->
    <change id="C5" priority="P2" source_item="F6 (minor)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Call detect_non_steady_state_trs() once per run and reuse the result
        for both QC (Step 6) and trimming (Step 7), eliminating the redundant
        second call at line 435.
      </description>
      <spec>
        1. Move the detect_non_steady_state_trs() call to BEFORE Step 5
           (brain masking), immediately after the decompress_if_needed block:
               n_remove = detect_non_steady_state_trs(rd["confounds_path"], logger)

        2. In Step 6 (QC), replace n_remove_for_qc with n_remove in the
           compute_preproc_qc() call. Remove the separate n_remove_for_qc
           variable.

        3. In Step 7 (trimming), remove the second call to
           detect_non_steady_state_trs() — n_remove is already in scope.
      </spec>
      <dependencies>none</dependencies>
      <risk>low — both calls previously returned the same value; no behavioral change</risk>
      <rollback>restore both separate detect_non_steady_state_trs() calls</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C6: F7 — Add debug-level grid log in compute_mask_intersection -->
    <!-- ============================================================ -->
    <change id="C6" priority="P2" source_item="F7 (minor)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add a debug-level log statement in compute_mask_intersection() that
        records the grid dimensions of the first (reference) mask before the
        3dmask_tool call. Uses 3dinfo -n4 to retrieve dimensions.
      </description>
      <spec>
        After the cache-hit early return block (and after the force_recompute
        os.remove, per C2), add:
            try:
                dim_result = subprocess.run(
                    ["3dinfo", "-n4", mask_paths[0]],
                    capture_output=True, text=True, check=True
                )
                logger.debug(
                    "Mask intersection reference grid (3dinfo -n4): %s — %s",
                    os.path.basename(mask_paths[0]), dim_result.stdout.strip()
                )
            except Exception:
                logger.debug("Could not retrieve grid dimensions for mask: %s", mask_paths[0])

        This is a best-effort debug log; any exception is silently suppressed.
      </spec>
      <dependencies>C2 (must insert after force_recompute os.remove)</dependencies>
      <risk>low — debug-level only; subprocess failure is caught and suppressed</risk>
      <rollback>remove the try/except block</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C7: F8 — Define __version__ constant                        -->
    <!-- ============================================================ -->
    <change id="C7" priority="P2" source_item="F8 (note)">
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Define __version__ = "3.1" as a module-level constant in
        orchestrate_first_level.py. Import and reference it in
        orchestrator_utils.py's consolidate_session_qc() to replace the
        hardcoded "3.1" string literal.
      </description>
      <spec>
        orchestrate_first_level.py:
          Add immediately after the module-level imports (before VALID_TASK_LABELS):
              __version__ = "3.1"

        orchestrator_utils.py:
          In consolidate_session_qc(), replace:
              "orchestrator_version": "3.1",
          with a lazy import of the constant:
              try:
                  from orchestrate_first_level import __version__ as _orch_ver
              except ImportError:
                  _orch_ver = "unknown"
              ...
              "orchestrator_version": _orch_ver,

          The try/except handles environments where the import may fail (e.g.
          unit tests that import orchestrator_utils in isolation).
      </spec>
      <dependencies>none</dependencies>
      <risk>low — fallback to "unknown" if import fails; no production impact</risk>
      <rollback>remove __version__ from orchestrate_first_level.py; restore hardcoded "3.1" in utils</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C8: F10 — Validate task labels in load_orchestrator_config() -->
    <!-- ============================================================ -->
    <change id="C8" priority="P2" source_item="F10 (note)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add VALID_TASK_LABELS validation inside load_orchestrator_config()
        immediately after the per-task task_label check, so invalid labels
        are caught at startup before any S3 requests are issued.

        VALID_TASK_LABELS must be accessible in orchestrator_utils.py without
        a circular import from orchestrate_first_level.py. Solution: define
        VALID_TASK_LABELS as a module-level constant in orchestrator_utils.py
        and import it from there into orchestrate_first_level.py (replacing
        the existing local definition).
      </description>
      <spec>
        orchestrator_utils.py:
          1. Add at module level (after class OrchestratorError, before S3 section):
                 VALID_TASK_LABELS = {"rest", "nback"}

          2. In load_orchestrator_config(), within the per-task validation loop,
             after task_labels.add(task["task_label"]), add:
                 if task["task_label"] not in VALID_TASK_LABELS:
                     raise OrchestratorError(
                         f"tasks[{i}] task_label '{task['task_label']}' is not a recognized "
                         f"task label for this orchestrator. Valid labels: "
                         f"{sorted(VALID_TASK_LABELS)}."
                     )

        orchestrate_first_level.py:
          Replace the existing module-level:
              VALID_TASK_LABELS = {"rest", "nback"}
          with an import from orchestrator_utils:
              from orchestrator_utils import VALID_TASK_LABELS
          (Add to the existing orchestrator_utils import block.)
      </spec>
      <dependencies>none</dependencies>
      <risk>low — raises OrchestratorError at startup for invalid labels; existing production configs are valid</risk>
      <rollback>remove VALID_TASK_LABELS from orchestrator_utils; restore local definition in orchestrate_first_level.py</rollback>
    </change>

  </changes>

  <execution_order>C8, C7, C2, C1, C3, C4, C5, C6</execution_order>
  <!-- Rationale:
       C8 first — moves VALID_TASK_LABELS to orchestrator_utils.py; C7 must come after
       imports are stable. C2 before C6 so the force_recompute os.remove is in place
       when the grid log is inserted after it. C1/C3/C4/C5 are independent within
       orchestrate_first_level.py and can be applied in any order. -->

</implement_plan>
```
