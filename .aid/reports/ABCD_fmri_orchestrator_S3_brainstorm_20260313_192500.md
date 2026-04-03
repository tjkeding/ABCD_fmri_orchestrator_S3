# Brainstorm: CR2 Findings Disposition (F1-F11)

```xml
<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-03-13T19:25:00Z" />

  <context_files>
    <file path="ABCD_fmri_orchestrator_S3_cr_20260313_192000.md" relevance="Source CR2 report — 11 findings from review of C1-C10 implementation" />
    <file path="orchestrate_first_level.py" relevance="Main orchestrator — affected by F1, F2, F4, F6, F9, F10" />
    <file path="orchestrator_utils.py" relevance="Core utilities — affected by F2, F7, F8, F10" />
  </context_files>

  <topics>

    <topic id="T1" title="CR2 findings disposition">
      <summary>
        Second critical review (CR2) of the C1-C10 implementation produced
        11 findings (2 critical, 2 major, 4 minor, 3 notes). User reviewed
        all findings and made the following decisions.
      </summary>

      <decisions>

        <!-- FIX -->
        <decision finding="F1" severity="critical" action="fix" priority="P0">
          session_wall_time hardcoded to 0.0 in consolidated QC JSON.
          Fix: track time.time() at top of _process_session(), pass actual
          elapsed to consolidate_session_qc().
        </decision>

        <decision finding="F2" severity="critical" action="fix" priority="P0">
          force_recompute broken for all 5 AFNI-backed caching functions
          (AFNI refuses to overwrite existing output). Fix: os.remove(out_path)
          before AFNI call when force_recompute=True.
        </decision>

        <decision finding="F4" severity="major" action="fix" priority="P1">
          Session status derivation duplicated in two locations. Fix: extract
          _derive_session_status() module-level helper, call from both
          _process_session() and process_participant().
        </decision>

        <decision finding="F9" severity="minor" action="fix" priority="P1">
          Pre-existing bug: per-run smoothing loop uses run_dicts[i] but
          per_run_bolds skips failed runs, causing index mismatch. Fix: use
          per_run_masks[i] and tracked run labels in the smoothing loop.
        </decision>

        <decision finding="F6" severity="minor" action="fix" priority="P2">
          detect_non_steady_state_trs() called twice per run on same file.
          Fix: call once before Step 5, reuse for both QC and trimming.
        </decision>

        <decision finding="F7" severity="minor" action="fix" priority="P2">
          compute_mask_intersection() does not verify grid match before
          3dmask_tool -inter. Fix: add debug-level log confirming grid
          dimensions match.
        </decision>

        <decision finding="F8" severity="note" action="fix" priority="P2">
          Version "3.1" hardcoded in 3 places. Fix: define __version__
          module constant, reference in consolidate_session_qc() and headers.
        </decision>

        <decision finding="F10" severity="note" action="fix" priority="P2">
          VALID_TASK_LABELS not checked until Step 4+; downloads waste S3
          requests on invalid labels. Fix: validate task labels in
          load_orchestrator_config() for fail-fast at startup.
        </decision>

        <!-- DEFER -->
        <decision finding="F3" severity="major" action="defer" rationale="Conservative NaN handling is warranted. NaN in motion files should be extremely rare in ABCD, and erring toward more censoring (not less) is the correct scientific choice. The inflated derivative-propagated NaN count in the log is acceptable." />

        <decision finding="F5" severity="minor" action="defer" rationale="Rotation unit check false-positive risk is limited to phantom/sedated populations outside ABCD scope. No change needed for this orchestrator." />

        <decision finding="F11" severity="note" action="defer" rationale="Automatically resolved by F4 — once _derive_session_status() is extracted as a module-level helper, the test will import and exercise the actual production function." />

      </decisions>
    </topic>

  </topics>
</brainstorm_report>
```
