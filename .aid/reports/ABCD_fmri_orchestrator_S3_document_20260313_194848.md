<document_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="document" timestamp="2026-03-13T19:48:48Z" />
  <files_updated>
    <file path="README.md"
          changes="QC section rewritten from per-run/per-analysis JSONs to consolidated session QC JSON; output directory structure updated (removed per-run preproc_qc.json and first_level_qc.json, added orchestrator_qc.json and concat_mask.nii.gz); group-level aggregation example updated; Step 12 description expanded to include Step 12b (consolidate_session_qc); Section J2 (compute_mask_intersection) added to section index and function-to-step mapping; discover_local_mmps_files added to Section A; consolidate_session_qc added to Section K and step mapping; No Motion-Based Gating design note updated to reference new QC field paths; Partial Success Model section already accurate; Preprocessing QC description corrected to Phase 1 non-motion-only metrics">
      <type>readme</type>
    </file>
    <file path="INPUT_SPECIFICATION.md"
          changes="Section 10.1 (Preprocessing QC JSON) replaced with Section 10.1 (Consolidated Session QC JSON) reflecting the new single-file-per-session schema with provenance, preprocessing, analyses, and session blocks; duplicate section header 10.2/10.3 deduplicated; Section 10.4 renumbered to 10.3; Section 8b.3 (Processing Flow for Motion TSV) corrected to remove FD computation from compute_preproc_qc() and clarify that FD is produced by fmri_first_level_proc; upstream QC summary path documented">
      <type>input_spec</type>
    </file>
    <file path="example_orchestrator_config.yaml"
          changes="QC block header comment updated to describe the two-phase consolidated approach; carpet_plots comment corrected to note FD removed from Phase 1">
      <type>inline_comment</type>
    </file>
    <file path="orch_config_final.yaml"
          changes="QC block header comment updated to reflect consolidated JSON output and two-phase QC architecture">
      <type>inline_comment</type>
    </file>
  </files_updated>
  <coverage>
    <public_functions_documented>37/37</public_functions_documented>
    <classes_documented>1/1</classes_documented>
    <modules_with_docstrings>2/2</modules_with_docstrings>
  </coverage>
  <summary>
    Documentation updated to reflect Session 14 implementation changes (v3.1). The primary
    architectural change documented is the QC overhaul: Phase 1 preprocessing QC now contains
    only non-motion metrics (DVARS, tSNR, brain mask coverage, registration Dice), while Phase 2
    motion metrics (FD, censor counts, DOF) are sourced exclusively from the upstream
    fmri_first_level_proc QC summary JSON. Both phases are consolidated into a single
    orchestrator_qc.json per session rather than the former pattern of N per-run
    preproc_qc.json files and N per-analysis first_level_qc.json files.

    Secondary changes documented: qualified session status (success/partial/failed) replacing
    binary reporting; compute_mask_intersection (Section J2) added to section index and
    function-to-step mapping; discover_local_mmps_files added to Section A; concat_mask.nii.gz
    added to output directory structure. All docstrings in orchestrator_utils.py (36/36) and
    orchestrate_first_level.py (3/4 — main() intentionally undocumented as a CLI entry point)
    were verified accurate against the current codebase.

    Note: tests/golden_config_baseline.yaml contains a stale censor_path field. This is a test
    fixture file and was not modified (functional code is outside document mode scope).
  </summary>
</document_report>
