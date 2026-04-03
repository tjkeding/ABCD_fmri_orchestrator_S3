<document_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="document" timestamp="2026-03-16T15:20:49+00:00" />
  <files_updated>
    <file path="README.md"
          changes="Three edits: (1) Step 6 description rewritten to remove stale claim that FD is computed by the orchestrator (Power et al. 2012); replaced with accurate non-motion QC description. (2) Step 12 description: removed 'Phase 1 preprocessing metrics' / 'Phase 2 per-analysis' labels; replaced with 'pre-analysis preprocessing metrics' / 'per-analysis status and upstream motion metrics'. (3) QC section headers: removed 'Phase 1' and 'Phase 2' labels from per-run and per-analysis metric blocks.">
      <type>readme</type>
    </file>
    <file path="INPUT_SPECIFICATION.md"
          changes="Section 7 confounds table: four rows updated to remove 'Ignored since Phase 2 refactor' notes and correct the framewise_displacement row description (FD is now computed by fmri_first_level_proc, not recomputed by the orchestrator using Power et al. 2012).">
      <type>input_spec</type>
    </file>
    <file path="example_orchestrator_config.yaml"
          changes="QC block header comment updated: replaced 'Phase 1 (preproc, non-motion)' and 'Phase 2 (per-analysis, motion-based)' labels with 'Pre-analysis preprocessing QC (non-motion)' and 'Per-analysis QC (motion-based)'.">
      <type>inline_comment</type>
    </file>
    <file path="tests/test_simulated_pipeline.py"
          changes="Line 468: test comment updated from 'Phase 1: no motion/censor' to 'non-motion metrics only; no FD/censor'.">
      <type>inline_comment</type>
    </file>
  </files_updated>
  <coverage>
    <public_functions_documented>28/28</public_functions_documented>
    <classes_documented>1/1</classes_documented>
    <modules_with_docstrings>2/2</modules_with_docstrings>
  </coverage>
  <summary>All internal development markers have been removed from active documentation and code. Specifically: (1) 'Phase 1' / 'Phase 2' labels removed from README.md (3 locations), INPUT_SPECIFICATION.md (4 table rows), example_orchestrator_config.yaml (QC block header), and tests/test_simulated_pipeline.py (1 comment); (2) stale FD-computation claim in README.md Step 6 corrected to reflect that FD is computed exclusively by fmri_first_level_proc, not by the orchestrator; (3) 'Ignored since Phase 2 refactor' table notes in INPUT_SPECIFICATION.md replaced with accurate architectural descriptions. No functional code was modified. All docstrings in orchestrate_first_level.py and orchestrator_utils.py were verified to be free of stale references. The orchestrate_first_level.py and orchestrator_utils.py files had already been updated in prior sessions (implement_build reports confirm Phase labels were removed from docstrings of generate_carpet_plot, compute_preproc_qc, and consolidate_session_qc). The REFACTOR_PLAN.md retains 'Phase 1' and 'Phase 2' section headings intentionally as historical record of the refactor structure. Session artifact .md files (brainstorm, implement_build, implement_plan, cr_history) retain internal development terminology as archival records.</summary>
</document_report>
