<document_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="document" timestamp="2026-04-03T17:24:12Z" />
  <files_updated>
    <file path="README.md" changes="Corrected three factual errors: (1) S3 archive contents description (archive includes qc/, preproc/ provenance, and concat/ provenance in addition to first_level_out/); (2) Step 8 description (rotations remain in degrees per fmri_first_level_proc >= 2.4.0 contract, NaN imputed to 999.0 not 0.0); (3) Design Decisions / Motion Data section (removed incorrect claim that rotations are converted to radians).">
      <type>readme</type>
    </file>
    <file path="INPUT_SPECIFICATION.md" changes="Corrected Section 8b: replaced 'Degree-to-Radian Conversion' subsection with accurate 'Rotation Unit Handling' subsection (no conversion applied; ambiguity check described; NaN imputed to 999.0). Updated Section 8b Processing Flow step 3 to match current code. Added force_recompute field to Section 2.1 study block table and validation rules. Added force_recompute validation row to Section 9 validation table.">
      <type>input_spec</type>
    </file>
    <file path="AID_LOG.md" changes="Created from template. Sections 2, 4, and 5 adapted to reflect ABCD_fmri_orchestrator_S3 project scope, actual workflow stages used, and specific human oversight contributions. Sections 1, 3, 6, 7 use stable template language.">
      <type>aid_log</type>
    </file>
    <file path="example_orchestrator_config.yaml" changes="Corrected calc_n_motion_derivs comment: replaced 'Rotations are converted from degrees to radians' with accurate statement that rotations are passed through in original units (degrees) per fmri_first_level_proc >= 2.4.0 input contract.">
      <type>inline_comment</type>
    </file>
    <file path="orchestrator_utils.py" changes="Added or completed docstrings for 12 public functions: apply_brain_mask, detect_non_steady_state_trs, remove_initial_trs_bold, remove_initial_trs_tabular, extract_tissue_signals, format_task_timing, save_qc_json, compute_tsnr, compute_registration_quality, write_temp_config, load_orchestrator_config, verify_afni_installation. Removed stray blank lines before docstrings in compute_preproc_qc, compute_first_level_qc, and format_task_timing.">
      <type>docstring</type>
    </file>
  </files_updated>
  <aid_log>
    <status>created</status>
    <sections_modified>All sections (new file). Sections 2, 4, 5 adapted to project specifics; Sections 1, 3, 6, 7 from template.</sections_modified>
  </aid_log>
  <coverage>
    <public_functions_documented>32/32</public_functions_documented>
    <classes_documented>1/1</classes_documented>
    <modules_with_docstrings>3/3</modules_with_docstrings>
  </coverage>
  <summary>Documentation was substantially accurate but contained three factual errors introduced by the v2.4.0 upstream contract change (motion rotations remaining in degrees rather than being converted to radians by the orchestrator, and NaN imputation to 999.0 rather than 0.0). All three errors were corrected across README.md, INPUT_SPECIFICATION.md, and example_orchestrator_config.yaml. A previously undocumented field (force_recompute) was added to INPUT_SPECIFICATION.md. AID_LOG.md was created. Twelve public functions in orchestrator_utils.py received complete or materially improved NumPy-style docstrings. No functional code was modified.</summary>
</document_report>
