<document_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="document" timestamp="2026-05-14T17:34:45Z" />
  <files_updated>
    <file path="README.md" changes="Section N (Function-to-Pipeline-Step Mapping): removed the deleted `compress_session_outputs` symbol; renamed the row label from 'Output Compression/Cleanup' to 'Output Cleanup' to reflect the post-2026-05-14 per-file upload contract.">
      <type>readme</type>
    </file>
    <file path="INPUT_SPECIFICATION.md" changes="Motion file format section: removed the obsolete 'converted to radians by the orchestrator' parenthetical and added the correct passthrough wording, citing the `fmri_first_level_proc` >= 2.4.0 degrees-only input contract. Sequenced denoising note: replaced the 'bundled into the orchestrator session archive' / 'archival' language with 'retained in the session output tree and included in the per-file S3 upload' / 'never reach S3' to match the post-2026-05-14 per-file upload contract.">
      <type>input_spec</type>
    </file>
    <file path=".aid/reports/ABCD_fmri_orchestrator_S3_brainstorm_20260514_154849.md" changes="Synced from project root to .aid/reports/ verbatim; paths in the source report were already relative, so no sanitization was required.">
      <type>aid_report</type>
    </file>
    <file path=".aid/reports/ABCD_fmri_orchestrator_S3_implement_plan_20260514_120440.md" changes="Synced from project root to .aid/reports/ verbatim; paths in the source report were already relative, so no sanitization was required.">
      <type>aid_report</type>
    </file>
    <file path=".aid/reports/ABCD_fmri_orchestrator_S3_implement_build_20260514_163512.md" changes="Synced from project root to .aid/reports/ with absolute project-root path prefix stripped from `spec_ref` and every `<file path=...>` attribute, bringing the report into alignment with the relative-path convention used by all prior published reports in this directory.">
      <type>aid_report</type>
    </file>
    <file path=".aid/reports/ABCD_fmri_orchestrator_S3_test_20260514_171124.md" changes="Synced from project root to .aid/reports/ with the two absolute project-root path references in the failing_test_dispositions notes and the design_phase files_created file path stripped to relative form.">
      <type>aid_report</type>
    </file>
  </files_updated>
  <aid_log>
    <status>unchanged</status>
    <sections_modified>none</sections_modified>
    <notes>AID_LOG.md substantive sections 1-7 remain accurate for the 2026-05-14 cycle. The Tools Used table, Development Workflow stages, Human Oversight contributions, and Audit Trail description still describe the project state correctly. Section 8 (Version History) is managed exclusively by the /publish skill per the AID_LOG template doctrine and is therefore intentionally not updated here. The 2026-05-14 cycle's Version History entry will be authored by /publish when the cycle is promoted to GitHub.</notes>
  </aid_log>
  <coverage>
    <public_functions_documented>existing docstrings preserved; no new public functions outside the four added by the 2026-05-14 implement build, which were documented at build time</public_functions_documented>
    <classes_documented>existing</classes_documented>
    <modules_with_docstrings>existing</modules_with_docstrings>
  </coverage>
  <summary>Documentation pass for the 2026-05-14 per-file-upload + legacy-archive auto-migration cycle. Two stale references in published docs were corrected (README.md Section N still listed the deleted `compress_session_outputs`; INPUT_SPECIFICATION.md claimed the orchestrator converted motion rotations to radians, contradicting the `fmri_first_level_proc` >= 2.4.0 degrees-only contract that has been in force since 2026-05-04). One additional stale reference was found and corrected in INPUT_SPECIFICATION.md (sequenced denoising note referring to a no-longer-existing 'session archive'). The four reports produced by the 2026-05-14 cycle (brainstorm, implement plan, implement build, test design) were synced to .aid/reports/ with absolute-path sanitization where required, matching the relative-path convention of existing published reports. AID_LOG.md substantive sections remain accurate; the Version History section was intentionally not edited as that is /publish's responsibility. PII Screening Gate and LLM-Attribution Scrub Gate were both executed on all touched files. PII scan returned zero hits across all touched files. Tier 1/Tier 2 LLM-attribution scan returned hits exclusively in AID_LOG.md, all of which fall under the AID (AI Disclosure) Framework exemption for the canonical disclosure document. The project is ready for /publish.</summary>
</document_report>
