<document_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="document" timestamp="2026-11-04T12:29:57Z" />
  <files_updated>
    <file path="AID_LOG.md" changes="Section 2: removed the statement that AI was not used for running analyses on real data, because the AI tool executed a pre-publication validation run of the pipeline at the researcher's direction. Reworded the closing sentence so the researcher-only claim covers application of the pipeline for scientific analysis. Both wordings were chosen by the researcher. Sections 3, 4 and 5 unchanged. Section 8 untouched.">
      <type>aid_log</type>
    </file>
    <file path=".aid/reports/ (1 new file)" changes="Synced the previous document report (2026-10-30). A term scan found no paths, bucket or producer names, usernames or identifiers, so no sanitization was needed. Transferred with a no-overwrite copy and byte-verified.">
      <type>inline_comment</type>
    </file>
    <file path="README.md" changes="Verified, no change needed. Its install and version-window text stays accurate under the chosen pin resolution.">
      <type>readme</type>
    </file>
    <file path="INPUT_SPECIFICATION.md" changes="Verified, no change needed. The session QC JSON fields, carpet-plot naming, session-root config file and the 12-column motion file under motion_deriv_degree 2 were compared with the structures produced by the validation run, and all matched.">
      <type>input_spec</type>
    </file>
  </files_updated>
  <verification>
    <code_unchanged>No production module, example config, README or INPUT_SPECIFICATION was modified after the previous document report, confirmed by file modification times. No code or comment was edited in this pass.</code_unchanged>
    <tests>Not re-run in this pass, because no code or test file changed during it. The most recent full run, after this session's test renames, passed 442 of 442.</tests>
  </verification>
  <aid_log>
    <status>updated</status>
    <sections_modified>2</sections_modified>
  </aid_log>
  <coverage>
    <public_functions_documented>41/41 (carried over from the previous pass; code unchanged)</public_functions_documented>
    <classes_documented>2/2 (carried over from the previous pass; code unchanged)</classes_documented>
    <modules_with_docstrings>0/3 (all three modules use the project's two-line header comment convention)</modules_with_docstrings>
  </coverage>
  <security_gate>
    <agents>5 (security-gate-agent-sonnet-medium, gate_type combined); patterns and exemptions extracted verbatim from CONVENTIONS.md</agents>
    <scope>4 files: AID_LOG.md, environment.yaml, .aid/project_claude.md and the newly synced report. tests/ is git-ignored and never published.</scope>
    <findings>0 PII/PHI findings and 0 Tier 1 or Tier 2 LLM-attribution findings from all 5 agents. A separate scan of the same files for subject identifiers and individual-level statements also found none.</findings>
  </security_gate>
  <summary>The published documentation matches the current code. The only substantive edit corrects the AID log's description of how AI tooling was used. One functional defect remains open: the dependency pin in environment.yaml installs an upstream build without two required fixes.</summary>
  <action_items>
    <item priority="P0" target_mode="implement" description="Change the fmri-first-level-proc pin in environment.yaml from @v4.0.0 to @e40d1bc6f519b9a61996a594b395fef4ec9c9fd2 (researcher's decision). The v4.0.0 tag resolves to commit 77221b2, which predates both upstream fixes (c9c1c6f and e40d1bc). Both builds report version 4.0.0, so the startup version check cannot tell them apart, and task_act analyses with more than 10 contrasts fail on the tagged build." />
    <item priority="P2" target_mode="end-session" description="Update the project memory Config State: environment.yaml no longer pins v3.2.0, and the conda environment now installs upstream from GitHub commit e40d1bc rather than a local checkout." />
  </action_items>
</document_report>
