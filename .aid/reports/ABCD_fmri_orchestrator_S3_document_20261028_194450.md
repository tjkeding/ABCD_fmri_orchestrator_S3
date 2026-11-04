<document_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="document" timestamp="2026-10-28T19:44:50Z" />
  <files_updated>
    <file path="README.md" changes="Full rewrite from the current code: overview, plain ordered pipeline overview, requirements (fmri-first-level-proc >= 4.0.0 and < 5.0.0, AFNI, AWS), installation, quick start, command-line reference for both entry points, exit codes, configuration summary, example analyses, input layout, output tree, session outcomes and completion sentinel, quality control with a group-level aggregation example, design notes, troubleshooting, repository contents, references, MIT license section. Placeholder IDs in SUBJ01 style, no bucket or producer names, no em-dashes, no step labels or numbered headings.">
      <type>readme</type>
    </file>
    <file path="INPUT_SPECIFICATION.md" changes="Full rewrite from the current code: every command-line flag and exit code, every orchestrator config field with type, default and validation message, the proc-config fields the orchestrator fills or reads, proc-config validation, producer input layout (discovery patterns, completion manifest, input-completeness gate, confounds, masks, events), run preparation (trimming, rotation-unit check, derivative count, tissue regressors, task timing with cue relabeling), run exclusion reasons, analysis-level processing, first-level QC, session outcome classification, S3 routing, upload and cleanup, all outputs including the session QC JSON schema and batch summary CSV, environment, and edge cases and known limitations.">
      <type>input_spec</type>
    </file>
    <file path="AID_LOG.md" changes="Section 3 tool table updated to the model families actually used (user-confirmed). Sections 4 and 5 factual corrections: three critical-review rounds (19, 11 and 11 findings); current suite of 454 tests, all passing; the real-data run (30 participants, 131 of 133 analyses) attributed to an earlier pipeline version; current motion strategy (producer confounds with a per-run rotation-unit check against producer framewise displacement; FD computation and censoring delegated to fmri-first-level-proc with thresholds in its configuration); clean reviews described as repeated cycles; hyphenated upstream name. An unverifiable simulated-session count was removed. Section 8 untouched.">
      <type>aid_log</type>
    </file>
    <file path="orchestrator_utils.py" changes="validate_proc_template docstring: numbered check list replaced by an accurate bullet list (adds the missing per-block name check). fix_nback_cue_labels: Raises text and look-ahead comment corrected to the {level}_back_{stimulus} match the code performs. compute_preproc_qc docstring: removed a change-history sentence. AfniUnvalidatedVersionFilter.filter: docstring added. Code verified unchanged by docstring-stripped AST comparison.">
      <type>docstring</type>
    </file>
    <file path="orchestrate_first_level.py" changes="main(): docstring added. Code verified unchanged.">
      <type>docstring</type>
    </file>
    <file path="run_orchestrator.py" changes="main(): docstring added. Code verified unchanged.">
      <type>docstring</type>
    </file>
    <file path=".aid/reports/ (32 new files)" changes="Synced this project's own brainstorm, critical review, clean, implementation plan and build, and test reports dated 2026-09-30 through 2026-10-28. Sanitized: local absolute paths to repo-relative or placeholder form, conda path to a placeholder, producer name and owner to generic terms, bucket names to placeholders, an AWS profile name and run nonces to placeholders, participant-format tokens to a placeholder. Transferred with a no-overwrite copy under a session-bound grant and byte-verified against the reviewed scratch copies.">
      <type>inline_comment</type>
    </file>
  </files_updated>
  <aid_log>
    <status>updated</status>
    <sections_modified>3, 4, 5</sections_modified>
  </aid_log>
  <coverage>
    <public_functions_documented>42/42</public_functions_documented>
    <classes_documented>2/2</classes_documented>
    <modules_with_docstrings>0/3 (all three modules use the project's header comment block convention: title, purpose and author; retained as the existing convention)</modules_with_docstrings>
  </coverage>
  <security_gate>
    <agents>5 (security-gate-agent-sonnet-medium, gate_type combined); patterns and exemptions extracted verbatim from CONVENTIONS.md</agents>
    <scope>43 files: README.md, INPUT_SPECIFICATION.md, AID_LOG.md, the three Python modules, LICENSE, .gitignore, both example configs, .aid/project_claude.md, and the 32 newly synced reports. Git-ignored local-only files (production configs, tests/) are excluded because they are never published.</scope>
    <findings>6 unique PII-tier findings after union-dedup (reported by 4 of 5 agents): uppercase UUID run nonces in two synced test reports. Remediated in place to session-1 through session-6; re-scan clean. 0 Tier 1 and 0 Tier 2 LLM-attribution findings.</findings>
  </security_gate>
  <summary>User-facing documentation now describes only the current pipeline (fmri-first-level-proc >= 4.0.0 and < 5.0.0, per-file producer inputs with completion manifests, verified per-file S3 upload with a completion sentinel). All public functions and classes carry docstrings, the AID log reflects the current workflow and tools, .aid/project_claude.md remains a faithful sanitized copy, and the AID audit trail is complete through 2026-10-28. Two correctness defects, one performance/noise issue, and one test assertion are routed below.</summary>
  <action_items>
    <item priority="P1" target_mode="implement" description="Invalid manifest in an unrequested session stops the participant: discover_subject_inputs (orchestrator_utils.py:847-884) validates every session's _complete.json (schema_version, kind, file-presence check) before returning, and --session filtering is applied only afterwards (orchestrate_first_level.py:156-164). When --session targets session 00, an invalid or inconsistent manifest in session 02 raises OrchestratorError and stops the entire participant. Fix: defer manifest-validity errors (not missing-manifest skips, which are correct) to a post-filter validation pass, or skip non-selected sessions' validation when a session filter is active. The completeness gate at line 167 already operates on selected_discovered, so only the manifest check needs adjustment." />
    <item priority="P1" target_mode="implement" description="Local-mode discovery walks the entire input_dir: discover_subject_inputs (orchestrator_utils.py:728-733) uses os.walk(input_dir) without restricting to the target participant's subdirectory. With thousands of participants under one input_dir, this produces thousands of 'Subject token differs' warnings and wastes time scanning files that cannot match. Fix: in local mode, build the participant-specific subdirectory path from the prefix templates (filling {subject} but listing from the {session}-containing parent) and walk only that, falling back to the full walk only when the templates are not available." />
    <item priority="P2" target_mode="test" description="Re-express the test_s3_enabled_with_v7_templates assertion in tests/test_production_configs.py to expect bucket '<bucket>' instead of the old placeholder '__PRODUCER_BUCKET__'. The user-approved bucket edit in the production config changed the actual value; this is an obsolete-test disposition." />
  </action_items>
</document_report>
