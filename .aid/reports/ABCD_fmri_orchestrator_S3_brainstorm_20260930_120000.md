<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-09-30T12:00:00-04:00" />
  <context_files>
    <file path="orchestrate_first_level.py" relevance="Main entry point; contains dispatch loop, error handling, and upstream API integration (lines 37-39, 814-871, 993)" />
    <file path="orchestrator_utils.py" relevance="Core utilities; contains verify_afni_installation (1156), format_task_timing (2018), concatenate_task_timing (2208), compute_first_level_qc (2811), build_first_level_config (3019), validate_proc_template (3427)" />
    <file path="example_proc_config.yaml" relevance="Proc template with v2.5.0 extraction schema (stat_buckets migration target)" />
    <file path="example_orchestrator_config.yaml" relevance="Orchestrator config; version references at line 64" />
    <file path="README.md" relevance="Version references at lines 67, 197, 356" />
    <file path="INPUT_SPECIFICATION.md" relevance="Version references at lines 191, 296, 449, 563" />
    <file path="github:tjkeding/fmri_first_level_proc@0ad0445:fmri_first_level_proc/__init__.py" relevance="v3.1.0 public API exports (ResolvedConfig, FirstLevelResult, DISPATCH, etc.)" />
    <file path="github:tjkeding/fmri_first_level_proc@0ad0445:fmri_first_level_proc/first_level_config.py" relevance="v3.1.0 ResolvedConfig dataclass and load_and_validate return type" />
    <file path="github:tjkeding/fmri_first_level_proc@0ad0445:fmri_first_level_proc/exceptions.py" relevance="v3.1.0 custom exception hierarchy (7 typed exceptions)" />
    <file path="github:tjkeding/fmri_first_level_proc@0ad0445:fmri_first_level_proc/runtime.py" relevance="v3.1.0 AFNI binary preflight and version-lock validation" />
    <file path="github:tjkeding/fmri_first_level_proc@0ad0445:fmri_first_level_proc/first_level_utils.py" relevance="v3.1.0 FirstLevelResult dataclass and graded_warning API" />
    <file path="github:tjkeding/fmri_first_level_proc@0ad0445:fmri_first_level_proc/run_first_level.py" relevance="v3.1.0 DISPATCH table, main() CLI entry point, exit code mapping" />
    <file path="github:tjkeding/fmri_first_level_proc@0ad0445:example_config.yaml" relevance="v3.1.0 config schema reference (stat_buckets, scale_to_psc)" />
    <file path="github:tjkeding/fmri_first_level_proc@0ad0445:INPUT_SPECIFICATION.md" relevance="v3.1.0 full input specification (BIDS timing format, extraction API, exit codes)" />
  </context_files>
  <topics>
    <topic id="S1" title="load_and_validate return type migration">
      <summary>The upstream load_and_validate() changed from returning list[tuple[str, argparse.Namespace, str]] (v2.5.0) to list[ResolvedConfig] (v3.1.0). The orchestrator's tuple unpacking at orchestrate_first_level.py:817 will crash with ValueError at runtime.</summary>
      <research>No external research required. Change is deterministic and visible in the upstream first_level_config.py diff (tuple return -> ResolvedConfig dataclass return). ResolvedConfig attribute names (analysis_type, analysis_name, out_dir, out_file_pre) are identical to the old Namespace attributes.</research>
      <approaches>
        <approach id="A1" label="ResolvedConfig attribute access" feasibility="high" risk="low">
          <description>Replace tuple unpacking with direct attribute access on ResolvedConfig objects. Change: for i, (atype, ns, name) in enumerate(analysis_list) becomes for i, config in enumerate(configs) with atype = config.analysis_type, name = config.analysis_name. All downstream references to ns become config.</description>
          <pros>Mechanical rename; no semantic change; attribute names identical between Namespace and ResolvedConfig.</pros>
          <cons>None.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">Mechanical migration. No design decisions required. Adopted as written.</decision>
    </topic>
    <topic id="S2" title="Pipeline error handling migration">
      <summary>Pipeline run() functions now return FirstLevelResult instead of None and raise typed exceptions (ModelError, ToolExecutionError, etc.) instead of calling sys.exit(). The orchestrator's except SystemExit clause (line 829) will never trigger for upstream errors; failures fall through to generic except Exception, losing structured exit-code semantics.</summary>
      <research>No external research required. Exception hierarchy and FirstLevelResult are defined in upstream exceptions.py and first_level_utils.py respectively. Exit code mapping documented in upstream README and INPUT_SPECIFICATION.</research>
      <approaches>
        <approach id="A1" label="Typed exception catches + FirstLevelResult capture" feasibility="high" risk="low">
          <description>Three sub-changes: (A) Capture FirstLevelResult from run_fn(config, logger). (B) Replace SystemExit/Exception catches with typed catches: ModelError (exit 5), ToolUnavailableError/ToolVersionError/ToolExecutionError (exit 4), ConfigError/InputError (exit 2), GuardError (exit 1), Exception (fallback). (C) Forward exception context dicts (e.context) into QC output for richer audit trails.</description>
          <pros>Preserves per-analysis error isolation; provides richer error classification for QC; aligns with upstream's intentional exception design; eliminates dead SystemExit catch.</pros>
          <cons>Adds 6 new imports from fmri_first_level_proc.exceptions.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">Adopt all three sub-changes (A+B+C). Import addition: from fmri_first_level_proc.exceptions import ConfigError, InputError, ModelError, GuardError, ToolUnavailableError, ToolVersionError, ToolExecutionError.</decision>
    </topic>
    <topic id="S3" title="AFNI preflight delegation">
      <summary>The orchestrator's verify_afni_installation() checks only the afni binary. The upstream v3.1.0 provides preflight_tool_environments() which validates all 15 required AFNI binaries. The orchestrator also decided to stay with the programmatic API (not CLI) based on logging coherence as the decisive factor for EC2 batch debugging.</summary>
      <research>No external research required. preflight_tool_environments() defined in upstream runtime.py; binary list verified against _AFNI_BINARIES constant. probe_tool_versions() architecture (lockfile resolves relative to repo root, absent for pip-installed consumers) verified from runtime.py source.</research>
      <approaches>
        <approach id="A1" label="Delegate preflight, skip version probing" feasibility="high" risk="low">
          <description>Replace verify_afni_installation(logger) call at orchestrate_first_level.py:993 with preflight_tool_environments(logger=logger), wrapped in try/except ToolUnavailableError -> OrchestratorError. Remove verify_afni_installation() from orchestrator_utils.py. Do NOT adopt probe_tool_versions() (Option A): the lockfile is architecturally absent from pip-installed packages, so calling it would only emit a LOCKFILE_ABSENT warning.</description>
          <pros>15-binary preflight vs. single binary check; removes redundant code; upstream maintains the binary list.</pros>
          <cons>None.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">Delegate to upstream preflight_tool_environments(). Skip probe_tool_versions() (Option A). Stay with programmatic API (not CLI). Remove verify_afni_installation() as dead code.</decision>
    </topic>
    <topic id="S4" title="Warning accumulator and FirstLevelResult integration">
      <summary>FirstLevelResult carries structured warnings (graded_warning records with severity, code, message) and qc_summary (same content as disk QC JSON). The orchestrator currently ignores the return value and reconstructs QC by post-hoc file inspection in compute_first_level_qc().</summary>
      <research>No external research required. FirstLevelResult schema defined in upstream first_level_utils.py. graded_warning record format (severity, code, message) defined in same module.</research>
      <approaches>
        <approach id="A1" label="Use FirstLevelResult on success, retain compute_first_level_qc as error-path fallback" feasibility="high" risk="low">
          <description>On the success path, populate outcome dict from result.qc_summary, result.warnings, result.output_files. On the error path (exception caught, no FirstLevelResult), fall back to compute_first_level_qc() for best-effort output inspection. This retains debugging capability for partial failures while eliminating redundant file inspection on the success path.</description>
          <pros>Authoritative QC from upstream on success; preserves debugging for failures; no lost functionality.</pros>
          <cons>Two code paths for QC population (success vs. error); slightly more complex dispatch loop.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">Option B: retain compute_first_level_qc as error-path fallback. Success path uses FirstLevelResult directly.</decision>
    </topic>
    <topic id="S5" title="Proc template config schema update">
      <summary>The v3.1.0 config schema replaces the task_act extraction fields (extract, extract_labels, extract_stat) with the stat_buckets API and adds scale_to_psc for all pipeline types.</summary>
      <research>No external research required. Schema changes verified by diff between v2.5.0 and v3.1.0 first_level_config.py. VALID_STAT_BUCKET_TYPES = {"Coef", "Tstat", "Fstat", "R^2", "Zstat"} verified from upstream source.</research>
      <approaches>
        <approach id="A1" label="Migrate extraction schema + add scale_to_psc" feasibility="high" risk="low">
          <description>task_act extraction block: remove extract, extract_labels, extract_stat. Add stat_buckets: ["Coef", "Tstat", "Zstat"]. Add scale_to_psc: true explicitly for all three analysis blocks. Update comments for CLI rename and timing format references.</description>
          <pros>Aligns with upstream schema; enables multi-stat extraction (Coef+Tstat+Zstat); PSC scaling is standard practice.</pros>
          <cons>None.</cons>
          <statistical_considerations>stat_buckets selection: Coef (beta coefficients) for effect size, Tstat for parametric inference, Zstat for cross-study comparability and meta-analysis. This covers the three standard statistical outputs for group-level fMRI analyses.</statistical_considerations>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">stat_buckets: ["Coef", "Tstat", "Zstat"]. scale_to_psc: true explicit for all three blocks. Old extraction fields removed. Comments updated.</decision>
    </topic>
    <topic id="S6" title="Task timing format migration">
      <summary>The orchestrator's format_task_timing() and concatenate_task_timing() produce/consume legacy CSV (CONDITION, ONSET, DURATION). The upstream v3.1.0 prefers BIDS TSV (trial_type, onset, duration) and deprecates CSV with a warning. Legacy CSV support is announced for removal in v4.0.0.</summary>
      <research>No external research required. Upstream timing format detection logic documented in INPUT_SPECIFICATION.md and implemented in first_level_utils.py. Legacy CSV deprecation warning (CSV_DEPRECATED) emitted at runtime for every legacy CSV file processed.</research>
      <approaches>
        <approach id="A1" label="Migrate to BIDS TSV now" feasibility="high" risk="low">
          <description>format_task_timing(): column names CONDITION->trial_type, ONSET->onset, DURATION->duration; separator comma->tab; output .csv->.tsv. concatenate_task_timing(): read with sep="\t"; column reference ONSET->onset; write with sep="\t". Callers: two file extension changes (.csv->.tsv) at orchestrate_first_level.py lines 644 and 726.</description>
          <pros>Eliminates ~11,000 x N_sessions deprecation warnings in batch runs; preempts v4.0.0 CSV removal; aligns with BIDS standard; mechanical 6-line change.</pros>
          <cons>None. Upstream detection is content-authoritative (inspects header columns), so the change is safe.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">Migrate to BIDS TSV now. Six code lines + two file extension updates.</decision>
    </topic>
    <topic id="S7" title="Documentation and version reference update">
      <summary>14 reference sites across published documentation and config files cite fmri_first_level_proc >= 2.4.0 or >= 2.5.0. These need updating to >= 3.1.0 to reflect the new minimum compatible version. Three CLI references cite the deprecated run-first-level command.</summary>
      <research>No external research required. Reference sites identified by grep across *.py, *.yaml, *.md files. AID_LOG.md historical entry (line 106) is a historical record and should not be updated.</research>
      <approaches>
        <approach id="A1" label="Update all non-historical references" feasibility="high" risk="low">
          <description>Update 13 version references to >= 3.1.0 (all except AID_LOG.md line 106 historical entry). Update 3 CLI references from run-first-level to fmri-first-level-proc in example_proc_config.yaml. Detailed documentation updates (README pipeline section, INPUT_SPECIFICATION proc template reference, timing format section) deferred to /document phase.</description>
          <pros>Accurate versioning across all published files; historical records preserved.</pros>
          <cons>None.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">Update all non-historical refs. Leave AID_LOG historical entry intact. Detailed doc updates deferred to /document.</decision>
    </topic>
  </topics>
  <action_items>
    <item priority="P0" target_mode="implement" description="S1: Migrate load_and_validate return type handling from tuple unpacking to ResolvedConfig attribute access (orchestrate_first_level.py:814-826)" />
    <item priority="P0" target_mode="implement" description="S2: Replace SystemExit/Exception catches with typed FmriFirstLevelError subclass catches; capture FirstLevelResult; forward e.context to QC (orchestrate_first_level.py:826-871)" />
    <item priority="P0" target_mode="implement" description="S3: Replace verify_afni_installation() with upstream preflight_tool_environments(); remove dead code (orchestrate_first_level.py:993, orchestrator_utils.py:1156-1186)" />
    <item priority="P1" target_mode="implement" description="S4: Use FirstLevelResult fields (qc_summary, warnings, output_files) on success path; retain compute_first_level_qc() as error-path fallback (orchestrate_first_level.py:826-871)" />
    <item priority="P1" target_mode="implement" description="S5: Update example_proc_config.yaml: replace extract/extract_labels/extract_stat with stat_buckets: ['Coef', 'Tstat', 'Zstat']; add scale_to_psc: true for all three blocks; update CLI and timing comments" />
    <item priority="P1" target_mode="implement" description="S6: Migrate format_task_timing() and concatenate_task_timing() from legacy CSV to BIDS TSV (column names, separator, file extension); update callers at orchestrate_first_level.py:644,726" />
    <item priority="P1" target_mode="document" description="S7: Update 13 version references to >= 3.1.0 and 3 CLI references to fmri-first-level-proc across README.md, INPUT_SPECIFICATION.md, example_orchestrator_config.yaml, example_proc_config.yaml, orchestrator_utils.py" />
    <item priority="P2" target_mode="test" description="Update existing test fixtures and golden configs for v3.1.0 schema changes (stat_buckets, BIDS TSV timing, scale_to_psc, ResolvedConfig)" />
  </action_items>
  <next_steps>Proceed to /implement to execute the 7 changes (S1-S7). S1-S3 are P0 (breaking: code will not run without them). S4-S6 are P1 (alignment). S7 is P1 for code references and deferred to /document for detailed prose updates. Test fixture updates (P2) should follow implementation via /test design.</next_steps>
</brainstorm_report>
