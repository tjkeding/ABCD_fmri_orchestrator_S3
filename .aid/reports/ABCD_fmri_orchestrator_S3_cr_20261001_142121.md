<cr_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="cr" timestamp="2026-10-01T14:21:21Z" />

  <scope>
    Adversarial critical review of all changes made to the ABCD_fmri_orchestrator_S3 codebase during
    harmonization with fmri_first_level_proc v3.1.0. The review covers the uncommitted diff on the
    main branch plus pre-existing code paths exposed by the diff. Upstream source files from the
    installed fmri_first_level_proc v3.1.0 package were read for cross-project verification.

    Files reviewed (orchestrator):
    - orchestrate_first_level.py (1028 LOC; modified in diff)
    - orchestrator_utils.py (3590 LOC; modified in diff)
    - example_proc_config.yaml (modified in diff)
    - environment.yaml (modified in diff)
    - README.md
    - INPUT_SPECIFICATION.md
    - proc_config_final.yaml (local only)
    - tests/test_simulated_pipeline.py
    - tests/test_cr_implementation_extended.py

    Files reviewed (upstream fmri_first_level_proc v3.1.0):
    - task_act_first_level.py
    - first_level_utils.py (scale_to_psc, build_deconvolve_base)
    - first_level_config.py (ResolvedConfig schema, remove_previous, stat_buckets)
    - checkpoint.py (config-hash ledger, dead code)
    - INPUT_SPECIFICATION.md
  </scope>

  <research_conducted>
    Proposal verification (PV) agents were dispatched for all eight findings (N=3 per finding,
    24 total). PV agents independently verified each recommendation against the codebase and
    upstream documentation. Key PV outcomes that revised recommendations:

    - F2: All 3 PV agents raised concerns about the initial documentation-only recommendation's
      scope. Revised to dual-pathway design covering both task_act and task_conn, removing the
      "(recommended)" label from scale_to_psc for multi-run analyses.
    - F5: PV-1 and PV-2 identified that the proposed result.status check is vacuous (always True)
      and the glob-based n_nifti_outputs fix inspects the same directory as the current path.
      Revised to file-existence postcondition and PSC-intermediate exclusion filter.
    - F6: PV-1 (critical) and PV-3 (major) identified that the import guard is intentional design
      for backwards-compatible version diagnostics. Import reorganization withdrawn; recommendation
      revised to preserve guard and add narrower NameError protection.
    - F8: All 3 PV agents flagged incomplete scope (README, INPUT_SPECIFICATION, orchestrate_first_level.py
      omitted) and breaking API change for downstream consumers. Revised to full-scope schema rename
      with documentation and version migration note.

    Literature anchors consulted during the review:
    - Chen, Taylor, and Cox (2017, NeuroImage 147:952-959): PSC scaling conventions and baseline
      interpretability in AFNI.
    - AFNI documentation for 3dDeconvolve -concat and -polort: run-boundary specification for
      concatenated multi-run data.
    - PEP 440 (Python version identification): pre-release ordering semantics.
  </research_conducted>

  <findings>

    <finding id="F1" severity="major" category="validity">
      <location file="orchestrate_first_level.py" lines="95-126, 296-305" />
      <location file="orchestrator_utils.py" lines="2957-2975" />
      <description>
        Upstream FirstLevelResult.status returns "success" or "warning" (never "completed").
        The "warning" status indicates at least one high-severity graded warning (e.g., >50%
        censoring). The orchestrator's _derive_session_status (line 95) checks only
        o["status"] == "success", collapsing upstream "warning" status to the same category
        as "success" for session-level status derivation. The participant summary log (line 296)
        counts high-severity warnings but does not propagate a structured flag to the QC JSON.
        Test mocks use status="completed" (test_simulated_pipeline.py) and upstream_status="ok"
        (test_cr_implementation_extended.py), neither of which are real upstream status values.
      </description>
      <evidence>
        Upstream FirstLevelResult docstring: status is "success" or "warning". CLI maps "warning"
        to exit code 3. _derive_session_status checks o["status"] == "success" only, treating
        "warning" identically. build_first_level_qc_from_result does not inspect result.status
        for warning severity.
      </evidence>
      <literature>N/A (codebase contract verification)</literature>
      <impact>
        Downstream group-level exclusion decisions that rely on session QC status will not
        distinguish between sessions where all analyses succeeded cleanly and sessions where
        one or more analyses triggered high-severity warnings (e.g., >50% censoring). This
        could lead to inclusion of marginal-quality data without explicit researcher awareness.
      </impact>
      <recommendation>
        Recommendation A (LOCKED): Add a QC-flag channel to the per-analysis outcome dict and
        consolidated QC JSON. Specifically:
        (a) Add qc_flagged (bool) per analysis, set True when result.status == "warning".
        (b) Add n_analyses_flagged (int) and any_analysis_flagged (bool) at session level.
        (c) Keep run-status logic unchanged (do not alter _derive_session_status).
        (d) Single source of truth from result.status for the flag.
        (e) Fix test mocks to use real upstream status values ("success", "warning").
        P1 orchestrator.
      </recommendation>
    </finding>

    <finding id="F2" severity="critical" category="scientific_rigor">
      <location file="orchestrator_utils.py" lines="2139" />
      <location file="example_proc_config.yaml" lines="scale_to_psc entry" />
      <location file="upstream: task_act_first_level.py" lines="throughout" />
      <location file="upstream: first_level_utils.py" lines="scale_to_psc, build_deconvolve_base" />
      <description>
        Two interrelated scientific correctness issues in multi-run task analyses:

        1. PSC scaling: fmri_first_level_proc v3.1.0 task pipelines compute PSC by calling
           scale_to_psc on the 3dTcat-concatenated multi-run scan, using a single temporal mean
           (3dTstat -mean) computed across all runs. AFNI's canonical afni_proc.py scale block
           computes the voxelwise temporal mean separately per run and scales each run to its own
           mean of 100. A single grand mean across runs conflates inter-run intensity offsets,
           producing incorrect PSC values whose magnitude depends on the ratio of per-run means.

        2. 3dDeconvolve -concat: The upstream 3dDeconvolve call uses -polort A but does not supply
           -concat to specify run boundaries, so the polynomial baseline model spans the
           concatenation discontinuity at run junctions. AFNI documentation states: "if you input
           multiple datasets with '-input', then the separate datasets are taken to be separate
           image runs that get separate baseline models."

        The orchestrator performs the 3dTcat concatenation (orchestrator_utils.py line 2139) before
        passing the data to upstream. The uncommitted diff adds "scale_to_psc: true # (recommended)"
        in example_proc_config.yaml.
      </description>
      <evidence>
        Chen, Taylor, and Cox (2017, NeuroImage 147:952-959): PSC interpretability depends on
        correct baseline, computed per run. AFNI 3dDeconvolve documentation: -concat specifies
        run boundaries for separate baseline models. Upstream scale_to_psc computes 3dTstat -mean
        on the entire concatenated scan, then 3dcalc -expr "min(200,a/b*100)*step(a)*step(b)".
      </evidence>
      <literature>
        Chen, Taylor, Cox (2017). Is the statistic value all we should care about in neuroimaging?
        NeuroImage, 147, 952-959. Documents PSC scaling conventions in AFNI.
        AFNI 3dDeconvolve help: -concat option for run-boundary specification.
      </literature>
      <impact>
        PSC beta coefficients from multi-run task analyses are not interpretable as true percent
        signal change, and polynomial baseline models span run-junction discontinuities, potentially
        biasing both baseline estimation and task-related activation estimates. This affects all
        multi-run task_act and task_conn analyses.
      </impact>
      <recommendation>
        Dual-pathway design (LOCKED):
        Upstream (fmri_first_level_proc) adds two input modes:
        (a) scan_paths (list of per-run file paths): standalone mode. Upstream concatenates
            internally, derives run_lengths, performs per-run PSC scaling, and adds -concat
            to 3dDeconvolve. Fully self-contained for non-orchestrator users.
        (b) scan_path + run_lengths: optimization mode for pre-concatenating orchestrators.
            Upstream uses provided run_lengths for per-run PSC and -concat.
        ABCD orchestrator switches to scan_paths (passes per-run file paths instead of
        pre-concatenating). P0 both projects.

        Orchestrator-side: remove "(recommended)" label from scale_to_psc in
        example_proc_config.yaml for multi-run analyses. Add runtime WARNING when scale_to_psc
        is true and the analysis has more than one run. Add README note under Step 12.
      </recommendation>
    </finding>

    <finding id="F3" severity="major" category="robustness">
      <location file="orchestrator_utils.py" lines="3491-3600" />
      <location file="example_proc_config.yaml" lines="stat_buckets entry" />
      <location file="upstream: first_level_config.py" lines="502-540" />
      <description>
        The orchestrator's validate_proc_template (orchestrator_utils.py lines 3491-3600)
        rejects censor_path/censor_paths but does NOT reject legacy extraction keys
        (extract, extract_labels, extract_stat). These legacy keys were replaced by
        stat_buckets in upstream v3.1.0. Upstream silently ignores unrecognized keys in
        load_and_validate (first_level_config.py validation does not reject them). A user
        who copies a pre-v3.1.0 config template retaining legacy keys would get no error
        and silently lose stat extraction functionality.
      </description>
      <evidence>
        validate_proc_template checks for censor_path/censor_paths (lines ~3530-3540) but
        has no corresponding check for extract/extract_labels/extract_stat. Upstream
        first_level_config.py stat_buckets field (line 133) replaces the legacy keys.
        Upstream validation (lines 502-540) does not reject unrecognized keys.
      </evidence>
      <literature>N/A (API contract verification)</literature>
      <impact>
        Silent data loss: stat extraction would not occur, and no error or warning would
        alert the user. The orchestrator would report success while producing incomplete output.
      </impact>
      <recommendation>
        LOCKED:
        (a) Orchestrator: In validate_proc_template, add rejection of legacy extraction keys
            (extract, extract_labels, extract_stat) with an isinstance(dict) guard to avoid
            false positives on string values containing these substrings. Error message should
            direct the user to use stat_buckets instead. P0 orchestrator.
        (b) Upstream: In load_and_validate, reject unrecognized top-level keys in the analysis
            config block, logging the unrecognized key names. P1 upstream.
      </recommendation>
    </finding>

    <finding id="F4" severity="major" category="reproducibility">
      <location file="orchestrator_utils.py" lines="3168" />
      <location file="upstream: task_act_first_level.py" lines="199, 249, 410, 446, 465, 483, 608, 754" />
      <location file="upstream: first_level_config.py" lines="104" />
      <description>
        The proc-config renderer (orchestrator_utils.py line 3168) sets block["out_dir"] but
        does NOT set remove_previous. Upstream's default is remove_previous=False
        (first_level_config.py line 104). Combined with upstream's pervasive existence-caching
        (task_act_first_level.py: GLM bucket, PSC file, stat buckets, onset files all skipped
        when output exists), reprocessing a session after a config or version change silently
        reuses stale artifacts from the previous run. The force_recompute flag in
        orchestrate_first_level.py (lines 371-384) controls directory creation with exist_ok=True
        but does not propagate to the upstream remove_previous parameter.
      </description>
      <evidence>
        Upstream checkpoint.py contains a config-hash ledger (compute_config_hash,
        resolve_rerun_set) that could detect stale artifacts, but no pipeline calls it.
        The existence checks in task_act_first_level.py are unconditional file-existence
        checks with no config-hash validation.
      </evidence>
      <literature>N/A (reproducibility verification)</literature>
      <impact>
        Reprocessed sessions could contain a mixture of old and new artifacts, producing
        scientifically invalid results with no error or warning. This is particularly
        dangerous when changing analysis parameters (e.g., FD thresholds, contrast definitions)
        or upgrading fmri_first_level_proc versions.
      </impact>
      <recommendation>
        LOCKED: Set block['remove_previous'] = True unconditionally in the proc-config renderer
        (orchestrator_utils.py line 3168). This ensures upstream clears out_dir before each
        analysis run, preventing stale artifact reuse. P0 orchestrator.
      </recommendation>
    </finding>

    <finding id="F5" severity="major" category="validity">
      <location file="orchestrator_utils.py" lines="2843-2932, 2957-2975" />
      <location file="orchestrate_first_level.py" lines="897-913" />
      <description>
        The success-path QC builder (build_first_level_qc_from_result, orchestrator_utils.py
        lines 2957-2975) has three defects:

        1. Hardcoded completed_successfully=True without verifying that output files actually
           exist on disk. The fallback path (compute_first_level_qc, lines 2843-2932) performs
           this check via glob; the success path does not.

        2. n_nifti_outputs is counted from result.output_files, which includes PSC intermediates
           (_psc.nii.gz). The fallback path's glob of out_dir would count these as well, but
           the inflation is not filtered in either path, producing an output count that includes
           intermediate files alongside final outputs.

        3. The QC builder call (orchestrate_first_level.py lines 897-913) sits outside the
           per-analysis try/except block. An AttributeError or OSError in the builder would
           propagate as an unhandled exception, aborting the session's remaining analyses.
      </description>
      <evidence>
        build_first_level_qc_from_result line 2966: completed_successfully is unconditionally True.
        compute_first_level_qc line 2880-2890: checks for non-empty NIfTI outputs via glob.
        orchestrate_first_level.py: QC builder call at lines 897-913 is after the per-analysis
        try/except block (lines 856-873) closes.
        PV verification: result.status check is vacuous (always "completed" when no exception);
        glob-based fix inspects same directory as result.output_files.
      </evidence>
      <literature>N/A (code correctness verification)</literature>
      <impact>
        (1) QC JSON could report completed_successfully=True when output files are missing or
        corrupt. (2) n_nifti_outputs count includes intermediates, overstating output volume.
        (3) A single QC builder failure aborts all remaining analyses in the session.
      </impact>
      <recommendation>
        LOCKED (revised per PV concerns):
        (a) Replace hardcoded completed_successfully=True with os.path.isfile() check on the
            expected primary GLM output NIfTI (derivable from result.output_dir and analysis
            name). Add out_dir parameter to the function signature. P0 orchestrator.
        (b) Filter result.output_files to exclude known PSC intermediates (*_psc.nii.gz) when
            counting n_nifti_outputs. Apply the same exclusion in the fallback path's glob
            for consistency. P1 orchestrator.
        (c) Wrap the QC builder call in its own try/except. On failure, log the error and set
            the analysis's QC entry to a minimal dict with completed_successfully=False and
            error="QC builder failed", then continue to the next analysis. P0 orchestrator.
      </recommendation>
    </finding>

    <finding id="F6" severity="major" category="reproducibility">
      <location file="orchestrator_utils.py" lines="Section B (_parse_version)" />
      <location file="orchestrate_first_level.py" lines="37-51" />
      <location file="environment.yaml" lines="pip section" />
      <description>
        Two related issues:

        1. _parse_version (orchestrator_utils.py Section B) uses a regex-based parser that
           treats pre-release and dev builds (e.g., 3.1.0rc1, 3.1.0.dev0) as satisfying
           >= 3.1.0 comparisons, contrary to PEP 440 ordering where pre-releases sort before
           the release (3.1.0rc1 < 3.1.0).

        2. In orchestrate_first_level.py, exception class names (ConfigError, InputError,
           ModelError, etc.) imported inside the guarded try/except block (lines 42-51) would
           be unbound NameError if called from a non-main() entry point (e.g., importing
           process_participant directly) where _UPSTREAM_IMPORT_ERROR is set but main()'s
           guard check is never reached.

        The import guard itself is INTENTIONAL DESIGN: the code comment explicitly states
        "v3.1.0+ upstream modules. Guarded so that an older install reaches the explicit
        version diagnostic in main() instead of failing at import time." The unguarded imports
        at lines 37-39 (setup_logging, load_and_validate, DISPATCH) would fail identically
        if the package were absent, but the guard allows main() to produce a clear
        "requires >= 3.1.0" diagnostic for batch-scale deployments where per-subject logs
        are the only diagnostic surface.
      </description>
      <evidence>
        _parse_version regex: extracts (major, minor, patch) as integers, ignoring any
        pre-release suffix. PEP 440 ordering: 3.1.0rc1 < 3.1.0. environment.yaml pins
        git+...@v3.1.0 (exact tag) but has no explicit packaging dependency. packaging is
        importable only transitively via matplotlib's declared dependency.
        PV verification (3/3): import guard is intentional; merging would break version diagnostic.
      </evidence>
      <literature>
        PEP 440: Version Identification and Dependency Specification.
      </literature>
      <impact>
        (1) A pre-release install (3.1.0rc1) would pass the version gate, potentially exposing
        users to incomplete or unstable upstream APIs. (2) Non-main entry points could hit
        NameError on exception class names if the guarded import failed.
      </impact>
      <recommendation>
        LOCKED (revised per PV findings):
        (a) Replace _parse_version with packaging.version.Version for PEP 440 compliant
            comparison. Wrap in try/except InvalidVersion, re-raising as a clear diagnostic
            error. P0 orchestrator.
        (b) Add packaging to environment.yaml as an explicit dependency (not reliant on
            transitive availability via matplotlib). P0 orchestrator.
        (c) PRESERVE the import guard as-is. Do NOT merge guarded imports into the unguarded
            section. The guard is intentional design for backwards-compatible version
            diagnostics at batch scale. N/A.
        (d) Add a guard check at the top of process_participant (or _process_session) that
            raises a clear error if _UPSTREAM_IMPORT_ERROR is set, preventing execution from
            reaching the except blocks with unbound exception class names. This preserves the
            intentional guard while closing the NameError gap for non-main entry points.
            P1 orchestrator.
      </recommendation>
    </finding>

    <finding id="F7" severity="minor" category="reproducibility">
      <location file="orchestrator_utils.py" lines="2040, 2230" />
      <location file="README.md" lines="timing references" />
      <location file="INPUT_SPECIFICATION.md" lines="timing references" />
      <description>
        The uncommitted diff changed the timing output from _timing.csv (comma-delimited,
        CONDITION column) to _timing.tsv (tab-delimited, trial_type column) to align with
        BIDS conventions and fmri_first_level_proc v3.1.0's reader. Sessions processed before
        this change have _timing.csv files on S3; sessions processed from v4.0 onward have
        _timing.tsv files. The upstream reader (read_and_validate_stim_data) auto-detects
        delimiter by extension and accepts both BIDS and legacy column names case-insensitively,
        so both formats are currently readable. However, the heterogeneity is undocumented,
        and the upstream legacy CSV reader is deprecated and slated for removal in
        fmri_first_level_proc v4.0.0.
      </description>
      <evidence>
        format_task_timing (line 2040) and concatenate_task_timing (line 2230) write
        tab-delimited .tsv with trial_type/onset/duration columns. Upstream
        INPUT_SPECIFICATION.md: "The legacy format will be removed in v4.0.0."
        PV verification: upstream auto-detection confirmed; deprecation timeline confirmed.
      </evidence>
      <literature>
        BIDS specification: Events files use .tsv format with onset/duration/trial_type columns.
      </literature>
      <impact>
        Downstream analyses that glob for timing files by extension (_timing.csv) will miss
        post-v4.0 outputs. Pre-v4.0 _timing.csv files on S3 will become unreadable when the
        orchestrator's fmri_first_level_proc dependency is upgraded past the legacy removal point.
      </impact>
      <recommendation>
        LOCKED (revised per PV concerns):
        (a) README: Under Step 12, document the format transition (pre-v4.0: _timing.csv,
            comma-delimited, CONDITION; v4.0+: _timing.tsv, tab-delimited, trial_type).
            State that upstream read_and_validate_stim_data (<= v3.x) auto-detects both. P1.
        (b) Deprecation warning: Include explicit warning that upstream legacy CSV reader is
            deprecated and slated for removal in fmri_first_level_proc v4.0.0. Pre-v4.0
            _timing.csv files on S3 will need manual conversion before reprocessing with
            upstream >= v4.0.0. P1.
        (c) INPUT_SPECIFICATION: Update timing file format description to reflect .tsv /
            trial_type convention and note the historical .csv / CONDITION variant. P1.
        (d) Code docstrings: Add one-line note in format_task_timing and concatenate_task_timing
            stating output format is BIDS-compliant TSV with trial_type/onset/duration. P2.
        All orchestrator.
      </recommendation>
    </finding>

    <finding id="F8" severity="major" category="validity">
      <location file="orchestrator_utils.py" lines="2945, 2972, 3072" />
      <location file="orchestrate_first_level.py" lines="899-913" />
      <location file="README.md" lines="277, 285, 300, 344" />
      <location file="INPUT_SPECIFICATION.md" lines="443, 600, 612, 625, 631" />
      <description>
        The consolidated session QC JSON has a misleading key nesting. In consolidate_session_qc
        (line 3072), each analysis entry is built as "upstream_qc": outcome.get("fl_qc"). The
        value is the orchestrator's QC dict (containing sub_id, analysis_name, pct_censored,
        completed_successfully, n_nifti_outputs, and an inner key also called "upstream_qc").
        The resulting path analyses[name].upstream_qc.upstream_qc is semantically ambiguous:
        the outer key is the orchestrator's QC envelope (not upstream-authored), and the inner
        key is the actual upstream qc_summary.json content. Both README and INPUT_SPECIFICATION
        document the current schema as the canonical field path for downstream exclusion
        decisions.
      </description>
      <evidence>
        consolidate_session_qc (line 3072): "upstream_qc": outcome.get("fl_qc").
        compute_first_level_qc (line 2945): "upstream_qc": upstream_qc (the actual qc_summary).
        build_first_level_qc_from_result (line 2972): "upstream_qc": result.qc_summary.
        README line 300: analyses[name].upstream_qc.pct_censored.
        INPUT_SPECIFICATION line 625: analyses.{name}.upstream_qc.pct_censored.
        PV verification (3/3): rename resolves ambiguity; scope must include docs and all code sites.
      </evidence>
      <literature>N/A (API contract verification)</literature>
      <impact>
        The naming ambiguity creates confusion for downstream consumers about which level
        contains orchestrator-computed fields (pct_censored, n_nifti_outputs) versus raw upstream
        data. This is a breaking API change: downstream scripts parsing the current schema will
        need updates. Historical QC JSON files on S3 retain the old key names.
      </impact>
      <recommendation>
        LOCKED (revised per PV concerns, full scope):
        (a) Rename outer consolidated key: In consolidate_session_qc (line 3072), change
            "upstream_qc": outcome.get("fl_qc") to "qc": outcome.get("fl_qc"). P0 orchestrator.
        (b) Rename inner QC dict key: In both compute_first_level_qc (line 2945) and
            build_first_level_qc_from_result (line 2972), change "upstream_qc": ... to
            "upstream_qc_summary": ... P0 orchestrator.
        (c) Update orchestrate_first_level.py: Rename local variable fl_qc to qc (lines
            899-913) for naming consistency. P2 orchestrator.
        (d) Update README.md: Change all references from analyses[name].upstream_qc.* to
            analyses[name].qc.* (lines 277, 285, 300, 344). P0 orchestrator.
        (e) Update INPUT_SPECIFICATION.md: Update schema example (lines 600, 612), field path
            references (lines 443, 625, 631), and all prose mentioning upstream_qc at the
            analysis level. P0 orchestrator.
        (f) Schema version note: Document the schema change as a breaking API change in both
            README and INPUT_SPECIFICATION: QC JSON files produced by orchestrator < v4.0 use
            analyses[name].upstream_qc.upstream_qc; orchestrator >= v4.0 uses
            analyses[name].qc.upstream_qc_summary. Downstream consumers processing
            mixed-version outputs must handle both schemas. P0 orchestrator.
        (g) Update tests: Update all test assertions that reference upstream_qc or fl_qc keys
            to use the new schema paths. P1 orchestrator.
      </recommendation>
    </finding>

    <finding id="F9" severity="minor" category="robustness">
      <location file="orchestrate_first_level.py" lines="850" />
      <description>
        Pre-existing (not in diff). DISPATCH[atype] at line 850 executes before the per-analysis
        try block begins at line 856. If atype is not a key in the DISPATCH dict, the resulting
        KeyError propagates to the outer try at line 837 (or to the finally at line 916),
        aborting all remaining analyses in the session loop, not just the current one.
      </description>
      <evidence>
        Line 850: run_fn = DISPATCH[atype]. Per-analysis try block starts at line 856.
        The KeyError would bypass the per-analysis except handlers (lines 858-873).
      </evidence>
      <literature>N/A</literature>
      <impact>
        A single unrecognized analysis type would abort all remaining analyses in the session,
        even though the other analyses are independent and could proceed. At batch scale, this
        could cause cascading session failures.
      </impact>
      <recommendation>
        Move run_fn = DISPATCH[atype] inside the per-analysis try/except block (between lines
        856 and 857). The existing except Exception handler (line 869) will catch KeyError and
        allow the session loop to continue. P1 orchestrator.
      </recommendation>
    </finding>

    <finding id="F10" severity="note" category="robustness">
      <location file="orchestrate_first_level.py" lines="834-835, 916-917" />
      <description>
        Pre-existing (not in diff). The code mutates global process state with os.chdir(session_out)
        at line 834 to avoid 3dDeconvolve.err file collisions across concurrent analyses. The
        original directory is restored in a finally block at line 917. While the restore is
        correctly guarded, os.chdir is inherently process-global: if this orchestrator were
        invoked with thread-level parallelism (ThreadPoolExecutor is already used for S3 uploads
        elsewhere in the codebase), concurrent sessions would race on the working directory. The
        current design is single-session-sequential, so this is not an active bug.
      </description>
      <evidence>
        Lines 834-835: original_dir = os.getcwd(); os.chdir(session_out).
        Line 917: os.chdir(original_dir) in finally block.
        Comment at line 833: "Change to session output dir to avoid 3dDeconvolve.err collision."
      </evidence>
      <literature>N/A</literature>
      <impact>
        Latent fragility: any future introduction of thread-level session parallelism would
        create a race condition on the process working directory. The fix is upstream
        (fmri_first_level_proc controls the 3dDeconvolve invocation).
      </impact>
      <recommendation>
        Documentation-only for now:
        (a) Add a comment at line 834 noting the single-process-sequential constraint.
            P2 orchestrator.
        (b) File as a P2 upstream issue for fmri_first_level_proc to accept a cwd parameter
            or use subprocess cwd= internally. P2 upstream.
      </recommendation>
    </finding>

    <finding id="F11" severity="note" category="reproducibility">
      <location file="proc_config_final.yaml" lines="50, 59, 157" />
      <description>
        Pre-existing (not in diff). proc_config_final.yaml (local only, not published) retains
        pre-TSV-migration references: task_timing_path examples use .csv extension (lines 50, 157),
        and a comment at line 59 reads "must match CONDITION column in timing CSV." These are
        inconsistent with the codebase's migration to .tsv / trial_type columns (see F7).
      </description>
      <evidence>
        Line 50: task_timing_path: "/path/to/subj001_nback_timing.csv"
        Line 59: # -- Condition labels (must match CONDITION column in timing CSV) --
        Line 157: task_timing_path: "/path/to/subj001_nback_timing.csv"
      </evidence>
      <literature>N/A</literature>
      <impact>
        Confusion if used as a reference alongside the updated example_proc_config.yaml.
        No runtime impact (local-only file).
      </impact>
      <recommendation>
        Update the three stale references in proc_config_final.yaml to use .tsv extension
        and trial_type column nomenclature. P2 orchestrator.
      </recommendation>
    </finding>

  </findings>

  <summary>
    <critical_count>1</critical_count>
    <major_count>6</major_count>
    <minor_count>2</minor_count>
    <note_count>2</note_count>
    <overall_assessment>needs_revision</overall_assessment>

    The codebase requires revision before publication. The critical finding (F2) identifies
    scientifically incorrect PSC scaling and missing 3dDeconvolve -concat run-boundary
    specification for multi-run task analyses, which would produce uninterpretable beta
    coefficients. Six major findings address: collapsed upstream warning status in QC (F1),
    silent acceptance of legacy extraction keys (F3), stale artifact reuse across config/version
    boundaries (F4), unverified QC postconditions and unguarded builder (F5), PEP 440
    non-compliant version gating (F6), and ambiguous QC schema nesting as a breaking API
    change (F8). All findings have locked recommendations with proposal verification.
  </summary>

  <action_items>
    <!-- P0: Must fix before release -->
    <item priority="P0" target_project="fmri_first_level_proc" target_mode="implement" finding_ref="F2"
          description="Dual-pathway design: add scan_paths (list) input mode with per-run PSC scaling and 3dDeconvolve -concat. Add scan_path + run_lengths optimization mode. Covers task_act and task_conn pipelines." />
    <item priority="P0" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F2"
          description="Switch to scan_paths input mode (pass per-run file paths instead of pre-concatenating). Remove '(recommended)' from scale_to_psc in example_proc_config.yaml for multi-run. Add runtime WARNING for scale_to_psc + multi-run. Add README note under Step 12." />
    <item priority="P0" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F3"
          description="In validate_proc_template, reject legacy extraction keys (extract, extract_labels, extract_stat) with isinstance(dict) guard. Error message directs user to stat_buckets." />
    <item priority="P0" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F4"
          description="Set block['remove_previous'] = True unconditionally in proc-config renderer." />
    <item priority="P0" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F5"
          description="Replace hardcoded completed_successfully=True with os.path.isfile() check on primary GLM output NIfTI. Add out_dir parameter to build_first_level_qc_from_result signature." />
    <item priority="P0" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F5"
          description="Wrap QC builder call in try/except. On failure, log error and set QC entry to minimal dict with completed_successfully=False and error='QC builder failed'." />
    <item priority="P0" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F6"
          description="Replace _parse_version with packaging.version.Version. Wrap in try/except InvalidVersion. Add packaging to environment.yaml as explicit dependency." />
    <item priority="P0" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F8"
          description="Rename QC schema: outer 'upstream_qc' to 'qc' in consolidate_session_qc; inner 'upstream_qc' to 'upstream_qc_summary' in both QC builder functions." />
    <item priority="P0" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F8"
          description="Update README.md and INPUT_SPECIFICATION.md: all QC schema references from upstream_qc.* to qc.*. Add schema version migration note for pre-v4.0 vs v4.0+ outputs." />

    <!-- P1: Should fix before release -->
    <item priority="P1" target_project="fmri_first_level_proc" target_mode="implement" finding_ref="F3"
          description="In load_and_validate, reject unrecognized top-level keys in analysis config blocks with logged key names." />
    <item priority="P1" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F1"
          description="Add QC-flag channel: qc_flagged (bool) per analysis, n_analyses_flagged and any_analysis_flagged at session level. Fix test mocks to use real upstream status values." />
    <item priority="P1" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F5"
          description="Filter result.output_files to exclude *_psc.nii.gz intermediates when counting n_nifti_outputs. Apply same exclusion in fallback path glob." />
    <item priority="P1" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F6"
          description="Add guard check at top of process_participant (or _process_session): raise clear error if _UPSTREAM_IMPORT_ERROR is set, preventing NameError on unbound exception class names." />
    <item priority="P1" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F7"
          description="README and INPUT_SPECIFICATION: document timing format transition (pre-v4.0 .csv vs v4.0+ .tsv). Include deprecation warning re: upstream legacy CSV reader removal in v4.0.0." />
    <item priority="P1" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F8"
          description="Update all test assertions referencing upstream_qc or fl_qc keys to use new schema paths (qc, upstream_qc_summary)." />
    <item priority="P1" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F9"
          description="Move run_fn = DISPATCH[atype] inside per-analysis try/except block (from line 850 to after line 856)." />

    <!-- P2: Nice to fix -->
    <item priority="P2" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F7"
          description="Add one-line docstring note in format_task_timing and concatenate_task_timing: output is BIDS-compliant TSV with trial_type/onset/duration." />
    <item priority="P2" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F8"
          description="Rename local variable fl_qc to qc in orchestrate_first_level.py (lines 899-913) for naming consistency." />
    <item priority="P2" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F10"
          description="Add comment at os.chdir (line 834) noting single-process-sequential constraint." />
    <item priority="P2" target_project="fmri_first_level_proc" target_mode="implement" finding_ref="F10"
          description="Upstream issue: accept cwd parameter or use subprocess cwd= for 3dDeconvolve invocation to eliminate os.chdir side effect." />
    <item priority="P2" target_project="ABCD_fmri_orchestrator_S3" target_mode="implement" finding_ref="F11"
          description="Update proc_config_final.yaml: change .csv to .tsv in task_timing_path examples and 'CONDITION column' to 'trial_type column' in comments." />
  </action_items>

</cr_report>
