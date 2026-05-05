<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-05-02T13:34:15Z" />

  <scope>
    Identify all orchestrator changes required for compatibility with
    fmri-first-level-proc v2.4.0 to v2.5.0. Two upstream changes between
    e2bca93 (v2.4.0, 2026-04-03) and cb2b8f8 (v2.5.0, 2026-05-01):
    (U1) DOF pre-flight regressor-count fix carrying over from this
    orchestrator's prior upstream issue transfer; and (U2) the addition of
    an opt-in sequenced denoising path for rest_conn (Ciric-inspired,
    NTRP-interpolation + decoupled BOLD/nuisance bandpass).
  </scope>

  <context_files>
    <file path="orchestrator_utils.py" relevance="build_first_level_config (L2437-2571) verifies verbatim passthrough; compress_session_outputs (L2973-3062) handles archive packaging" />
    <file path="orchestrate_first_level.py" relevance="upstream dispatch via DISPATCH and load_and_validate (L686-712); no field-level handling" />
    <file path="example_proc_config.yaml" relevance="rest_conn block target for use_sequenced_bandpass field addition (L185 area)" />
    <file path="proc_config_final.yaml" relevance="ABCD production rest_conn block target for use_sequenced_bandpass field addition; pre-existing stale CLI comment at L7" />
    <file path="INPUT_SPECIFICATION.md" relevance="verbatim-passed field list (L85) for documentation update" />
    <file path="README.md" relevance="upstream version requirement and verbatim-passthrough description (L337)" />
    <file path="AID_LOG.md" relevance="AID framework disclosure log requires v2.5.0 entry" />
    <file path="tests/conftest.py" relevance="rest_conn config fixtures (L143, L454) require new field" />
    <file path="tests/golden_config_baseline.yaml" relevance="rest_conn fixture (L45) requires new field" />
    <file path="tests/golden_config_refactored.yaml" relevance="rest_conn fixture (L80) requires new field" />
    <file path="tests/test_coverage_gaps.py" relevance="rest_conn config dict (L1262) requires new field" />
    <file path="tests/test_preprocessing.py" relevance="rest_conn fixtures (L113, L571) require new field" />
    <file path=".aid/reports/ABCD_fmri_orchestrator_S3_brainstorm_20260403_v240_alignment.md" relevance="v2.4.0 alignment precedent: established the verbatim-passthrough pattern and config-template-only strategy for rest_conn schema additions" />
  </context_files>

  <topics>

    <topic id="T1" title="Upstream change inventory (v2.4.0 to v2.5.0)">
      <summary>
        Two upstream commits separate the tags. (U1) The DOF pre-flight
        regressor-count fix corrects an over-conservative count that
        previously assumed polort 2 after v2.3.1 had switched to polort -1.
        (U2) v2.5.0 adds an opt-in sequenced denoising path for rest_conn:
        a new boolean config field use_sequenced_bandpass (default false),
        a new CLI flag --use_sequenced_bandpass, two new utility functions
        (censor_interpolate_1d_afni, bandpass_filter_1d_afni), and a refactor
        of gen_residual_ts that dispatches to either
        _generate_run_residual_simultaneous (current behavior) or
        _generate_run_residual_sequenced (six-step Ciric-inspired pipeline).
        The sequenced path produces a per-run intermediate directory
        _sequenced_intermediates/{run_label}/ inside the rest_conn output
        directory, retained or deleted per the existing keep_run_res_dtseries
        flag. Final residual location and naming are unchanged regardless of
        backend.
      </summary>
      <decision status="decided">
        Inventory accepted as-is. No upstream behavior is unexpected or
        mid-flight. The orchestrator must align config templates and
        documentation with the new optional field; no orchestrator code
        changes are required for the field to function.
      </decision>
    </topic>

    <topic id="T2" title="Orchestrator contract surface: verbatim passthrough confirmation">
      <summary>
        Hypothesis under test: the orchestrator passes the proc_config to
        upstream verbatim, and the only behavioral surface affected by
        sequenced-bandpass adoption is output packaging.
      </summary>
      <approaches>
        <approach id="A1" label="Verify against build_first_level_config">
          <description>
            build_first_level_config (orchestrator_utils.py L2437-2571)
            deep-copies the proc template and overrides only subject-specific
            fields: global.tr (validates or injects against study.TR),
            block out_dir, block out_file_pre, block fd_threshold,
            block censor_prev_tr, block paths (entirely replaced based on
            analysis type), block extraction.extract_out_file_pre, and
            block connectivity.conn_out_file_pre. Every other analysis-level
            field, including bandpass, notch_filter_band, motion_deriv_degree,
            keep_run_res_dtseries, use_tissue_derivs, and use_sequenced_bandpass,
            is preserved verbatim.
          </description>
          <pros>Empirically verified passthrough; matches the v2.4.0 alignment precedent.</pros>
          <cons>None.</cons>
        </approach>
        <approach id="A2" label="Verify upstream dispatch">
          <description>
            orchestrate_first_level.py L698 calls
            fmri_first_level_proc.run_first_level.load_and_validate to parse
            the written proc-config YAML; L708 dispatches via DISPATCH[atype]
            to upstream. Field validation (BOOL_KEYS membership, defaulting)
            is performed by upstream's build_namespace, which v2.5.0 already
            extends to recognize use_sequenced_bandpass.
          </description>
          <pros>No orchestrator-side parsing or allowlist gate; field is consumed entirely upstream.</pros>
          <cons>None.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1+A2">
        Hypothesis confirmed. No Python code changes are required in the
        orchestrator for use_sequenced_bandpass to function end-to-end.
      </decision>
    </topic>

    <topic id="T3" title="Output packaging policy when sequenced denoising is active">
      <summary>
        compress_session_outputs (orchestrator_utils.py L2973-3062) recursively
        adds first_level_out/ via tar.add. When use_sequenced_bandpass is
        true and keep_run_res_dtseries is true, upstream retains
        _sequenced_intermediates/{run_label}/ per run; the recursive add
        archives those files. When keep_run_res_dtseries is false, upstream
        deletes the intermediates after each run, so they never reach
        archival. The retention gate is therefore enforced upstream.
      </summary>
      <approaches>
        <approach id="O1" label="Rely on upstream gate (status quo)" feasibility="high" risk="low">
          <description>
            Make no change to compress_session_outputs. Archive contents are
            controlled entirely by upstream's keep_run_res_dtseries semantic.
            If the user retains intermediates upstream, they are also
            archived; if upstream deletes them, archives stay slim.
          </description>
          <pros>Zero code change. Single, consistent semantic for output retention. No risk of orchestrator-side filter drifting from upstream cleanup.</pros>
          <cons>Intermediates can substantially inflate archive size when both flags are true (per-run BOLD NIfTI on the order of 100s of MB plus filtered 1D regressors). User must manage size by toggling the upstream flag.</cons>
        </approach>
        <approach id="O2" label="Orchestrator-side exclusion of _sequenced_intermediates/" feasibility="high" risk="low">
          <description>
            Add an explicit exclusion to compress_session_outputs that skips
            any path matching _sequenced_intermediates/ during recursive add.
            Final residuals and extract_raw_ptseries outputs would still be
            archived; intermediates would always be local-only.
          </description>
          <pros>Bounds archive size predictably regardless of upstream flag setting.</pros>
          <cons>Splits the retention semantic across orchestrator and upstream; user expectations may diverge from observed archive contents. Adds tested code path with no current scientific need.</cons>
        </approach>
        <approach id="O3" label="Configurable archive-side exclusion" feasibility="med" risk="low">
          <description>
            New orchestrator config field (e.g., archive_sequenced_intermediates)
            that defaults to true and can be toggled per study.
          </description>
          <pros>Maximum flexibility.</pros>
          <cons>Adds surface area for a use case that is not yet validated; introduces a third decision (alongside use_sequenced_bandpass and keep_run_res_dtseries) for an opt-in feature that is not used in current production.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="O1">
        Rely on upstream's keep_run_res_dtseries gate. No orchestrator code
        change. If intermediates are retained upstream, they are bundled
        with the rest of the outputs; if not, they are not. This preserves
        a single retention semantic and aligns with the v2.4.0 alignment
        principle of minimal orchestrator-side intervention in upstream
        contract surfaces.
      </decision>
    </topic>

    <topic id="T4" title="Default value of use_sequenced_bandpass for ABCD configs">
      <summary>
        The default for use_sequenced_bandpass in proc_config_final.yaml
        becomes the de facto study default for any subsequent re-run of
        ABCD rest_conn.
      </summary>
      <approaches>
        <approach id="A1" label="false (preserve v2.4.0 / N=30 behavior)" feasibility="high" risk="low">
          <description>
            Use the simultaneous denoising backend
            (_generate_run_residual_simultaneous) for all production runs.
            Scientific outputs identical to the published N=30 cohort.
          </description>
          <pros>No silent scientific drift across the v2.4.0 to v2.5.0 transition. Existing N=30 archives remain comparable to any subsequent re-run. No archive bloat. Opt-in switch to sequenced backend remains available per study or per re-test.</pros>
          <cons>Does not opportunistically adopt the DOF-preserving sequenced path; does not address the 2 of 133 rest_conn DOF failures via the new path.</cons>
        </approach>
        <approach id="A2" label="true (adopt Ciric-inspired path as default)" feasibility="high" risk="med">
          <description>
            Use the sequenced denoising backend
            (_generate_run_residual_sequenced) for all production runs.
            Reclaims DOF lost to bandpass-implied regressors.
          </description>
          <pros>May rescue DOF-marginal sessions. Aligns with Ciric et al. 2017 best-practice recommendations for nuisance regression in rest-state.</pros>
          <cons>Scientific outputs differ from the published N=30 cohort. Per-run intermediates added to archives when keep_run_res_dtseries is true (current setting). Adopting it without further validation risks an undocumented denoising-backend change in subsequent results.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">
        Default to false in both proc_config_final.yaml and
        example_proc_config.yaml. The example file documents the option
        with a comment noting that true enables the Ciric-inspired path for
        DOF-constrained cohorts.
      </decision>
    </topic>

    <topic id="T5" title="Documentation and config-template updates">
      <summary>
        Doc/config-template touches required to reflect the v2.5.0 contract,
        plus one pre-existing stale comment surfaced for explicit scope
        decision.
      </summary>
      <decision status="decided">
        Implement all of S1 through S7 (no items deferred). Specifics:

        S1. Add use_sequenced_bandpass: false to the rest_conn block in both
        proc_config_final.yaml and example_proc_config.yaml, with a comment
        documenting that true enables the Ciric-inspired path.

        S2. Update INPUT_SPECIFICATION.md L85 verbatim-passed field list to
        include use_sequenced_bandpass; add a brief note about the optional
        sequenced rest_conn backend and its retention coupling to
        keep_run_res_dtseries.

        S3. Update README.md upstream version requirement from "&gt;= 2.4.0"
        to "&gt;= 2.5.0".

        S4. Append a v2.5.0 alignment entry to AID_LOG.md following the
        AID-framework template, framing the LLM strictly as a tool.

        S5. Update test fixtures (tests/conftest.py L143 and L454,
        tests/golden_config_baseline.yaml L45,
        tests/golden_config_refactored.yaml L80,
        tests/test_coverage_gaps.py L1262,
        tests/test_preprocessing.py L113 and L571) to include
        use_sequenced_bandpass: false (or the Python-dict equivalent) in
        every rest_conn schema fixture. Add one new passthrough unit test
        asserting build_first_level_config preserves use_sequenced_bandpass
        in the deep-copied config without modification.

        S6. Pre-existing v2.4.0 leftover: replace the stale
        "python run_first_level.py --config example_config.yaml" usage
        comments at proc_config_final.yaml L6-L9 with the current
        "run-first-level --config ..." entry-point form (the same
        replacement was applied to example_proc_config.yaml during the
        v2.4.0 alignment).

        S7. Pre-flight: verify fmri-first-level-proc v2.5.0 is installed
        (editable) in the ABCD_fmri_orchestrator_S3 conda environment before
        any test execution. Update Config State in MEMORY.md to reflect
        the version bump.
      </decision>
    </topic>

    <topic id="T6" title="Testing strategy">
      <summary>
        Because the orchestrator change is documentation/config/fixture-only,
        the test surface partitions cleanly between unit/integration tests
        (which must always run after fixture updates) and optional
        real-world verification (regression guard plus opportunistic
        re-evaluation of DOF-failed sessions under the corrected pre-flight).
      </summary>
      <approaches>
        <approach id="T-A" label="Unit/integration suite (274 tests) after fixture updates" feasibility="high" risk="low">
          <description>
            Run the full pytest suite locally after S5 fixture updates to
            verify all schema-aware tests still pass and the new passthrough
            unit test for use_sequenced_bandpass functions correctly.
          </description>
          <pros>Mandatory regression guard for fixture-touching changes; minutes-scale runtime.</pros>
          <cons>None.</cons>
        </approach>
        <approach id="T-B" label="N=1 smoke test under v2.5.0 with use_sequenced_bandpass: false" feasibility="high" risk="low">
          <description>
            Run one previously-passing real-world participant
            end-to-end under v2.5.0 with sequenced bandpass disabled. Verify
            outputs match the v2.4.0 reference by spot-checking residual
            statistics, censor totals, and connectivity matrices.
          </description>
          <pros>Regression guard against any silent behavioral drift in upstream's preserved _generate_run_residual_simultaneous() path or in the DOF pre-flight fix; sandbox-only, ~45 minutes.</pros>
          <cons>Modest sandbox time cost.</cons>
          <statistical_considerations>The simultaneous-path code is preserved verbatim in v2.5.0; behavioral drift would only arise via the DOF pre-flight fix (which could change which runs are accepted vs. rejected). Comparing accepted-run lists between v2.4.0 and v2.5.0 outputs for a single participant is the most informative single check.</statistical_considerations>
        </approach>
        <approach id="T-C" label="Re-run of the 2 N=30 rest_conn DOF failures under v2.5.0" feasibility="high" risk="low">
          <description>
            Re-execute the rest_conn analysis for the 2 sessions that failed
            DOF pre-flight in the N=30 run, with use_sequenced_bandpass: false.
            The corrected DOF pre-flight may reclassify these as passing.
            Updates the N=30 success rate from 131 of 133 toward potentially
            133 of 133.
          </description>
          <pros>Affordable scientific gain; clarifies whether the failures were genuine DOF-insufficiency or pre-flight false rejections.</pros>
          <cons>Requires re-uploading the affected session archives to S3 if outputs change.</cons>
          <statistical_considerations>If the failed sessions pass under v2.5.0, the prior failure was a pre-flight artifact, not a true DOF insufficiency. If they still fail, the DOF insufficiency is real and the failures are scientifically valid (consistent with bandpass regressors and aggressive censoring eating remaining DOF).</statistical_considerations>
        </approach>
        <approach id="T-D" label="Full N=30 re-run" feasibility="med" risk="low">
          <description>
            Re-execute all 30 subjects under v2.5.0 with sequenced bandpass
            disabled.
          </description>
          <pros>Maximum verification of behavioral parity.</pros>
          <cons>Sequenced bandpass is disabled by default, so the simultaneous path is unchanged; the only mechanism for behavioral drift is the DOF pre-flight fix, which is captured cheaply by T-C plus T-B. T-D adds hours of sandbox + S3 cost for negligible scientific gain.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="T-A,T-B,T-C">
        Run T-A (mandatory) + T-B (regression guard on the simultaneous path)
        + T-C (opportunistic re-evaluation of the 2 DOF failures). Skip T-D.
      </decision>
    </topic>

    <topic id="T7" title="Pre-publish verification gate (incident-anchored)">
      <summary>
        A prior cycle on this project leaked an LLM-attribution marker into
        committed content, after which GitHub auto-added "Claude" as a
        spurious collaborator on the public repository. Removal required
        direct GitHub support intervention. This v2.5.0 cycle must avoid
        any recurrence with 1000% confidence (per user's framing).
      </summary>
      <decision status="decided">
        Before any commit and before any push, run an exhaustive scrub of:
        (a) the full git diff and the staged tree,
        (b) every tracked file in the repository (not just changed files),
        (c) the commit message bodies of the most recent N commits on the
        branch (since prior commits would propagate on push),
        for the marker classes documented in
        memory/feedback_publish_llm_attribution.md (identity strings:
        claude, anthropic, chatgpt, openai, gpt-4, copilot, gemini, llama,
        case-insensitive; commit-trailer attribution patterns of any form
        following a non-human identifier; LLM-tool and competitor-tool
        mention-handle patterns recognized by GitHub's commit-attribution
        heuristic; authorship phrasings: "written by", "refactored by",
        "added by" in conjunction with any AI identifier; "AI-assisted",
        "AI-generated", "AI-written", "LLM-generated", "language-model-generated";
        process-tooling leakage: .claude/, claude-config, claude_session
        path fragments). The AID framework (.aid/ directory and AID_LOG.md)
        is the only sanctioned LLM-tool-use disclosure surface; even there,
        no contributor or collaborator framing is permitted. A non-empty
        match in any class is a hard halt, with the (file, line, pattern)
        tuples surfaced to the user; no auto-remediation is permitted.
        This gate is enforced regardless of whether /publish is invoked or
        a direct git commit/push is issued from the main conversation.
      </decision>
    </topic>

  </topics>

  <action_items>
    <item priority="P0" target_mode="implement" description="S1: Add use_sequenced_bandpass: false to rest_conn block in proc_config_final.yaml and example_proc_config.yaml, with explanatory comment about the Ciric-inspired path." />
    <item priority="P0" target_mode="implement" description="S2: Update INPUT_SPECIFICATION.md L85 verbatim-passed field list to include use_sequenced_bandpass; add a note describing the optional sequenced rest_conn backend and its retention coupling to keep_run_res_dtseries." />
    <item priority="P0" target_mode="implement" description="S3: Update README.md upstream version requirement from '&gt;= 2.4.0' to '&gt;= 2.5.0'." />
    <item priority="P0" target_mode="implement" description="S4: Append a v2.5.0 alignment entry to AID_LOG.md, framing the LLM strictly as a tool (no contributor/collaborator language)." />
    <item priority="P0" target_mode="implement" description="S5: Update tests/conftest.py (L143, L454), tests/golden_config_baseline.yaml (L45), tests/golden_config_refactored.yaml (L80), tests/test_coverage_gaps.py (L1262), tests/test_preprocessing.py (L113, L571) to include use_sequenced_bandpass: false in every rest_conn schema fixture; add one passthrough unit test asserting build_first_level_config preserves use_sequenced_bandpass unmodified." />
    <item priority="P1" target_mode="implement" description="S6: Replace stale 'python run_first_level.py' usage comments at proc_config_final.yaml L6-L9 with the current 'run-first-level --config ...' form (parity with example_proc_config.yaml v2.4.0 update)." />
    <item priority="P0" target_mode="run-local" description="S7 / pre-flight: install fmri-first-level-proc v2.5.0 (editable) in the ABCD_fmri_orchestrator_S3 conda environment; update MEMORY.md Config State to reflect version bump." />
    <item priority="P0" target_mode="test" description="T-A: Run full pytest suite (274 tests) after S5 fixture updates; verify all pass and the new passthrough unit test functions correctly." />
    <item priority="P1" target_mode="run-local" description="T-B: N=1 smoke test under v2.5.0 with use_sequenced_bandpass: false; spot-check residual statistics, censor totals, accepted-run lists, and connectivity matrices vs. v2.4.0 reference outputs." />
    <item priority="P1" target_mode="run-local" description="T-C: Re-run the 2 N=30 rest_conn DOF-failed sessions under v2.5.0 with use_sequenced_bandpass: false; report whether the corrected DOF pre-flight reclassifies them as passing." />
    <item priority="P0" target_mode="publish" description="T7 pre-publish scrub: exhaustively scan staged tree, all tracked files, and recent commit messages for LLM-attribution markers (identity strings, attribution trailers, GitHub-mention triggers, authorship phrasings, process-tooling leakage) per memory/feedback_publish_llm_attribution.md. Hard halt on any match; no auto-remediation." />
  </action_items>

  <next_steps>
    Recommended downstream sequence: (1) /implement plan to translate the
    P0 and P1 action items into a concrete tech spec; (2) /implement build
    to apply the changes; (3) /test run_suite for T-A; (4) /run-local for
    T-B and T-C; (5) /document for the AID_LOG entry and any incidental
    documentation polish surfaced during implementation; (6) /publish with
    the T7 pre-publish scrub enforced as the irreversible gate. Skip T-D
    (full N=30 re-run) entirely.
  </next_steps>

</brainstorm_report>
