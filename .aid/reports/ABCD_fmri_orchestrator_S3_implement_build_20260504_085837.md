<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build" timestamp="2026-05-04T12:58:37Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20260504_082541.md</spec_ref>
  <scope_note>
    This build executed only the implement-scope changes (the first five entries) of the tech spec.
    The remaining five entries are test-scope changes deferred to /test design per the
    skill_scope_partition section of the tech spec. The user will invoke /test against the same
    plan file with explicit scoping to those entries.
  </scope_note>
  <changes_applied>
    <change id="C1" status="done" user_decision="n/a">
      <files_modified>
        <file path="proc_config_final.yaml" lines_changed="4" />
      </files_modified>
      <notes>Two-part change applied to the production proc-config template. First, the stale CLI invocation comment (header comment block at lines 6-8) was rewritten from the legacy "python run_first_level.py --config example_config.yaml [...]" form to the current console-script form "run-first-level --config example_config.yaml [...]", aligning with the v2.4.0 packaging that exposes a console_scripts entry point. Second, a single new field was inserted into the rest_conn parameter block immediately after the keep_run_res_dtseries entry, declaring use_sequenced_bandpass with a default value of false and an inline comment explaining the toggle (true selects the v2.5.0 Ciric-inspired sequenced denoising backend with NTRP interpolation and decoupled BOLD/nuisance bandpass; false preserves the simultaneous-denoising behavior used in the v2.4.0 N=30 cohort). No other content in the file was modified; surrounding YAML keys and indentation match the existing rest_conn block conventions.</notes>
    </change>
    <change id="C2" status="done" user_decision="n/a">
      <files_modified>
        <file path="example_proc_config.yaml" lines_changed="1" />
      </files_modified>
      <notes>The new rest_conn denoising-backend field was added to the example proc-config template at the location immediately following the keep_run_res_dtseries entry, mirroring the production template insertion. The inline comment was tailored to the example-template audience: it documents the v2.5.0 sequenced denoising backend as the opt-in branch suited to DOF-constrained cohorts, and notes the simultaneous-denoising default. No other lines in the file were touched.</notes>
    </change>
    <change id="C3" status="done" user_decision="n/a">
      <files_modified>
        <file path="INPUT_SPECIFICATION.md" lines_changed="4" />
      </files_modified>
      <notes>Four edits applied to the orchestrator input specification. First, a new bullet was appended to the proc-template passthrough field list (the "Everything Else" section) to surface the rest_conn denoising-backend toggle on the verbatim-passed surface, consistent with the project decision to keep this field on the proc-template surface rather than the orchestrator-config surface. Second, a new Note paragraph was inserted directly below the bullet list, documenting the v2.5.0 sequenced denoising backend semantics, the retention coupling with keep_run_res_dtseries, and the explicit ABCD-config default of false to preserve N=30 cohort behavioral parity. Third, the upstream version anchor in the Rotation Unit Handling section was bumped from the prior minor revision to v2.5.0. Fourth, the example QC JSON provenance string in the QC-record example was bumped to "2.5.0" to match the install-anchor sweep. The intentional textual reference to the v2.4.0 cohort outputs inside the new Note paragraph is preserved as historical provenance and is not a stale version reference.</notes>
    </change>
    <change id="C4" status="done" user_decision="n/a">
      <files_modified>
        <file path="README.md" lines_changed="3" />
      </files_modified>
      <notes>All three upstream-dependency version anchors in the project README were bumped to the new minor revision: the install-requirement line in the dependency block, the motion-contract anchor where the contract introduction is cited, and the design-decisions section's motion-contract anchor. A whole-file grep verified zero remaining occurrences of the prior minor-revision substring after the edits, confirming the complete sweep. No other content was modified.</notes>
    </change>
    <change id="C5" status="done" user_decision="n/a">
      <files_modified>
        <file path="AID_LOG.md" lines_changed="2" />
      </files_modified>
      <notes>A single new dated bullet was appended below the existing 2026-04-03 v2.4.0 entry, documenting the v2.5.0 alignment scope: the rationale (alignment with upstream's opt-in sequenced denoising backend and corrected DOF pre-flight regressor count), the engineering observation that no orchestrator code change was required because the new proc-template field flows through the existing deep-copy passthrough in build_first_level_config, the ABCD production default of false for behavioral parity with the prior cohort outputs, the test-fixture and golden-config update plan, and the planned enforcement of the pre-publish LLM-attribution scrub gate. The prior 2026-04-03 entry was preserved byte-identical as an immutable historical record per the AID-framework convention. The new entry was authored under strict tool-only framing per the project's AID disclosure rules.</notes>
    </change>
  </changes_applied>
  <summary>
    <total_changes>5</total_changes>
    <completed>5</completed>
    <skipped>0</skipped>
    <blocked>0</blocked>
  </summary>
  <verification_status>
    Post-build verification was performed via parallel non-mutating reads against each modified file.
    The new rest_conn field is present at the expected location in both proc-config templates with
    the documented default value. The orchestrator input specification surfaces the new toggle on
    the verbatim-passed field list and carries the v2.5.0 Note paragraph, with the version-anchor
    sweep complete. The README install/motion anchors all reflect the new minor revision with no
    residual stale substrings. The AID log retains its prior immutable entry and carries the new
    dated entry. No collateral edits were observed in any file.
  </verification_status>
  <next_steps>
    Recommended sequence:
    1. Invoke /test against the same plan file scoped to the test-scope changes (the remaining five
       entries) to update the rest_conn fixtures across conftest.py, the two golden config baselines,
       the coverage-gaps test module (including the new passthrough unit test), and the preprocessing
       test module.
    2. Once /test design and /test run_suite are complete and the suite is green, invoke /run-local
       to install the v2.5.0 upstream package (editable) into the project conda environment and to
       update the project memory's Config State entry atomically.
    3. Run the deferred N=1 smoke regression and the two N=30 DOF-failed re-runs under /run-local
       per the open action items in the project memory.
    4. Reserve /publish for after all validation is complete, with strict enforcement of the
       LLM-attribution scrub gate per the project's incident-anchored publish policy.
  </next_steps>
</implement_report>
