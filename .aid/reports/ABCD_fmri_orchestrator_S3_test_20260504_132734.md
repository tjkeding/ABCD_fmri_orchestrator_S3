<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" submodule="design" timestamp="2026-05-04T13:27:34Z" />

  <input_spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20260504_082541.md</input_spec_ref>

  <scope_note>
    User invoked the design submodule explicitly with scope restricted to the test-scope changes
    of the shared implement plan. The pre-design and post-design run_suite passes were not executed
    because the user did not request the full sequence; the suite will be executed under a separate
    /test run_suite invocation. All write operations were confined to the tests/ directory per the
    skill's write-access constraint.
  </scope_note>

  <design_phase>
    <tests_created>1</tests_created>
    <tests_modified>5</tests_modified>
    <files_modified>
      <file path="tests/conftest.py" change_count="2" coverage_target="adds the new rest_conn proc-template field to both rest_conn fixtures so any consumer that builds a first-level config from these fixtures sees the v2.5.0 field set to its production-default value." />
      <file path="tests/golden_config_baseline.yaml" change_count="1" coverage_target="keeps the deterministic golden-file equality (yaml.dump with sort_keys=True equals the file content) consistent with the updated conftest fixture." />
      <file path="tests/golden_config_refactored.yaml" change_count="1" coverage_target="parallel update to the refactored-path golden file so the second golden equality assertion in the preprocessing test module remains satisfied." />
      <file path="tests/test_coverage_gaps.py" change_count="2" coverage_target="updates the inline rest_conn proc-template dict inside the existing task_conn-paths-populated test so the build_first_level_config call inside that test continues to operate on a fixture that includes the new field; adds one new explicit passthrough test method to the build-first-level-config additional-edge-cases class." />
      <file path="tests/test_preprocessing.py" change_count="2" coverage_target="adds the new field to both rest_conn fixtures used by the preprocessing test module so the upstream-passthrough assertions and any downstream assertions that iterate the rest_conn block see a structurally complete v2.5.0 fixture." />
    </files_modified>

    <new_test_methods>
      <method
        class="TestBuildFirstLevelConfigAdditional"
        file="tests/test_coverage_gaps.py"
        name="test_use_sequenced_bandpass_preserved_in_passthrough">
        Asserts that the new v2.5.0 proc-template-only boolean is preserved verbatim by the
        deep-copy passthrough in build_first_level_config. The test exercises both directions
        (True passes through; False passes through) using the existing sample_orchestrator_config
        fixture restricted to its rest_conn analysis entry, plus a single-analysis proc template
        with the new field set to each value. The bidirectional design catches a future change
        that hard-codes the field to either constant. Failure messages explicitly cite the
        forward and inverse direction so a regression bisects cleanly.
      </method>
    </new_test_methods>

    <design_rationale>
      The v2.5.0 alignment introduces a single new proc-template-only boolean, with no orchestrator
      Python code change. The orchestrator's build_first_level_config function deep-copies the proc
      template and overrides only subject-specific fields; the new field must therefore flow through
      unchanged. Two classes of test surface need to follow:

      First, every existing fixture that constructs a rest_conn proc-template block must be updated
      so the structurally complete proc-template input matches the v2.5.0 contract. Five such
      fixtures exist across three modules (two in conftest, two in test_preprocessing, one inline
      in test_coverage_gaps). All five have been updated to include the new field set to the
      ABCD-production default of False. This preserves byte-level parity for any test that compares
      a built config against a golden YAML, because the golden YAMLs (alphabetically sorted by
      yaml.dump with sort_keys=True) have been updated in the same pass.

      Second, the v2.5.0 alignment cycle needs an explicit invariant assertion that the orchestrator
      passes the new field through unchanged. Without such an assertion, a future refactor that
      accidentally hard-codes or drops the field would pass all existing tests (because the existing
      fixtures all use the same value, False, so they would not detect a constant). The new
      bidirectional passthrough test exercises both True and False against the build pipeline and
      asserts the field's value matches the input, catching any future field-level interception.

      Risk profile of the additions: low. All five file modifications are pure additive insertions
      of a single dict-key or YAML-key entry, with no rewrites of surrounding logic. The new test
      method follows the established pattern of the surrounding TestBuildFirstLevelConfigAdditional
      class, reuses existing conftest fixtures, and runs in milliseconds because build_first_level_config
      is a pure-Python function with no I/O.
    </design_rationale>
  </design_phase>

  <summary>
    <design_complete>true</design_complete>
    <recommendation>proceed_to_run_suite</recommendation>
    <coupling_note>
      The conftest fixture additions and the two golden-YAML additions are structurally coupled
      under the deterministic-output equality assertions in the preprocessing test module. A
      partial application (fixture updated but golden not, or vice versa) would surface as a
      failing equality assertion. All halves were applied together in this design pass, so the
      coupling is internally satisfied.
    </coupling_note>
  </summary>

  <action_items>
    <item priority="P1" target_mode="test" description="Run the full pytest suite via /test run_suite to validate that the 274-test baseline still passes and that the new passthrough test method registers and passes; this is the post-design run that the design-only submodule deferred." />
    <item priority="P1" target_mode="run-local" description="Install fmri-first-level-proc v2.5.0 (editable) in the project conda environment and update MEMORY.md Config State atomically per the project memory's open action item; this is required before the N=1 smoke regression and the two N=30 DOF-failed re-runs can run under the new upstream version." />
  </action_items>
</test_report>
