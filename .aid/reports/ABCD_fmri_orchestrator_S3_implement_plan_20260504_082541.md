<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-05-04T12:25:41Z" />

  <input_reports>
    <report path="<local_path>/brainstorm_history/ABCD_fmri_orchestrator_S3_brainstorm_20260502_093415.md" mode="brainstorm" key_items="6" />
  </input_reports>

  <skill_scope_partition>
    This plan is consumed by two skills, each restricted to its own change set. The /implement build submodule must execute ONLY the implement-scope changes; the /test design submodule must execute ONLY the test-scope changes.

    IMPLEMENT-SCOPE (codebase + user-facing docs; consumed by /implement build):
      C1  proc_config_final.yaml          (S1 rest_conn field + S6 CLI comment fix)
      C2  example_proc_config.yaml        (S1 rest_conn field with explanatory comment)
      C3  INPUT_SPECIFICATION.md          (S2 passthrough doc + S3-expanded version pin bumps at L430, L544)
      C4  README.md                       (S3 + S3-expanded version pin bumps at L67, L197, L345)
      C5  AID_LOG.md                      (S4 v2.5.0 alignment entry append)

    TEST-SCOPE (test fixtures, golden files, new test method; consumed by /test design):
      C6  tests/conftest.py               (rest_conn fixture additions at L143, L454)
      C7  tests/golden_config_baseline.yaml   (rest_conn field addition; coupled to C6)
      C8  tests/golden_config_refactored.yaml (rest_conn field addition; coupled to C6)
      C9  tests/test_coverage_gaps.py     (inline rest_conn dict update at L1262 + new passthrough test method)
      C10 tests/test_preprocessing.py     (rest_conn fixture additions at L113, L571)

    Cross-scope dependency note: C6/C7/C8 are structurally coupled (the test_preprocessing.py golden-file equality assertion requires conftest fixtures and golden YAMLs to move together). All three live in TEST-SCOPE, so the coupling is internal to /test design and does not cross the skill boundary. There are no IMPLEMENT-SCOPE -> TEST-SCOPE runtime dependencies: build_first_level_config's deep-copy passthrough behavior is unchanged by C1-C5, so the test additions in C6-C10 do not require C1-C5 to land first to be valid.
  </skill_scope_partition>

  <scope_resolutions>
    Three plan-discipline gates resolved before producing the spec below:

    (1) S7 (conda environment install of fmri-first-level-proc v2.5.0 + corresponding MEMORY.md Config State update) is target_mode="run-local" in the brainstorm action items and is excluded from this implement plan. It runs separately under /run-local with explicit per-invocation user approval per CLAUDE.md Technical Preferences. The MEMORY.md update is bundled with the install (single atomic operation) so MEMORY.md never reflects an inaccurate state.

    (2) S2 placement: the brainstorm references "INPUT_SPECIFICATION.md L85 verbatim-passed field list", but L84-85 is the deprecated-fields list under section 2.3 (orchestrator analyses block), and the formal verbatim-passthrough surface is at L177-186 under section 3 ("Proc Template Config" subsection "Everything Else"). use_sequenced_bandpass is added to the L177-186 list (formal proc-template-passthrough surface) only; it is NOT added to L85 (which is for fields historically misplaced in the orchestrator config; use_sequenced_bandpass is brand new and was never in any orchestrator config, so deprecated-list inclusion is semantically wrong). User-confirmed.

    (3) S3 scope expanded by user directive: in addition to the brainstorm-specified version pin update at README.md L67, all "fmri_first_level_proc >= 2.4.0" references in the orchestrator's user-facing documentation are bumped to ">= 2.5.0", including the contract-introduction anchors at README.md L197, L345 and INPUT_SPECIFICATION.md L430, plus the example QC JSON snippet at INPUT_SPECIFICATION.md L544 ("fmri_first_level_proc_version": "2.4.0" -> "2.5.0"). The historical AID_LOG.md L104 entry for the 2026-04-03 v2.4.0 alignment cycle is preserved as an immutable changelog record; the new v2.5.0 work gets a new dated entry appended below it (S4 / C5).
  </scope_resolutions>

  <changes>

    <!-- ========================================================================
         IMPLEMENT-SCOPE: C1-C5 (consumed by /implement build)
         Codebase + user-facing documentation only. NOT to be executed by /test.
         ======================================================================== -->

    <change id="C1" priority="P0" source_item="S1, S6">
      <file path="proc_config_final.yaml" action="modify" />
      <description>Add use_sequenced_bandpass: false to the production rest_conn block (S1) and replace stale "python run_first_level.py" usage comments with the entry-point form "run-first-level" (S6). Single-file edit pass.</description>
      <spec>
        Edit 1 (S6, header CLI comments at L6-L9):
          Replace lines 6-8:
            "#   python run_first_level.py --config example_config.yaml --dry-run"
            "#   python run_first_level.py --config example_config.yaml"
            "#   python run_first_level.py --config example_config.yaml --analyses 0 2"
          With:
            "#   run-first-level --config example_config.yaml --dry-run"
            "#   run-first-level --config example_config.yaml"
            "#   run-first-level --config example_config.yaml --analyses 0 2"
          Line 9 (the "--analyses 0 2 =>" comment) is unchanged. The replacement matches the entry-point form already adopted in example_proc_config.yaml during the v2.4.0 alignment cycle.

        Edit 2 (S1, rest_conn block field addition):
          In the rest_conn analysis block (block starts at L238 with "name: \"rest_conn\""), inside the "-- Block settings --" subsection (L264-272 area), insert a new line immediately AFTER:
            "    keep_run_res_dtseries: true  # true = keep all run-specific dtseries (false to save space)"
          The new line, with two-space block-level indentation matching surrounding fields:
            "    use_sequenced_bandpass: false # true = use Ciric-inspired sequenced denoising (NTRP interpolation + decoupled BOLD/nuisance bandpass); false = simultaneous denoising (preserves v2.4.0/N=30 behavior)"
          Default value is false to preserve scientific behavioral parity with the published N=30 cohort.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - additive YAML field at established placement; comment-form replacement parallels the v2.4.0 precedent on example_proc_config.yaml. Field default preserves existing behavior.</risk>
      <rollback>git checkout HEAD -- proc_config_final.yaml</rollback>
    </change>

    <change id="C2" priority="P0" source_item="S1">
      <file path="example_proc_config.yaml" action="modify" />
      <description>Add use_sequenced_bandpass: false to the example rest_conn block with an explanatory comment documenting the Ciric-inspired path opt-in.</description>
      <spec>
        In the rest_conn analysis block (block starts at L153 with "name: \"rest_connectivity\""), inside the "-- Block settings --" subsection (L179-186 area), insert a new line immediately AFTER:
          "    keep_run_res_dtseries: true  # true = keep all run-specific dtseries (false to save space)"
        The new line, with two-space block-level indentation matching surrounding fields:
          "    use_sequenced_bandpass: false # true = use Ciric-inspired sequenced denoising (NTRP interpolation + decoupled BOLD/nuisance bandpass) for DOF-constrained cohorts; false = simultaneous denoising (default)"
      </spec>
      <dependencies>none</dependencies>
      <risk>low - additive YAML field at established placement.</risk>
      <rollback>git checkout HEAD -- example_proc_config.yaml</rollback>
    </change>

    <change id="C3" priority="P0" source_item="S2, S3-expanded">
      <file path="INPUT_SPECIFICATION.md" action="modify" />
      <description>Add use_sequenced_bandpass to the verbatim-passthrough list in section 3 ("Everything Else") with a brief note about the optional sequenced rest_conn backend and its retention coupling to keep_run_res_dtseries. Bump version pins at L430 (input contract anchor) and L544 (example QC JSON provenance).</description>
      <spec>
        Edit 1 (S2, verbatim-passthrough list at L177-186):
          Currently:
            "All other fields in the proc template are passed through verbatim to the generated first-level config. This includes:
            - HRF model settings
            - Contrast definitions
            - Bandpass filter parameters
            - Motion derivative degree
            - Atlas/template paths
            - Extraction parameters (ROI definitions, average type, etc.)
            - Connectivity parameters (methods, thresholds, etc.)"
          Append a new bullet at the end of the existing list:
            "- Resting-state denoising backend toggle (`use_sequenced_bandpass`)"
          Append a new paragraph immediately after the bulleted list (between L186 and the existing "### Cross-Validation Rules" subsection at L188):
            "**Note:** As of `fmri_first_level_proc` >= 2.5.0, the rest_conn block accepts an opt-in `use_sequenced_bandpass` boolean (default `false`). When `true`, upstream uses a Ciric-inspired six-step sequenced denoising pipeline (NTRP interpolation + decoupled BOLD and nuisance bandpass) and may write per-run intermediates to `_sequenced_intermediates/{run_label}/` inside the rest_conn output directory. Retention of these intermediates is coupled to the existing `keep_run_res_dtseries` flag: if `true`, intermediates are retained and bundled into the orchestrator's session archive; if `false`, upstream deletes them after each run and they never reach archival. The orchestrator does not gate or filter intermediates separately. ABCD production configs default `use_sequenced_bandpass: false` to preserve behavioral parity with v2.4.0 outputs."

        Edit 2 (S3-expanded, contract-anchor version bump at L430):
          Replace the substring ">= 2.4.0" with ">= 2.5.0" on the line currently reading:
            "per the `fmri_first_level_proc` >= 2.4.0 input contract, which expects degrees and"
          Result:
            "per the `fmri_first_level_proc` >= 2.5.0 input contract, which expects degrees and"

        Edit 3 (S3-expanded, example QC JSON version bump at L544):
          Replace the line currently reading:
            "    \"fmri_first_level_proc_version\": \"2.4.0\","
          With:
            "    \"fmri_first_level_proc_version\": \"2.5.0\","
      </spec>
      <dependencies>none</dependencies>
      <risk>low - additive bullet/paragraph in a descriptive section; literal version-string substitutions on two well-anchored lines.</risk>
      <rollback>git checkout HEAD -- INPUT_SPECIFICATION.md</rollback>
    </change>

    <change id="C4" priority="P0" source_item="S3, S3-expanded">
      <file path="README.md" action="modify" />
      <description>Bump three "fmri_first_level_proc >= 2.4.0" references to ">= 2.5.0": the install requirement at L67 (S3 explicit) and the two motion-contract-introduction anchors at L197 and L345 (S3-expanded).</description>
      <spec>
        Edit 1 (L67, install requirement):
          Replace the substring "&gt;= 2.4.0" with "&gt;= 2.5.0" on the line currently reading:
            "The conda environment installs `fmri_first_level_proc` directly from GitHub via pip. **Requires `fmri_first_level_proc` >= 2.4.0.** See `environment.yaml` for the full dependency list."
          Result:
            "The conda environment installs `fmri_first_level_proc` directly from GitHub via pip. **Requires `fmri_first_level_proc` >= 2.5.0.** See `environment.yaml` for the full dependency list."

        Edit 2 (L197, motion-contract anchor):
          Replace ">= 2.4.0" with ">= 2.5.0" on the line currently reading:
            "Extracts the 6 base motion parameters ... per the `fmri_first_level_proc` >= 2.4.0 input contract; no unit conversion is applied. ..."

        Edit 3 (L345, motion-contract anchor in Design Decisions):
          Replace ">= 2.4.0" with ">= 2.5.0" on the line currently reading:
            "All motion parameters, framewise displacement (FD), and motion derivatives are sourced from raw motion.tsv files (mmps_mproc) ... per the `fmri_first_level_proc` >= 2.4.0 input contract; FD computation and radian conversion are handled exclusively by `fmri_first_level_proc`. ..."

        All three edits are pure version-string substitutions; surrounding prose is unchanged.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - three literal substitutions on well-anchored lines.</risk>
      <rollback>git checkout HEAD -- README.md</rollback>
    </change>

    <change id="C5" priority="P0" source_item="S4">
      <file path="AID_LOG.md" action="modify" />
      <description>Append a new dated entry to the Version History section (Section 8) describing the v2.5.0 alignment work. Frame the LLM strictly as a tool: no contributor or collaborator language. Preserve the existing 2026-04-03 entry as immutable historical record.</description>
      <spec>
        Append a new bullet immediately after the existing 2026-04-03 entry (L104), with one blank line between the two entries:

          "- **2026-05-04**: Orchestrator aligned with `fmri_first_level_proc` >= 2.5.0. Documentation and configuration-template updates reflect the new opt-in sequenced denoising backend (`use_sequenced_bandpass`) for resting-state connectivity and the corrected DOF pre-flight regressor count. No orchestrator code changes were required: the new proc-template field is preserved verbatim via the deep-copy passthrough in `build_first_level_config`. ABCD production configs default `use_sequenced_bandpass: false` to preserve behavioral parity with the v2.4.0 N=30 cohort outputs. Test fixtures and golden config files updated; one new passthrough unit test added. Pre-publish LLM-attribution scrub gate enforced per project memory."

        No other AID_LOG.md content is modified. The Section 8 header ("## 8. Version History") and the L104 entry remain untouched.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - pure append at a stable section; phrasing audited for tool-only framing (no co-author/contributor language).</risk>
      <rollback>git checkout HEAD -- AID_LOG.md</rollback>
    </change>

    <!-- ========================================================================
         TEST-SCOPE: C6-C10 (consumed by /test design)
         Test fixtures, golden config files, and one new passthrough test method.
         NOT to be executed by /implement build.
         ======================================================================== -->

    <change id="C6" priority="P0" source_item="S5">
      <file path="tests/conftest.py" action="modify" />
      <description>Add use_sequenced_bandpass: False to the two rest_conn proc-template fixtures at L143 and L454. Both insertions are within a Python dict literal in the rest_conn analysis block.</description>
      <spec>
        Edit 1 (L143 area, within the first rest_conn fixture dict):
          Insert a new line immediately AFTER the existing line:
            "                \"keep_run_res_dtseries\": True,"
          The new line, with matching 16-space indentation:
            "                \"use_sequenced_bandpass\": False,"

        Edit 2 (L454 area, within the second rest_conn fixture dict):
          Insert a new line immediately AFTER the existing line:
            "                \"keep_run_res_dtseries\": True,"
          The new line, with matching 16-space indentation:
            "                \"use_sequenced_bandpass\": False,"

        Both insertions follow the exact same pattern; the line precedes "use_tissue_derivs": False in both fixtures.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - additive dict key in a Python literal; matching pattern at two locations with identical surrounding context.</risk>
      <rollback>git checkout HEAD -- tests/conftest.py</rollback>
    </change>

    <change id="C7" priority="P0" source_item="S5">
      <file path="tests/golden_config_baseline.yaml" action="modify" />
      <description>Add use_sequenced_bandpass: false to the rest_conn block of the golden baseline config so it matches the new conftest.py fixture content. The golden file is alphabetically sorted (yaml.dump with sort_keys=True per test_preprocessing.py); the new field belongs alphabetically before use_tissue_derivs (s &lt; t) and after type (since the key prefix is "use_").</description>
      <spec>
        In the rest_conn analysis block (the block beginning with "average_type: mean" at L33 and ending with "use_tissue_derivs: false" at L69), insert a new line immediately BEFORE the existing line:
          "  use_tissue_derivs: false"
        The new line, with matching two-space indentation:
          "  use_sequenced_bandpass: false"

        Alphabetical ordering (sort_keys=True) places use_sequenced_bandpass before use_tissue_derivs because "use_s..." sorts before "use_t...".
      </spec>
      <dependencies>C6 (the conftest.py fixture must be updated together with the golden file or the deterministic-output assertion at test_preprocessing.py::TestGoldenFileBaseline will fail)</dependencies>
      <risk>low - YAML field insertion at an alphabetically determined position. The golden-file invariant is that yaml.dump(build_first_level_config(...), sort_keys=True) equals the file content; both halves of the equality must update together (covered by C6 + C7).</risk>
      <rollback>git checkout HEAD -- tests/golden_config_baseline.yaml</rollback>
    </change>

    <change id="C8" priority="P0" source_item="S5">
      <file path="tests/golden_config_refactored.yaml" action="modify" />
      <description>Add use_sequenced_bandpass: false to the rest_conn block of the golden refactored config (parallel to C7).</description>
      <spec>
        In the rest_conn analysis block (the block beginning with "average_type: mean" at L66 and ending with "use_tissue_derivs: false" at L101), insert a new line immediately BEFORE the existing line:
          "  use_tissue_derivs: false"
        The new line, with matching two-space indentation:
          "  use_sequenced_bandpass: false"
      </spec>
      <dependencies>C6 (same coupling as C7: conftest fixture and golden file must be consistent for the deterministic-output assertion at test_preprocessing.py::TestGoldenFileRefactored to pass)</dependencies>
      <risk>low - same shape as C7.</risk>
      <rollback>git checkout HEAD -- tests/golden_config_refactored.yaml</rollback>
    </change>

    <change id="C9" priority="P0" source_item="S5">
      <file path="tests/test_coverage_gaps.py" action="modify" />
      <description>Two edits in the same file: (a) add use_sequenced_bandpass: False to the inline rest_conn proc-template dict at L1262 inside test_task_conn_paths_populated; (b) add a new passthrough unit test method to TestBuildFirstLevelConfigAdditional that asserts build_first_level_config preserves use_sequenced_bandpass verbatim from the proc template.</description>
      <spec>
        Edit 1 (L1262, fixture update):
          Insert a new key immediately AFTER the existing line:
            "                    \"keep_run_res_dtseries\": True, \"use_tissue_derivs\": False,"
          OR, equivalently, modify the existing line in place to include the new key. Concrete approach: change the line at L1262 from:
            "                    \"keep_run_res_dtseries\": True, \"use_tissue_derivs\": False,"
          To:
            "                    \"keep_run_res_dtseries\": True, \"use_sequenced_bandpass\": False, \"use_tissue_derivs\": False,"
          (single-line in-place insertion to match the existing inline-dict style at this site).

        Edit 2 (new test method appended to TestBuildFirstLevelConfigAdditional class):
          Append a new test method to the class TestBuildFirstLevelConfigAdditional (which begins at L1208). The method should be inserted as a new method within that class body, after the last existing method but before the next top-level class/function definition.

          Method signature and intent:
            def test_use_sequenced_bandpass_preserved_in_passthrough(
                self, sample_orchestrator_config, mock_logger
            ):
                """build_first_level_config preserves use_sequenced_bandpass verbatim from proc template."""

          Test body (intent, not verbatim):
            1. Construct an orchestrator config restricted to a single rest_conn analysis (mirror the rest_conn analysis-block pattern used by test_task_conn_paths_populated at L1216-1224, but for type "rest_conn" with task_label "rest", post_id_out_pre "rest", post_id_extract_pre "rest_Shen368", post_id_conn_pre "rest_Shen368"; do NOT reuse the sample_orchestrator_config nback_act analysis since it has no rest_conn entry).
            2. Construct a proc template containing exactly one rest_conn analysis block matching the L1254-1266 pattern, but with the additional key "use_sequenced_bandpass": True. Include the surrounding required keys (paths with rest scan_paths/motion_paths/CSF_paths/WM_paths/GS_paths=None, out_dir, out_file_pre, remove_previous, average_type, bandpass, motion_deriv_degree, keep_run_res_dtseries, use_tissue_derivs, extraction.extract_ptseries+extract_out_file_pre, connectivity.calc_conn+conn_out_file_pre+pcorr+fishZ).
            3. Construct the processed_files dict with a "rest" entry containing bolds, motions, csf, wm, gs=None.
            4. Call build_first_level_config("TEST001", "00", orch["study"], orch["tasks"], processed_files, orch_analyses, template, mock_logger).
            5. Locate the rest_conn block in the returned config (use the next(b for b in ... pattern from L1291).
            6. Assert rest_block["use_sequenced_bandpass"] is True with a clear failure message: "use_sequenced_bandpass should be preserved verbatim from the proc template (forward direction)".
            7. Re-run the same flow with template["analyses"][0]["use_sequenced_bandpass"] = False (ensuring the orchestrator passes False through unchanged), and assert rest_block["use_sequenced_bandpass"] is False with message: "use_sequenced_bandpass should be preserved verbatim from the proc template (inverse direction)".

          Both directions (True passes through; False passes through) verify the field is not silently defaulted, masked, or overridden by the orchestrator. This matches the brainstorm A1 verification claim that build_first_level_config preserves use_sequenced_bandpass via deep copy with no field-level handling.
      </spec>
      <dependencies>none for the fixture edit; none for the new test (the test does not depend on conftest fixtures beyond the existing sample_orchestrator_config and mock_logger, both already defined in conftest.py).</dependencies>
      <risk>low - the fixture edit is a single-line key insertion. The new test follows the established pattern in TestBuildFirstLevelConfigAdditional and exercises a passthrough invariant that is structurally guaranteed by build_first_level_config's deep-copy semantics; risk of false negatives is low.</risk>
      <rollback>git checkout HEAD -- tests/test_coverage_gaps.py</rollback>
    </change>

    <change id="C10" priority="P0" source_item="S5">
      <file path="tests/test_preprocessing.py" action="modify" />
      <description>Add use_sequenced_bandpass: False to the two rest_conn proc-template fixtures at L113 and L571.</description>
      <spec>
        Edit 1 (L113 area, within the first rest_conn fixture dict):
          Insert a new line immediately AFTER the existing line:
            "                \"keep_run_res_dtseries\": True,"
          The new line, with matching 16-space indentation:
            "                \"use_sequenced_bandpass\": False,"

        Edit 2 (L571 area, within the second rest_conn fixture dict):
          Insert a new line immediately AFTER the existing line:
            "                \"keep_run_res_dtseries\": True,"
          The new line, with matching 16-space indentation:
            "                \"use_sequenced_bandpass\": False,"

        Both insertions parallel C6's pattern.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - additive dict key at two well-anchored locations with identical surrounding context.</risk>
      <rollback>git checkout HEAD -- tests/test_preprocessing.py</rollback>
    </change>

  </changes>

  <execution_order>
    The plan is partitioned by skill scope. /implement build executes C1-C5 only; /test design executes C6-C10 only. Within each scope, all changes touch distinct files, so they are partitionable for parallel execution under the agent-dispatch contract (one agent per file, non-overlapping edits).

    IMPLEMENT-SCOPE conceptual ordering (executed under /implement build):
      1. C1, C2 (proc config templates): establish the new field default.
      2. C3, C4 (user-facing docs): bring INPUT_SPECIFICATION.md and README.md into alignment with v2.5.0.
      3. C5 (AID_LOG.md): append v2.5.0 alignment entry.

    TEST-SCOPE conceptual ordering (executed under /test design):
      4. C6, C10 (Python test fixtures), C7, C8 (golden YAMLs), C9 (one fixture edit + new passthrough test method): bring the test surface into alignment.
    Note that C7 and C8 are coupled to C6 by the deterministic-golden-file invariant (both halves of the equality must move together), but the dependency is structural (test will fail on a partial application), not edit-ordering.

    Cross-skill ordering: IMPLEMENT-SCOPE and TEST-SCOPE are independent and may run in either order. build_first_level_config's deep-copy passthrough behavior is unchanged by C1-C5, so the test fixture additions in C6-C10 remain valid regardless of whether C1-C5 has landed.
  </execution_order>

</implement_plan>
