<test_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="test" timestamp="2026-05-14T17:11:24Z" />

  <scope>
    Design submodule only. Scope locked by user to the two functions
    produced by the 2026-05-14 implement build covering legacy detection
    and migration:

      - orchestrator_utils.determine_session_routing
      - orchestrator_utils.migrate_session_from_archive

    No pre-design or post-design run_suite phases were executed per the
    project's paused-indefinitely directive (2026-05-04) on direct pytest
    re-runs. Test correctness is therefore unverified by execution; the
    deliverable of this design pass is the test file itself plus the
    case inventory below.
  </scope>

  <pre_design_run>
    <total>not_run</total>
    <passed>not_run</passed>
    <failed>not_run</failed>
    <errors>not_run</errors>
    <coverage_pct>not_run</coverage_pct>
    <failures />
    <notes>Direct pytest re-run paused indefinitely by user direction on 2026-05-04. No baseline failure ledger was constructed for the targeted functions because no failures could be observed without a suite run. The two targeted functions are net-new additions from the 2026-05-14 build and have no prior tests to disposition.</notes>
  </pre_design_run>

  <failing_test_dispositions>
    <notes>No failing tests to disposition. The targeted functions are net-new additions from the 2026-05-14 implement build (see ABCD_fmri_orchestrator_S3_implement_build_20260514_163512.md, changes C4 and C6). No prior assertion exists for either function; therefore the dispositions framework does not apply on this pass.</notes>
  </failing_test_dispositions>

  <design_phase>
    <tests_created>14</tests_created>
    <tests_modified>0</tests_modified>
    <files_created>
      <file path="tests/test_legacy_routing_migration.py" test_count="14" coverage_target="determine_session_routing (6 cases) and migrate_session_from_archive (8 cases) — all branches of the routing decision tree plus all failure-mode classes of the migration procedure." />
    </files_created>
    <design_rationale>
      Cases derived from the docstrings + plan-spec contracts of the two
      target functions in orchestrator_utils.py. The S3 surface is stubbed
      via the existing convention used in tests/test_motion_and_qc.py:
      patch orchestrator_utils._get_s3_client to return a MagicMock and
      configure head_object / download_file / delete_object via
      return_value or side_effect. Real tarballs are constructed in
      tmp_test_dir for migration tests, and download_file is stubbed to
      copy the prebuilt tarball into the function's local_archive_path.

      Coverage:

      determine_session_routing (6 cases):
        - SKIP: sentinel present, force_recompute=False. Asserts
          routing="skip" and remote_metadata.sentinel_last_modified is
          the ISO8601 of the head_object LastModified.
        - FULL via force_recompute: sentinel present, force_recompute=True.
          Asserts delete_object called once on the sentinel key and
          routing="full" with empty remote_metadata; the tarball probe is
          never attempted.
        - MIGRATE: sentinel HEAD 404, tarball HEAD succeeds. Asserts
          routing="migrate" with ETag, size, and last_modified populated
          verbatim from the tarball head response.
        - FULL no-artifacts: both heads 404/NoSuchKey. Asserts
          routing="full" with empty remote_metadata and both probe keys
          appear in head_object.call_args_list.
        - OrchestratorError on non-404 tarball probe (e.g. 500). Asserts
          the wrapped OrchestratorError mentions the tarball key.
        - ClientError on non-404 sentinel probe (e.g. 403). Asserts the
          ClientError propagates with code 403 and that the tarball
          probe is never attempted.

      migrate_session_from_archive (8 cases):
        - Happy path: well-formed tarball with all four arcnames and a
          QC JSON. Asserts canonical subdir population, provenance block
          (status, ETag, ISO8601-UTC timestamp, version), upload_session_to_s3
          delegation, legacy tarball delete, and staging-dir cleanup.
        - Corrupt tar (TarError): downloaded bytes are not a gzipped
          tarball. Asserts LegacyArchiveCorruptError; upload and delete
          NOT called.
        - Path-traversal-only members: tarball entries all use parent-
          traversal names so the realpath-startswith guard filters all
          of them. Asserts LegacyArchiveCorruptError("no safe members");
          upload and delete NOT called.
        - Missing required arcname: tarball lacks the
          '{prefix}_first_level_out' directory. Asserts
          LegacyArchiveCorruptError with "missing required arcname";
          upload and delete NOT called.
        - Missing QC JSON: qc/ present but no orchestrator_qc.json inside.
          Asserts WARNING logged identifying the missing-QC condition;
          no QC file created; upload AND delete still proceed.
        - Upload failure: upload_session_to_s3 raises OrchestratorError.
          Asserts OrchestratorError propagated; delete_object on the
          legacy tarball NOT called (preserves source for retry).
        - Non-fatal delete failure: upload succeeds, delete_object on the
          legacy tarball raises a ClientError. Asserts the function
          returns the upload's success dict and logs an ERROR-level
          message identifying the failed deletion with the tarball key;
          NO re-raise.
        - Partial subdir mapping: tarball contains only the two required
          arcnames (no _preproc, no _concat). Asserts required subdirs
          populated; optional subdirs created by the prepare step but
          empty; upload and delete still occur.

      Test Design Discipline (per /test skill doctrine):
        - Every assertion encodes the docstring + plan-spec contract of
          the function. No tautologies (e.g. no `assert isinstance(x, type(x))`,
          no `assert True`).
        - No unconditional skip/xfail; no try/except wrappers suppressing
          assertions.
        - Warning-path tests (missing-QC, non-fatal-delete-failure) make
          positive assertions on the captured log stream (StringIO via
          the mock_logger fixture's .string_stream attribute), not
          tautological no-raise checks.
        - Test docstrings explicitly state the contract under test and
          the rationale for each assertion, so a future reader can audit
          the postcondition without re-deriving it.
    </design_rationale>
  </design_phase>

  <post_design_run>
    <total>not_run</total>
    <passed>not_run</passed>
    <failed>not_run</failed>
    <errors>not_run</errors>
    <coverage_pct>not_run</coverage_pct>
    <failures />
    <notes>
      Post-design execution paused indefinitely per the 2026-05-04 user
      directive. The /test design submodule produced the test file
      tests/test_legacy_routing_migration.py with 14 cases as enumerated
      above; correctness of those test cases will be verified the next
      time the user lifts the pause and authorizes a direct pytest run
      under direct observation.

      Known dependency for execution:
        - pytest is on the project's conda env (ABCD_fmri_orchestrator_S3).
        - boto3 / botocore.exceptions.ClientError are imported via the
          existing requirements; the existing test_motion_and_qc.py uses
          the same import.
        - No new pytest plugins, mark registrations, or conftest entries
          are required; the new test file relies entirely on the
          existing mock_logger and tmp_test_dir fixtures from
          tests/conftest.py.
    </notes>
  </post_design_run>

  <summary>
    <assertions_preserved_or_strengthened>n/a</assertions_preserved_or_strengthened>
    <bugs_routed_to_implement>0</bugs_routed_to_implement>
    <recommendation>proceed_to_document</recommendation>
    <notes>
      The assertions_preserved_or_strengthened metric is n/a on this pass
      because no prior tests existed for either targeted function; there
      were no assertions to preserve or strengthen. All 14 new tests
      encode contracts at-or-stronger than the docstring + plan spec.

      No product bugs were identified during this design pass. The
      function contracts as written in the implement-build are internally
      consistent and testable; the 14 test cases exercise every branch
      of the routing decision tree and every documented failure mode of
      the migration procedure without revealing a gap in the
      implementation contract.

      Recommendation is proceed_to_document for the cycle-wide doc
      refresh deferred by the implement plan's scope notes (README and
      INPUT_SPECIFICATION harmonization beyond the targeted edits, and
      AID_LOG sync). A /cr review of the routing decision tree was also
      identified as a downstream action by the implement next_steps
      block; it is not strictly blocked by this test design but is
      recommended before /publish.
    </notes>
  </summary>

  <action_items>
    <item priority="P1" target_mode="test" description="When the user lifts the 2026-05-04 paused-pytest directive, run pytest tests/test_legacy_routing_migration.py under direct observation to verify all 14 cases pass. Capture any failures via the standard run_suite path." />
    <item priority="P2" target_mode="document" description="Sync AID_LOG.md and the .aid/ reports for the 2026-05-14 implement build (changes C1-C16) and this test design report, per the cycle-wide doc refresh deferred from the implement plan's scope notes." />
    <item priority="P2" target_mode="cr" description="Code-review the routing decision tree in determine_session_routing for adversarial S3 states (e.g. partial first_level_out tarball + stale sentinel; concurrent upload races), per the implement build's next_steps recommendation." />
  </action_items>
</test_report>
