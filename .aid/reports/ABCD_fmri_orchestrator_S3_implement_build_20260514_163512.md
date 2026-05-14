<implement_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="build" timestamp="2026-05-14T16:35:12Z" />
  <spec_ref>ABCD_fmri_orchestrator_S3_implement_plan_20260514_120440.md</spec_ref>
  <changes_applied>
    <change id="C1" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~70" />
      </files_modified>
      <notes>Added `enumerate_upload_targets(session_out_dir, sub_id, session)` as the single source of truth for the per-session expected key set; used by both the upload gap computation and the verification step.</notes>
    </change>
    <change id="C2" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~40" />
      </files_modified>
      <notes>Added `_session_upload_prefix`, `check_session_complete`, and `delete_session_sentinel` helpers; centralize S3 key construction for the per-session prefix and the zero-byte sentinel object.</notes>
    </change>
    <change id="C3" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~6" />
      </files_modified>
      <notes>Added `LegacyArchiveCorruptError` exception class as the fall-through signal from the migration step to the routing-aware caller when a downloaded legacy archive fails extraction or sanity checks.</notes>
    </change>
    <change id="C4" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~75" />
      </files_modified>
      <notes>Added `determine_session_routing` returning `"skip" | "migrate" | "full"` via `head_object` probes against the sentinel key and the legacy tarball key. Pure metadata lookups; downloads no payload. Integrity verification is deferred to the migration step.</notes>
    </change>
    <change id="C5" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~160" />
      </files_modified>
      <notes>Added `upload_session_to_s3` implementing the per-file upload contract: enumerate-and-diff against the remote inventory via `list_objects_v2`, parallel uploads with `ThreadPoolExecutor` sized by `s3.upload_max_workers`, batched size/ETag verification, and atomic write of the zero-byte `_COMPLETE` sentinel only after verification passes.</notes>
    </change>
    <change id="C6" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~160" />
      </files_modified>
      <notes>Added `migrate_session_from_archive` covering the full legacy-rehosting procedure: download the legacy tarball, extract to a staging directory, map archived arcnames back to canonical local subdirectories, insert the migration provenance block into the staged session orchestrator QC JSON, delegate to the per-file upload routine, and delete the legacy tarball from S3 on success. Migration provenance key insertion is implemented inline as part of this routine.</notes>
    </change>
    <change id="C7" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~12" />
      </files_modified>
      <notes>Extended `load_orchestrator_config` to default and validate the new `s3.upload_max_workers` field (int, default 8, range [1, 64], boolean rejected).</notes>
    </change>
    <change id="C8" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~-60" />
      </files_modified>
      <notes>Deleted the legacy single-archive upload function entirely; all callers were migrated to the per-file routine added above. Section header in the utils module also renamed to reflect the surviving cleanup-only role.</notes>
    </change>
    <change id="C9" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrator_utils.py" lines_changed="~-90" />
      </files_modified>
      <notes>Deleted the session-archive compression function entirely; no caller remains after the orchestrator refactor.</notes>
    </change>
    <change id="C10" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="~24" />
      </files_modified>
      <notes>Refactored `_derive_session_status` to the new signature `(routing, analysis_outcomes)`. The full-routing path retains the existing success/partial/failed mapping; the skip and migrate routings emit two new statuses respectively.</notes>
    </change>
    <change id="C11" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="~140" />
      </files_modified>
      <notes>Refactored `_process_session` for routing-aware dispatch with the structured-dict return contract. Inserted the routing pre-flight at session entry; the skip and migrate paths return early after a banner log; the migrate path catches the legacy-archive corruption signal and falls through to the full path; the full path replaces the prior compress-and-upload sequence with a direct call to the per-file upload routine. Captures the QC JSON path. Final return is the four-key dict (routing, analysis_outcomes, qc_path, remote_metadata). Docstring updated.</notes>
    </change>
    <change id="C12" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="~60" />
      </files_modified>
      <notes>Updated `process_participant` to consume the structured-dict return from the per-session routine; propagates routing and remote_metadata into `session_results`; extends both exception branches to include routing="full" plus the new dict-shape fields; extends the per-session summary counter loop with skipped and migrated banner lines; updates the all-failed-raises guard to also exclude the new statuses.</notes>
    </change>
    <change id="C13" status="done" user_decision="n/a">
      <files_modified>
        <file path="orchestrate_first_level.py" lines_changed="~6" />
      </files_modified>
      <notes>Removed the legacy upload and compression symbols from the orchestrator's import block; added the four new public symbols (LegacyArchiveCorruptError, determine_session_routing, upload_session_to_s3, migrate_session_from_archive).</notes>
    </change>
    <change id="C14" status="done" user_decision="n/a">
      <files_modified>
        <file path="example_orchestrator_config.yaml" lines_changed="~12" />
      </files_modified>
      <notes>Added a header comment block above the s3 section describing the per-file mirror layout, the sentinel object, and the auto-migration behavior. Added the new tuning field next to existing s3 tuning fields with a two-line comment noting default, valid range, and boto3 client thread-safety.</notes>
    </change>
    <change id="C15" status="done" user_decision="n/a">
      <files_modified>
        <file path="README.md" lines_changed="~40" />
      </files_modified>
      <notes>Replaced single-archive prose in the upload step and output-directory sections with the per-file mirror layout, sentinel, and idempotent resume semantics. Added a Legacy Archive Auto-Migration subsection under the S3 Data Structure section documenting auto-detection, byte-equivalent rehosting, post-migration tarball deletion, and the migration provenance block in the QC JSON. Added the new config field to the s3 fields list. Remaining references to the legacy tarball name are now explicitly annotated as legacy behavior. Source-pattern table, architecture diagram step label, and Development Notes section index were brought into consistency with these contract-level edits.</notes>
    </change>
    <change id="C16" status="done" user_decision="n/a">
      <files_modified>
        <file path="INPUT_SPECIFICATION.md" lines_changed="~12" />
      </files_modified>
      <notes>Added the new tuning field to the s3 field table with type, default, range, and description matching the plan wording, plus a corresponding validation rule. Replaced the single-archive upload target description in Section 5 with the per-file layout and idempotent resume / auto-migration contract. Added the sentinel operator note prohibiting operator-placed objects with the reserved sentinel name.</notes>
    </change>
  </changes_applied>
  <summary>
    <total_changes>16</total_changes>
    <completed>16</completed>
    <skipped>0</skipped>
    <blocked>0</blocked>
  </summary>
  <next_steps>Recommended: run /test to validate all changes. The brainstorm next_steps also identify a cycle-wide doc refresh (/document) and a code review of the routing decision tree (/cr) as downstream actions; the four scope notes embedded in the plan specifically deferred cycle-wide README / INPUT_SPECIFICATION harmonization and AID_LOG sync to that subsequent /document invocation. Action items for the testing pass per the brainstorm: unit tests for the upload-target enumerator and the routing helper; integration tests for the per-file upload and the legacy-archive migration.</next_steps>
</implement_report>
