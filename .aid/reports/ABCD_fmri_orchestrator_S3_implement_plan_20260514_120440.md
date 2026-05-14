<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-05-14T12:04:40-04:00" />

  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_brainstorm_20260514_154849.md" mode="brainstorm" key_items="15" />
  </input_reports>

  <scope_notes>
    <note id="N1">
      The brainstorm's AI9 phrase "Update validate_proc_template and the README/INPUT_SPECIFICATION accordingly" includes `validate_proc_template` in the list. After codebase inspection, `validate_proc_template` (orchestrator_utils.py:2839) cross-validates the orchestrator config's `analyses` block against the proc template; it does not touch the `s3` config section. The new `s3.upload_max_workers` field requires no change in `validate_proc_template`. The plan therefore implements the schema and operator-doc parts of AI9 but does not introduce a no-op edit to `validate_proc_template`.
    </note>
    <note id="N2">
      The brainstorm's AI9 (P1) and AI10 (P2) both call for README/INPUT_SPECIFICATION updates; the brainstorm's `next_steps` further stages a cycle-wide refresh under `/document` after the build phase. This plan therefore covers the targeted, contract-level edits required for operator-facing correctness (new config field, per-file layout, sentinel, auto-migration banner) and defers cycle-wide polish (cross-references, AID_LOG, .aid/ provenance reports, prose harmonization) to the subsequent `/document` invocation per the brainstorm's stated workflow.
    </note>
    <note id="N3">
      The brainstorm action items name a function `migrate_session_from_archive` whose body description includes integrity-check semantics (intact tarball → migrate; corrupt tarball → fall through to FULL). To keep the routing function pure and avoid duplicating the download+extract step, the plan introduces a `LegacyArchiveCorruptError` exception (consistent with the existing `OrchestratorError` pattern in orchestrator_utils.py:32) raised by `migrate_session_from_archive` on integrity failure; `_process_session` catches this signal and falls through to the FULL path. `determine_session_routing` performs only `head_object` checks (sentinel, then legacy tarball) and returns `"skip"`, `"migrate"`, or `"full"` without downloading.
    </note>
    <note id="N4">
      Option A locked for `_process_session` return contract: the function returns a structured dict `{"routing": str, "analysis_outcomes": list, "qc_path": str|None, "remote_metadata": dict}`. `_derive_session_status` is refactored to accept `(routing, analysis_outcomes)` and emit `"skipped"` for `routing == "skip"` and `"migrated"` for `routing == "migrate"`. The FULL path retains the existing three-way mapping (`success`/`partial`/`failed`). `process_participant` is updated to consume the dict, propagate `routing` and `remote_metadata` into `session_results`, and extend the per-session summary printout with the new statuses.
    </note>
  </scope_notes>

  <changes>

    <change id="C1" priority="P1" source_item="brainstorm AI1">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add helper `enumerate_upload_targets(session_out_dir, sub_id, session)` that walks the canonical session output tree and returns the upload target list. This is the single source of truth for the per-session expected key set used by both upload (gap computation, verification) and the routing's downstream consumers.
      </description>
      <spec>
        Insert as a new helper near the top of Section A (after the `_get_s3_client` definition, before `discover_available_sessions`) so all S3 helpers live in the same section.

        Signature: `def enumerate_upload_targets(session_out_dir, sub_id, session):`

        Returns: `list[dict]` where each dict has keys `{"local_path": str, "s3_key_suffix": str, "size_bytes": int}`. `s3_key_suffix` is the part of the S3 key that follows `{upload_prefix}/sub-{sub_id}/ses-{session}A/`. The full S3 key is composed at upload time by `upload_session_to_s3`.

        Walk rules (mirror the inclusion logic in the deprecated `compress_session_outputs`):
          - `first_level_out/`: full recursive `os.walk`; include all files regardless of extension.
          - `qc/`: full recursive `os.walk`; include all files regardless of extension.
          - `preproc/`: single-level `os.listdir`; include files only (skip dirs); exclude `*.nii.gz`.
          - `concat/`: single-level `os.listdir`; include files only (skip dirs); exclude `*.nii.gz`.

        For each included file:
          - `local_path` = absolute path returned by `os.path.join`.
          - `s3_key_suffix` = path relative to `session_out_dir`, with `os.sep` replaced by `"/"` for forward-slash S3 keys.
          - `size_bytes` = `os.path.getsize(local_path)`.

        Behaviour: missing subdirs (any of the four) → silently skip that subdir, do not raise. Empty subdirs → silently skip. The result list is sorted by `s3_key_suffix` for deterministic ordering (eases test fixtures and log readability).

        Does NOT include the `_COMPLETE` sentinel; the sentinel is written separately by `upload_session_to_s3` after verification.

        Logger usage: no logging at this level; the function is a pure enumerator.
      </spec>
      <dependencies>none</dependencies>
      <risk>low — pure enumeration over the existing local tree, no side effects, no S3 calls.</risk>
      <rollback>delete the function definition; no callers exist until C5 lands.</rollback>
    </change>

    <change id="C2" priority="P1" source_item="brainstorm AI3">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add three internal helpers for sentinel-object management. These centralize the S3 key conventions for the per-session prefix and the sentinel object, so call sites (routing, upload, migrate) do not duplicate key construction.
      </description>
      <spec>
        Insert immediately after `enumerate_upload_targets` (Section A).

        Helper 1 (private):
          `def _session_upload_prefix(s3_config, sub_id, session) -> str:`
          Returns `f"{s3_config['upload_prefix']}/sub-{sub_id}/ses-{session}A"`.
          No trailing slash. Used as the prefix root for all per-session S3 keys.

        Helper 2 (public):
          `def check_session_complete(s3_config, sub_id, session, logger) -> bool:`
          Calls `s3_client.head_object(Bucket=s3_config['bucket'], Key=f"{_session_upload_prefix(...)}/_COMPLETE")`.
          Returns `True` if the head_object succeeds, `False` if `ClientError` with `404`/`NoSuchKey`. Re-raises any other `ClientError` for the caller to handle as an OrchestratorError condition.
          Logs at DEBUG level on hit ("Sentinel found: %s") and miss ("Sentinel absent: %s"); does not log on re-raised errors (the caller handles).

        Helper 3 (public):
          `def delete_session_sentinel(s3_config, sub_id, session, logger) -> None:`
          Calls `s3_client.delete_object(Bucket=..., Key=f"{_session_upload_prefix(...)}/_COMPLETE")`. Idempotent — boto3 `delete_object` returns success for non-existent keys. Logs at INFO level ("Deleted sentinel: %s") on completion. Re-raises `ClientError` to the caller.

        All three use `_get_s3_client()` internally and do not cache the client (consistent with the rest of orchestrator_utils.py).
      </spec>
      <dependencies>none</dependencies>
      <risk>low — head_object/delete_object on a single key with no fan-out.</risk>
      <rollback>delete the three helpers; no callers until C4/C5/C6/C11 land.</rollback>
    </change>

    <change id="C3" priority="P1" source_item="brainstorm AI5 (signal mechanism implied by Q8 fall-through requirement)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add a signal-only exception class `LegacyArchiveCorruptError` raised by `migrate_session_from_archive` when the legacy tarball download succeeds but the archive fails extraction or sanity checks. This is the mechanism by which `_process_session` learns to fall through to FULL processing per Q8 routing rule 4 ("first_level_out.tar.gz exists but extraction fails or the extracted file set fails sanity checks: silent fall-through to FULL processing").
      </description>
      <spec>
        Insert immediately after the existing `OrchestratorError` class (orchestrator_utils.py:32-34):

        ```python
        class LegacyArchiveCorruptError(Exception):
            """
            Raised by migrate_session_from_archive when a legacy on-S3 tarball
            cannot be migrated cleanly (tarfile.TarError on extract, or extracted
            file set fails sanity checks). Caught by _process_session as a signal
            to fall through to FULL processing per the Q8 routing decision tree.
            """
            pass
        ```

        Note: this is intentionally a sibling of `OrchestratorError`, not a subclass. Subclassing would risk accidental catch-and-promote in upstream `except OrchestratorError` blocks that should never see this signal.
      </spec>
      <dependencies>none</dependencies>
      <risk>low — pure class definition.</risk>
      <rollback>delete the class definition.</rollback>
    </change>

    <change id="C4" priority="P1" source_item="brainstorm AI4">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add routing helper `determine_session_routing(s3_config, sub_id, session, force_recompute, logger)` returning `"skip"`, `"migrate"`, or `"full"` per the Q8 decision tree. Pure `head_object`-based; does not download anything. Integrity is verified later by `migrate_session_from_archive` (which raises `LegacyArchiveCorruptError` on failure).
      </description>
      <spec>
        Insert after `delete_session_sentinel` (Section A).

        Signature:
          `def determine_session_routing(s3_config, sub_id, session, force_recompute, logger):`
          Returns `dict` with two keys:
            - `"routing"`: one of `"skip"`, `"migrate"`, `"full"`.
            - `"remote_metadata"`: dict carrying artifact metadata for the chosen routing path.
                - On `"skip"`: `{"sentinel_last_modified": "<ISO8601>"}` from the sentinel head_object.
                - On `"migrate"`: `{"source_tarball_etag": "<ETag>", "source_tarball_size": int, "source_tarball_last_modified": "<ISO8601>"}` from the tarball head_object.
                - On `"full"`: `{}` (empty dict — no remote artifact to attribute).

        Decision algorithm (executed in order):

          1. `sentinel_present = check_session_complete(s3_config, sub_id, session, logger)`. If `True`:
             - If `force_recompute == False`:
               - Read sentinel head_object to retrieve `LastModified`.
               - Return `{"routing": "skip", "remote_metadata": {"sentinel_last_modified": <iso>}}`.
             - If `force_recompute == True`:
               - Call `delete_session_sentinel(s3_config, sub_id, session, logger)` to clear the stale marker.
               - Log INFO: "force_recompute=True: deleted existing sentinel for sub-%s ses-%s; routing FULL".
               - Fall through to legacy-tarball check below (force_recompute does not interact with the legacy tarball decision — if a tarball still exists alongside force_recompute, the user wants reprocessing, not migration).
               - Return `{"routing": "full", "remote_metadata": {}}` immediately (skip step 2 in this branch).

          2. `tarball_key = f"{_session_upload_prefix(...)}/first_level_out.tar.gz"`.
             Try `head_object` on `tarball_key`:
               - On success: extract `ETag`, `ContentLength`, `LastModified` from the response.
                 Return `{"routing": "migrate", "remote_metadata": {"source_tarball_etag": <etag>, "source_tarball_size": <size>, "source_tarball_last_modified": <iso>}}`.
               - On `ClientError` with `404`/`NoSuchKey`: continue.
               - On any other `ClientError`: re-raise as `OrchestratorError(f"Routing probe failed for {tarball_key}: {e}")`.

          3. Neither sentinel nor tarball: return `{"routing": "full", "remote_metadata": {}}`.

        Banner logging: INFO-level one-line banner identifying the chosen routing path and the relevant remote artifact:
          - SKIP: "Routing: SKIP for sub-%s ses-%s (sentinel last_modified=%s)"
          - MIGRATE: "Routing: MIGRATE for sub-%s ses-%s (source tarball ETag=%s size=%d)"
          - FULL: "Routing: FULL for sub-%s ses-%s (no prior outputs detected)"

        ISO8601 conversion: boto3 returns `LastModified` as a `datetime.datetime` with tzinfo; call `.isoformat()` to render as a string for the dict. Lazy import `from datetime import datetime, timezone` at top of orchestrator_utils.py if not already imported (it is currently imported lazily inside `consolidate_session_qc` only; promote to module-level for clarity since multiple new helpers need it).
      </spec>
      <dependencies>C2 (check_session_complete, delete_session_sentinel, _session_upload_prefix)</dependencies>
      <risk>low — three head_object calls maximum per invocation; bounded latency; no fan-out.</risk>
      <rollback>delete the function definition; no callers until C11 lands.</rollback>
    </change>

    <change id="C5" priority="P1" source_item="brainstorm AI2">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add `upload_session_to_s3(s3_config, sub_id, session, session_out_dir, logger)`. Replaces the deprecated `upload_to_s3` (single-archive variant). Implements the full per-file upload contract: enumerate-and-diff, parallel upload, batched verification, atomic sentinel write.
      </description>
      <spec>
        Insert after `determine_session_routing` (Section A).

        Signature:
          `def upload_session_to_s3(s3_config, sub_id, session, session_out_dir, logger):`
          Returns `dict` with keys `{"n_files_uploaded": int, "n_files_total": int, "verified_keys": int, "sentinel_key": str}`. Returned for downstream logging only; the orchestrator typically does not consume the return.

        Procedure:

          1. **Enumerate expected targets**:
             `targets = enumerate_upload_targets(session_out_dir, sub_id, session)`.
             If `len(targets) == 0`: raise `OrchestratorError(f"No upload targets enumerated for sub-{sub_id} ses-{session}A; cannot proceed.")` — an empty session output tree at upload time is a defect, not a no-op.

          2. **Build expected-keys map**: `expected = {prefix + "/" + t["s3_key_suffix"]: t for t in targets}` where `prefix = _session_upload_prefix(s3_config, sub_id, session)`.

          3. **Probe remote inventory**:
             - `s3_client = _get_s3_client()`.
             - Paginate `list_objects_v2(Bucket=..., Prefix=prefix + "/")` and accumulate a dict `remote = {key: contents["Size"] for contents in pages...}`. The `_COMPLETE` sentinel may appear here from a stale prior attempt; that case is filtered separately below.
             - If `prefix + "/_COMPLETE"` is in `remote`: caller's responsibility (the routing layer) was to delete it before invoking upload — if we see it here, raise `OrchestratorError(f"Stale sentinel present at upload start: {prefix}/_COMPLETE. Caller did not clear it; aborting.")`. This is a defensive invariant; correct caller usage prevents this path.

          4. **Compute upload gap**:
             - For each `expected_key, target in expected.items()`:
                 - If `expected_key in remote` AND `remote[expected_key] == target["size_bytes"]`: skip (already uploaded with matching size).
                 - Else: add to `upload_list`.
             - Log INFO: "Upload gap for sub-%s ses-%s: %d/%d files to upload (%d already present)".

          5. **Parallel upload via ThreadPoolExecutor**:
             - `max_workers = s3_config.get("upload_max_workers", 8)`.
             - `from concurrent.futures import ThreadPoolExecutor, as_completed` (import at module top with other stdlib imports).
             - `with ThreadPoolExecutor(max_workers=max_workers) as ex:`
                 - For each target in `upload_list`: `fut = ex.submit(s3_client.upload_file, target["local_path"], s3_config["bucket"], expected_key)`. Stash futures in a list with their target reference.
                 - Iterate `as_completed`: on `fut.result()` exception, store the (target, exception) pair in a `failures` list; on success, increment counter.
             - After pool drains: if `failures`: raise `OrchestratorError(f"S3 upload failed for {len(failures)} file(s) in sub-{sub_id} ses-{session}A; first failure: {failures[0][0]['s3_key_suffix']}: {failures[0][1]}").` Do NOT proceed to verification or sentinel.

          6. **Batched verification**:
             - Re-run paginated `list_objects_v2` on the same prefix.
             - Build `final_remote = {key: size, ...}` filtering out the `_COMPLETE` key.
             - For each `expected_key, target in expected.items()`:
                 - If `expected_key not in final_remote`: collect into `missing`.
                 - Elif `final_remote[expected_key] != target["size_bytes"]`: collect into `mismatched` with `(expected_key, expected_size, actual_size)`.
             - If `missing` or `mismatched`: raise `OrchestratorError(f"Post-upload verification failed for sub-{sub_id} ses-{session}A: {len(missing)} missing, {len(mismatched)} size mismatch; first missing: {missing[0] if missing else 'n/a'}; first mismatch: {mismatched[0] if mismatched else 'n/a'}").` Do NOT write sentinel.

          7. **Write sentinel** (only on full verification pass):
             - `sentinel_key = f"{prefix}/_COMPLETE"`.
             - `s3_client.put_object(Bucket=s3_config["bucket"], Key=sentinel_key, Body=b"")`. Zero-byte object.
             - Log INFO: "Sentinel written: s3://%s/%s".

          8. **Return**:
             `return {"n_files_uploaded": len(upload_list), "n_files_total": len(expected), "verified_keys": len(expected), "sentinel_key": sentinel_key}`.

        Logging:
          - INFO at start: "Upload phase: sub-%s ses-%s, %d expected targets, max_workers=%d".
          - INFO after gap: "Upload gap: %d to upload, %d already present".
          - INFO during upload: per-file at DEBUG, every 10th at INFO ("%d/%d uploaded"). Not strictly required, but useful for large sessions.
          - INFO after verification: "Verification pass: %d keys present with matching sizes".
          - INFO on sentinel: as above.

        Boto3 thread-safety: the boto3 `Client` is thread-safe (documented). One `s3_client` reused across all worker threads. No per-thread client construction.

        Failure semantics: any raised `OrchestratorError` leaves the session in a half-uploaded state on S3 with NO sentinel. Re-entry under the same `s3_config` + `force_recompute=False` will route through `upload_session_to_s3` again (because the sentinel absent → routing returns FULL for fresh sessions, or MIGRATE for legacy-tarball sessions; in either case the per-file gap is small on a partial-prior-upload because the gap computation captures the already-uploaded subset).
      </spec>
      <dependencies>C1 (enumerate_upload_targets), C2 (_session_upload_prefix)</dependencies>
      <risk>medium — concurrent S3 I/O with verification. Failure modes are well-defined (raise without sentinel, leave half-state for resume) and idempotent resume relies on remote-state introspection rather than orchestrator memory.</risk>
      <rollback>delete the function definition; restore `upload_to_s3` references in callers (only `_process_session`) if the deletion has progressed. Pre-C8/C9 rollback is trivial.</rollback>
    </change>

    <change id="C6" priority="P1" source_item="brainstorm AI5, AI7">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Add `migrate_session_from_archive(s3_config, sub_id, session, session_out_dir, logger, source_tarball_etag)`. Downloads the legacy tarball, extracts to a staging dir, maps `{prefix}_{first_level_out,qc,preproc,concat}/` arcnames back to canonical local subdirs, inserts the migration provenance key into the staged session QC JSON, delegates to `upload_session_to_s3`, deletes the legacy tarball from S3 on success.

        AI7 (migration provenance key insertion) is implemented inline within this function rather than as a separate change; it is logically part of the migration procedure and shares context with the staging step.
      </description>
      <spec>
        Insert after `upload_session_to_s3` (Section A).

        Signature:
          `def migrate_session_from_archive(s3_config, sub_id, session, session_out_dir, logger, source_tarball_etag):`
          `source_tarball_etag` is passed in by the caller (`_process_session`) from the routing result's `remote_metadata`; threading it through avoids a duplicate `head_object` call inside this function.
          Returns `dict` with keys `{"n_files_migrated": int, "sentinel_key": str}` for downstream logging.

        Constants:
          - `ses_label = f"ses-{session}A"`.
          - `archive_prefix = f"sub-{sub_id}_{ses_label}"`.
          - `bucket = s3_config["bucket"]`.
          - `tarball_key = f"{_session_upload_prefix(s3_config, sub_id, session)}/first_level_out.tar.gz"`.

        Procedure:

          1. **Prepare directories**:
             - `os.makedirs(session_out_dir, exist_ok=True)`.
             - Subdirs `first_level_out/`, `qc/`, `preproc/`, `concat/` under `session_out_dir`: create them with `os.makedirs(..., exist_ok=True)`.
             - Staging dir: `staging_dir = os.path.join(session_out_dir, "_migration_staging")`. `os.makedirs(staging_dir, exist_ok=True)`.
             - Local tarball: `local_archive_path = os.path.join(session_out_dir, "legacy_archive.tar.gz")`.

          2. **Download legacy tarball**:
             - `s3_client = _get_s3_client()`.
             - `s3_client.download_file(bucket, tarball_key, local_archive_path)`. On `ClientError`: re-raise as `OrchestratorError(f"Failed to download legacy tarball {tarball_key}: {e}")`.
             - Log INFO: "Migration download complete: %s (%.1f MB)".

          3. **Extract to staging** (any failure here raises `LegacyArchiveCorruptError`):
             - Reuse the path-traversal guard pattern from `extract_session_archive` (orchestrator_utils.py:461-477): open with `tarfile.open(local_archive_path, "r:gz")`, filter members to safe paths, `extractall` into `staging_dir`.
             - On `tarfile.TarError` (or `tarfile.ReadError` which inherits): `raise LegacyArchiveCorruptError(f"Failed to extract legacy tarball {tarball_key}: {e}")` — `_process_session` catches this and falls through to FULL.
             - On 0 safe members extracted: `raise LegacyArchiveCorruptError(f"Legacy tarball {tarball_key} contained no safe members.")`.

          4. **Validate arcname structure** (sanity check; raises `LegacyArchiveCorruptError` on failure):
             - Expected arcnames at the top level of `staging_dir`: `{archive_prefix}_first_level_out/`, `{archive_prefix}_qc/`, and optionally `{archive_prefix}_preproc/` and `{archive_prefix}_concat/`.
             - Required: `{archive_prefix}_first_level_out/` and `{archive_prefix}_qc/` must both exist as directories under `staging_dir`.
             - If either required arcname is missing: `raise LegacyArchiveCorruptError(f"Legacy tarball {tarball_key} missing required arcname(s); found at staging root: {os.listdir(staging_dir)}")`.

          5. **Stage files under canonical subdirs**:
             - For each `(src_arcname, dst_subdir)` in `[(f"{archive_prefix}_first_level_out", "first_level_out"), (f"{archive_prefix}_qc", "qc"), (f"{archive_prefix}_preproc", "preproc"), (f"{archive_prefix}_concat", "concat")]`:
                 - `src_path = os.path.join(staging_dir, src_arcname)`.
                 - `dst_path = os.path.join(session_out_dir, dst_subdir)`.
                 - If `os.path.isdir(src_path)`: move/copy contents from `src_path` into `dst_path`. Use `shutil.copytree(src_path, dst_path, dirs_exist_ok=True)` so existing directory structure is preserved.
                 - If the optional `_preproc`/`_concat` arcname is absent: skip silently.

          6. **Insert migration provenance key into staged session QC JSON (AI7)**:
             - `qc_json_path = os.path.join(session_out_dir, "qc", f"sub-{sub_id}_{ses_label}_orchestrator_qc.json")`.
             - If file exists: read with `json.load`; add top-level `"migration"` key:
               ```python
               from datetime import datetime, timezone
               try:
                   from orchestrate_first_level import __version__ as _orch_ver
               except ImportError:
                   _orch_ver = "unknown"
               qc_obj["migration"] = {
                   "status": "migrated_from_archive",
                   "source_tarball_etag": source_tarball_etag,
                   "migration_timestamp_utc": datetime.now(timezone.utc).isoformat(),
                   "orchestrator_version_at_migration": _orch_ver,
               }
               ```
               Write back atomically with `json.dump(..., indent=2)` via a `.tmp` + `os.rename` pattern (consistent with `save_qc_json`).
             - If file does not exist: log WARNING: "Legacy tarball lacked session QC JSON at %s; migration provenance key not inserted." Do NOT raise — proceed to upload (per-analysis upstream qc_summary.json files inside `first_level_out/{analysis}/` are untouched and remain authoritative for analysis-level QC).

          7. **Upload via per-file routine**:
             - `upload_result = upload_session_to_s3(s3_config, sub_id, session, session_out_dir, logger)`.
             - If this raises: leave the legacy tarball alone (do NOT delete the legacy archive on S3) so the next invocation can re-attempt the migration. Re-raise.

          8. **Delete legacy tarball from S3** (only on successful sentinel write):
             - `s3_client.delete_object(Bucket=bucket, Key=tarball_key)`.
             - Log INFO: "Legacy tarball deleted from S3: %s/%s".
             - On `ClientError`: log ERROR but do NOT raise (the migration is functionally complete; a stuck legacy tarball is recoverable manually and does not break correctness of the new layout). Future routing on this session will return SKIP because the sentinel is present.

          9. **Cleanup local intermediates**:
             - `shutil.rmtree(staging_dir, ignore_errors=True)`.
             - `os.remove(local_archive_path)` (wrapped in try/except OSError → log warning).
             - The canonical subdirs under `session_out_dir` remain in place; the subsequent cleanup path in `_process_session` (gated by `s3.cleanup_after_upload`) handles `session_out_dir` removal.

          10. **Return**: `return {"n_files_migrated": upload_result["n_files_uploaded"], "sentinel_key": upload_result["sentinel_key"]}`.

        Banner logging:
          - INFO at entry: "Migration start: sub-%s ses-%s, source ETag=%s".
          - INFO at completion: "Migration complete: sub-%s ses-%s, %d files migrated, sentinel written".
      </spec>
      <dependencies>C1, C2, C3 (LegacyArchiveCorruptError), C5 (upload_session_to_s3)</dependencies>
      <risk>medium — multi-step procedure with several failure points (download, extract, stage, upload, delete). Each failure mode has a defined behavior (raise OrchestratorError or LegacyArchiveCorruptError; never write sentinel on partial state).</risk>
      <rollback>delete the function definition; orchestrate_first_level.py routing dispatch (C11) currently has the only caller and will skip the MIGRATE branch.</rollback>
    </change>

    <change id="C7" priority="P1" source_item="brainstorm AI9 (config schema portion)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Extend `load_orchestrator_config` to validate and default the new `s3.upload_max_workers` field (default 8, type int, range [1, 64]) per the Q4 decision.
      </description>
      <spec>
        Edit the `s3` validation block in `load_orchestrator_config`. The exact insertion point is after the existing `s3.cleanup_after_upload` and `s3.available_sessions` setdefault lines at orchestrator_utils.py:2832-2833.

        Add immediately after line 2833:

        ```python
        config["s3"].setdefault("upload_max_workers", 8)
        umw = config["s3"]["upload_max_workers"]
        if not isinstance(umw, int) or isinstance(umw, bool) or not (1 <= umw <= 64):
            raise OrchestratorError(
                f"s3.upload_max_workers must be an integer in [1, 64], got: {umw!r}"
            )
        ```

        The `isinstance(umw, bool)` check is required because `bool` is a subtype of `int` in Python; without it, `True`/`False` would silently pass the int check.

        No change to `validate_proc_template` (see scope_notes N1).
      </spec>
      <dependencies>none</dependencies>
      <risk>low — defaulting plus simple range validation; semantically additive (existing configs without the field continue to work with the default).</risk>
      <rollback>remove the four added lines.</rollback>
    </change>

    <change id="C8" priority="P1" source_item="brainstorm AI8">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Delete `compress_session_outputs` (orchestrator_utils.py:2973-3062) entirely. It has no remaining caller after C11 lands.
      </description>
      <spec>
        Remove the function definition spanning the comment header at line 2970 ("# Section N: Output Compression and Local Cleanup") down through and including line 3062 (closing of the function). Retain the immediately-following `cleanup_local_inputs` definition (line 3065+).

        Also remove the Section N comment header for "Output Compression" (only the compression portion); rename the remaining section header to "Section N: Local Cleanup" to reflect what is left in the section (just `cleanup_local_inputs`).

        Order constraint: must be executed AFTER C11 (which removes the call site in `_process_session`) and after C13 (which removes the import in orchestrate_first_level.py). Otherwise the import or call resolution would fail mid-build.
      </spec>
      <dependencies>C11, C13</dependencies>
      <risk>low — pure deletion of an unreferenced symbol after upstream call site removal.</risk>
      <rollback>restore the function from git history.</rollback>
    </change>

    <change id="C9" priority="P1" source_item="brainstorm AI2 (replacement clause)">
      <file path="orchestrator_utils.py" action="modify" />
      <description>
        Delete `upload_to_s3` (orchestrator_utils.py:507-566) entirely. Replaced by `upload_session_to_s3` (C5). No remaining caller after C11/C13.
      </description>
      <spec>
        Remove the function definition spanning lines 507-566. Retain the immediately-following Section B header for "AFNI Check" at line 568.

        Order constraint: must be executed AFTER C11 (which removes the call site in `_process_session`) and after C13 (which removes the import in orchestrate_first_level.py).
      </spec>
      <dependencies>C11, C13</dependencies>
      <risk>low — pure deletion after upstream call site removal.</risk>
      <rollback>restore the function from git history.</rollback>
    </change>

    <change id="C10" priority="P1" source_item="Option A locked contract (Q-locked sub-decision, post-brainstorm)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Refactor `_derive_session_status` (orchestrate_first_level.py:79-102) to accept `(routing, analysis_outcomes)` and emit two new statuses `"skipped"` and `"migrated"` for the SKIP and MIGRATE routing paths respectively. The FULL path retains the existing three-way mapping.
      </description>
      <spec>
        Replace the existing function body with:

        ```python
        def _derive_session_status(routing, analysis_outcomes):
            """
            Derive a qualified session status from the routing decision and (for the
            FULL path) per-analysis outcome dicts.

            Parameters
            ----------
            routing : str
                One of "full", "skip", "migrate". From the dict returned by
                _process_session.
            analysis_outcomes : list of dict
                Per-analysis outcomes (populated only when routing == "full").

            Returns
            -------
            str
                "skipped"  — routing == "skip" (prior outputs satisfied, no work done)
                "migrated" — routing == "migrate" (legacy tarball rehosted as per-file)
                "success"  — routing == "full", all analyses succeeded
                "partial"  — routing == "full", at least one analysis succeeded
                "failed"   — routing == "full", no analyses ran or all analyses failed
            """
            if routing == "skip":
                return "skipped"
            if routing == "migrate":
                return "migrated"
            # routing == "full"
            if not analysis_outcomes:
                return "failed"
            if all(o["status"] == "success" for o in analysis_outcomes):
                return "success"
            if any(o["status"] == "success" for o in analysis_outcomes):
                return "partial"
            return "failed"
        ```

        Update the two existing call sites to pass `routing` as the new first argument:
          - orchestrate_first_level.py:202 → see C12 (this caller is in `process_participant` and reads from the new dict return).
          - orchestrate_first_level.py:764 → see C11 (this caller is inside `_process_session`'s FULL path and always passes routing="full").
      </spec>
      <dependencies>none</dependencies>
      <risk>low — pure function with deterministic mapping.</risk>
      <rollback>restore the prior signature from git history; update the two call sites back to the single-argument form.</rollback>
    </change>

    <change id="C11" priority="P1" source_item="brainstorm AI6">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Refactor `_process_session` (orchestrate_first_level.py:279-811) for routing-aware dispatch with structured-dict return per Option A. At session entry (before Step 1), call `determine_session_routing`. SKIP and MIGRATE paths return early after a banner log; FULL path proceeds through the existing Steps 1-13 with Step 13 swapped to call `upload_session_to_s3` directly (no compress step). MIGRATE catches `LegacyArchiveCorruptError` and falls through to FULL.
      </description>
      <spec>
        **A. Return type change (Option A).**
        `_process_session` returns `dict` with keys:
          - `"routing"`: `"full"`, `"skip"`, or `"migrate"` (the routing actually taken; if MIGRATE fell through due to LegacyArchiveCorruptError, this is `"full"`).
          - `"analysis_outcomes"`: list of analysis outcome dicts (populated for `"full"`; empty list for `"skip"` and `"migrate"`).
          - `"qc_path"`: str path to the consolidated session QC JSON if written, else `None`.
          - `"remote_metadata"`: dict carrying routing-specific metadata (passed through from `determine_session_routing`).

        **B. Routing dispatch (inserted after Step 0 / before Step 1).**

        Replace the current opening of `_process_session` after the existing setup block (after the `try:` at line 325) with a routing pre-flight. The routing pre-flight runs only when `s3_cfg.get("enabled", False)` is True — local mode (s3 disabled) preserves the current direct-to-FULL behavior because there is no remote inventory to probe.

        Pseudocode for the new entry block (replaces nothing; inserted at L325 after `try:`):

        ```python
        # Routing pre-flight (only meaningful when S3 is enabled)
        routing = "full"
        remote_metadata = {}
        if s3_cfg.get("enabled", False):
            routing_result = determine_session_routing(
                s3_cfg, sub_id, session, force_recompute, logger
            )
            routing = routing_result["routing"]
            remote_metadata = routing_result["remote_metadata"]

        if routing == "skip":
            logger.info(
                "Session %s for sub-%s already complete on S3 "
                "(sentinel last_modified=%s); skipping.",
                ses_label, sub_id,
                remote_metadata.get("sentinel_last_modified"),
            )
            return {
                "routing": "skip",
                "analysis_outcomes": [],
                "qc_path": None,
                "remote_metadata": remote_metadata,
            }

        if routing == "migrate":
            logger.info(
                "Session %s for sub-%s has legacy tarball on S3 "
                "(ETag=%s); migrating to per-file layout.",
                ses_label, sub_id,
                remote_metadata.get("source_tarball_etag"),
            )
            try:
                migrate_result = migrate_session_from_archive(
                    s3_cfg, sub_id, session, session_out, logger,
                    remote_metadata["source_tarball_etag"],
                )
                logger.info(
                    "Session %s for sub-%s migrated: %d files; sentinel %s",
                    ses_label, sub_id,
                    migrate_result["n_files_migrated"],
                    migrate_result["sentinel_key"],
                )
                # Cleanup local intermediates (same contract as FULL path)
                if s3_cfg.get("cleanup_after_upload", True):
                    if os.path.isdir(session_out):
                        shutil.rmtree(session_out, ignore_errors=True)
                        logger.info("Removed session output directory: %s", session_out)
                return {
                    "routing": "migrate",
                    "analysis_outcomes": [],
                    "qc_path": None,
                    "remote_metadata": remote_metadata,
                }
            except LegacyArchiveCorruptError as e:
                logger.warning(
                    "Legacy tarball for sub-%s ses-%s is corrupt: %s. "
                    "Falling through to FULL processing.",
                    sub_id, ses_label, str(e)
                )
                # Reset routing to FULL; clean staging from any partial migration
                routing = "full"
                # Best-effort cleanup of staging dir if migration partially executed
                staging_dir = os.path.join(session_out, "_migration_staging")
                if os.path.isdir(staging_dir):
                    shutil.rmtree(staging_dir, ignore_errors=True)
                legacy_archive = os.path.join(session_out, "legacy_archive.tar.gz")
                if os.path.isfile(legacy_archive):
                    try:
                        os.remove(legacy_archive)
                    except OSError:
                        pass
                # Fall through to Step 1 (FULL processing) below
        ```

        **C. Step 13 replacement (FULL-path upload).**

        Replace lines 776-796 (the current Step 13a/13b/13c block):

        ```python
        # OLD (lines 776-796):
        # ============================================================
        # Step 13: Compress, upload, cleanup
        # ============================================================
        if s3_cfg.get("enabled", False) and processed_files:
            logger.info("Step 13a: Compressing session outputs...")
            archive_path = compress_session_outputs(sub_id, session, session_out, logger)
            logger.info("Step 13b: Uploading results archive to S3...")
            upload_to_s3(s3_cfg, sub_id, session, archive_path, logger)
            if s3_cfg.get("cleanup_after_upload", True):
                ...
        ```

        With:

        ```python
        # NEW:
        # ============================================================
        # Step 13: Per-file upload + sentinel + local cleanup
        # ============================================================
        if s3_cfg.get("enabled", False) and processed_files:
            logger.info("Step 13a: Uploading session outputs (per-file) to S3...")
            upload_result = upload_session_to_s3(
                s3_cfg, sub_id, session, session_out, logger
            )
            logger.info(
                "Step 13a complete: %d files uploaded, sentinel at %s",
                upload_result["n_files_uploaded"], upload_result["sentinel_key"]
            )

            if s3_cfg.get("cleanup_after_upload", True):
                logger.info("Step 13b: Cleaning up local files...")
                cleanup_local_inputs(downloaded_paths, logger)
                if extracted_dir and os.path.isdir(extracted_dir):
                    shutil.rmtree(extracted_dir, ignore_errors=True)
                    logger.info("Removed extracted directory: %s", extracted_dir)
                if os.path.isdir(session_out):
                    shutil.rmtree(session_out, ignore_errors=True)
                    logger.info("Removed session output directory: %s", session_out)
        ```

        Step renumbering: 13a/13b/13c → 13a/13b. The compress step is gone; 13a is now upload; 13b is now cleanup.

        **D. QC JSON construction (Step 12b at L762-774): pass routing="full" to `_derive_session_status`.**

        Update the call site:

        ```python
        # OLD (line 764):
        qc_session_status = _derive_session_status(analysis_outcomes)
        # NEW:
        qc_session_status = _derive_session_status("full", analysis_outcomes)
        ```

        Also capture the QC path for the structured return:

        ```python
        qc_path = None
        if not skip_qc and (preproc_qc_by_run or analysis_outcomes):
            qc_session_status = _derive_session_status("full", analysis_outcomes)
            qc_json_path = os.path.join(
                session_out, "qc",
                f"sub-{sub_id}_{ses_label}_orchestrator_qc.json"
            )
            session_wall_time = time.time() - session_start_time
            consolidate_session_qc(
                sub_id, ses_label, qc_session_status, session_wall_time,
                preproc_qc_by_run, analysis_outcomes, qc_json_path, logger
            )
            qc_path = qc_json_path
        ```

        **E. Return statement change (L810-811).**

        Replace `return analysis_outcomes` with:

        ```python
        return {
            "routing": "full",
            "analysis_outcomes": analysis_outcomes,
            "qc_path": qc_path,
            "remote_metadata": remote_metadata,
        }
        ```

        `remote_metadata` is `{}` on FULL routing reached natively, or the routing dict's metadata if a MIGRATE fell through (in which case the metadata carries provenance for the failed legacy tarball; useful for logging/debugging).

        **F. Docstring update.**

        Update the docstring at lines 280-294 to describe the new routing-aware lifecycle:

        ```
        Process a single session through the full pipeline.

        Routing pre-flight: probe S3 for prior outputs.
          - SKIP: prior sentinel found and force_recompute=False; return immediately.
          - MIGRATE: prior legacy tarball found; rehost as per-file layout and return.
            (Falls through to FULL if the legacy tarball is corrupt.)
          - FULL: no prior outputs (or force_recompute=True); run the full pipeline.

        FULL pipeline steps:
          1. Download session data from S3
          2. Extract archive
          3. Discover files per task
          4. Per-task preprocessing (mask, QC, NSS, motion, tissue, timing)
          5. Concatenate or collect runs
          6. Build first-level config
          7. Run first-level analyses
          12b. Consolidated session QC JSON
          13a. Per-file upload + sentinel
          13b. Local cleanup
        ```

        **G. `shutil` and `os` imports**: both are already imported at the top of orchestrate_first_level.py (lines 27-32). No new imports needed in this file for C11 beyond what C13 adds.
      </spec>
      <dependencies>C2, C4, C5, C6, C10, C13</dependencies>
      <risk>medium — large refactor of the session driver. Each path (SKIP/MIGRATE/MIGRATE-fall-through/FULL) requires explicit test coverage in /test design.</risk>
      <rollback>restore the function body from git history.</rollback>
    </change>

    <change id="C12" priority="P1" source_item="Option A locked contract (Q-locked sub-decision, post-brainstorm)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Update `process_participant` (orchestrate_first_level.py:184-269) to consume the new dict return from `_process_session`. Propagate `routing` and `remote_metadata` into `session_results`. Extend the per-session summary counter loop to recognize the new `"skipped"` and `"migrated"` statuses.
      </description>
      <spec>
        **A. Call-site update (L196-207).**

        Replace:

        ```python
        # OLD (L196-207):
        analysis_outcomes = _process_session(...)
        session_status = _derive_session_status(analysis_outcomes)
        session_results[session] = {
            "status": session_status,
            "analyses": analysis_outcomes,
        }
        ```

        With:

        ```python
        # NEW:
        session_result = _process_session(
            config, sub_id, session, proc_template,
            skip_qc, skip_first_level, logger
        )
        session_status = _derive_session_status(
            session_result["routing"], session_result["analysis_outcomes"]
        )
        session_results[session] = {
            "status": session_status,
            "routing": session_result["routing"],
            "analyses": session_result["analysis_outcomes"],
            "qc_path": session_result["qc_path"],
            "remote_metadata": session_result["remote_metadata"],
        }
        ```

        **B. Exception-path session_results entries (L209-231).**

        Update the `except OrchestratorError` block and the `except Exception` block to also include `routing="full"` (the exception originated during FULL processing — by construction, SKIP/MIGRATE early-return before any exception-prone steps run):

        ```python
        session_results[session] = {
            "status": "failed",
            "routing": "full",
            "analyses": [],
            "qc_path": None,
            "remote_metadata": {},
            "error": session_error,
        }
        ```

        Both `except` blocks get this updated entry. (SKIP cannot reach these except blocks because it returns before the `try:` body's content executes; MIGRATE either succeeds-and-returns or falls through to FULL where exceptions are caught.)

        **C. Per-session summary counter loop (L244-270).**

        Update the counter logic to recognize the two new statuses. Currently:

        ```python
        n_success = 0
        n_partial = 0
        n_failed = 0
        for ses, result in session_results.items():
            ...
            if ses_status == "success":
                n_success += 1
            elif ses_status == "partial":
                n_partial += 1
            else:
                n_failed += 1
        logger.info(
            "Total: %d success, %d partial, %d failed out of %d session(s)",
            n_success, n_partial, n_failed, len(session_results)
        )
        ```

        Replace with:

        ```python
        n_success = 0
        n_partial = 0
        n_failed = 0
        n_skipped = 0
        n_migrated = 0
        for ses, result in session_results.items():
            ses_status = result["status"]
            routing = result.get("routing", "full")
            # Banner line for the session
            if routing == "skip":
                logger.info("  ses-%sA: %s (skipped — prior outputs intact)", ses, ses_status)
            elif routing == "migrate":
                logger.info("  ses-%sA: %s (migrated from legacy archive)", ses, ses_status)
            else:
                logger.info("  ses-%sA: %s", ses, ses_status)

            # Per-analysis breakdown only for FULL path
            for outcome in result.get("analyses", []):
                if outcome["status"] == "success":
                    logger.info("    [OK]     %s (%.2fs)", outcome["name"], outcome["wall_time_seconds"])
                else:
                    logger.info("    [FAILED] %s: %s", outcome["name"], outcome["error"])
            if result.get("error"):
                logger.info("    Session-level error: %s", result["error"])

            if ses_status == "success":
                n_success += 1
            elif ses_status == "partial":
                n_partial += 1
            elif ses_status == "skipped":
                n_skipped += 1
            elif ses_status == "migrated":
                n_migrated += 1
            else:
                n_failed += 1
        logger.info(
            "Total: %d success, %d partial, %d skipped, %d migrated, %d failed "
            "out of %d session(s)",
            n_success, n_partial, n_skipped, n_migrated, n_failed,
            len(session_results)
        )
        ```

        **D. All-failed-raises guard (L272-276).**

        Update the terminal raise to count SKIP and MIGRATE as non-failures:

        ```python
        # OLD:
        if n_success == 0 and n_partial == 0 and n_failed > 0:
            raise OrchestratorError(...)
        # NEW:
        if n_success == 0 and n_partial == 0 and n_skipped == 0 and n_migrated == 0 and n_failed > 0:
            raise OrchestratorError(
                f"All {n_failed} session(s) failed for sub-{sub_id}. "
                f"Check log for details."
            )
        ```

        Semantics: only raise if every session attempted ended in `"failed"` — sessions that were skipped or migrated count as successful pipeline outcomes.
      </spec>
      <dependencies>C10, C11</dependencies>
      <risk>low — call-site refactor with deterministic mapping; the new counter logic is additive.</risk>
      <rollback>restore from git history.</rollback>
    </change>

    <change id="C13" priority="P1" source_item="brainstorm AI8 (implicit — import block must track removed symbols)">
      <file path="orchestrate_first_level.py" action="modify" />
      <description>
        Update the `from orchestrator_utils import (...)` block (lines 43-76) to remove `upload_to_s3` and `compress_session_outputs`, and to add the new public symbols introduced in C3, C4, C5, C6.
      </description>
      <spec>
        Current import block (orchestrate_first_level.py:43-76) imports:

        ```python
        from orchestrator_utils import (
            OrchestratorError,
            VALID_TASK_LABELS,
            discover_available_sessions,
            download_session_data,
            discover_local_mmps_files,
            extract_session_archive,
            upload_to_s3,
            compress_session_outputs,
            cleanup_local_inputs,
            ...
        )
        ```

        Modify the block to:

        ```python
        from orchestrator_utils import (
            OrchestratorError,
            LegacyArchiveCorruptError,
            VALID_TASK_LABELS,
            discover_available_sessions,
            download_session_data,
            discover_local_mmps_files,
            extract_session_archive,
            determine_session_routing,
            upload_session_to_s3,
            migrate_session_from_archive,
            cleanup_local_inputs,
            ...
        )
        ```

        Removed: `upload_to_s3`, `compress_session_outputs`.
        Added: `LegacyArchiveCorruptError`, `determine_session_routing`, `upload_session_to_s3`, `migrate_session_from_archive`.

        Other imports in the block (`verify_afni_installation`, `load_orchestrator_config`, etc.) are unchanged.

        Order constraint: this change must land AFTER C3, C4, C5, C6 are in place in orchestrator_utils.py (so the new symbols exist for import) and BEFORE C8, C9 (so the old symbols are still importable until their call sites have been swapped — but since C11 swaps the call sites and C11 depends on C13, the actual sequencing is: add new imports first, run C11 to update call sites, then C8/C9 delete the now-unreferenced functions).
      </spec>
      <dependencies>C3, C4, C5, C6 (must precede); C8, C9 (must follow)</dependencies>
      <risk>low — pure import-block edit.</risk>
      <rollback>restore the prior import block from git history.</rollback>
    </change>

    <change id="C14" priority="P2" source_item="brainstorm AI10">
      <file path="example_orchestrator_config.yaml" action="modify" />
      <description>
        Update the example orchestrator config to document the new `s3.upload_max_workers` field and a brief operator-facing note about the per-file upload contract.
      </description>
      <spec>
        Read the current `example_orchestrator_config.yaml` to determine the exact insertion point. The s3 section already contains fields like `bucket`, `upload_prefix`, `cleanup_after_upload`, `available_sessions`. Add a single new commented line in that section:

        ```yaml
        # Number of concurrent worker threads for per-file S3 upload.
        # Default 8; valid range [1, 64]. Boto3 client is thread-safe.
        upload_max_workers: 8
        ```

        Also append (or update if a comment block already exists) a short header comment near the top of the s3 section explaining the upload contract:

        ```yaml
        # Output uploads use a per-file layout under the mirror key pattern
        # s3://{bucket}/{upload_prefix}/sub-{ID}/ses-{NN}A/{first_level_out,qc,preproc,concat}/...
        # A zero-byte sentinel object _COMPLETE marks successful upload completion;
        # subsequent runs of this orchestrator detect the sentinel and skip the session.
        # Legacy on-S3 .tar.gz archives (from prior orchestrator versions) are
        # auto-migrated to the per-file layout on first re-touch (no reprocessing).
        ```

        Exact placement: add the comment block as the first item in the `s3:` section (before any field) so operators see the contract upfront. Add the `upload_max_workers` line near other tuning fields.

        File overwrite: this is an edit of an existing file. Per the user's file-management policy, file overwrites are permitted because the edit is within the project workspace and is a direct modification (not a write-from-scratch).
      </spec>
      <dependencies>C7 (config schema must accept the new field)</dependencies>
      <risk>low — documentation edit.</risk>
      <rollback>restore from git history.</rollback>
    </change>

    <change id="C15" priority="P2" source_item="brainstorm AI9 (README portion), AI10 (operator-facing docs portion)">
      <file path="README.md" action="modify" />
      <description>
        Targeted, contract-level updates to README.md to reflect the per-file upload contract, the `_COMPLETE` sentinel, the auto-migration behavior, and the new `s3.upload_max_workers` config field. Cycle-wide refresh (cross-references, prose harmonization, AID_LOG sync) is deferred to the subsequent `/document` invocation per the brainstorm's next_steps.
      </description>
      <spec>
        Required edits in README.md (read the current file to find the exact anchors before editing):

          1. **Upload section / S3 section**: Replace any prose describing the single-archive upload (`first_level_out.tar.gz`) with prose describing the per-file mirror layout. Key sentences to include:
             - "Each session's outputs are uploaded as individual files mirroring the local session output tree: `s3://{bucket}/{upload_prefix}/sub-{ID}/ses-{NN}A/{first_level_out,qc,preproc,concat}/...`."
             - "A zero-byte `_COMPLETE` sentinel object is written under the per-session prefix only after every expected file has been uploaded and verified."
             - "Subsequent invocations probe the sentinel and skip already-complete sessions; partial-upload sessions resume via remote-state set-difference."

          2. **Migration section** (new subsection or paragraph): document the auto-migration:
             - "Sessions previously uploaded as a single `first_level_out.tar.gz` are automatically detected on next re-touch and rehosted in the per-file layout. No reprocessing occurs; original numerical outputs are preserved byte-equivalently. The legacy archive is deleted from S3 after successful migration."
             - Include a note on the migration provenance key in QC JSONs: "The per-session orchestrator QC JSON for migrated sessions carries a top-level `migration` block with the source ETag, migration timestamp, and orchestrator version at migration time."

          3. **Configuration reference** (the existing s3-config block in README): add the new `upload_max_workers` field with its default (8) and range ([1, 64]). Add a one-sentence description tying it to the per-file upload's parallelism.

          4. **Find-and-replace check**: grep README.md for `first_level_out.tar.gz` and either remove the reference or annotate it as legacy ("In prior orchestrator versions, outputs were bundled as `first_level_out.tar.gz`; auto-migration handles legacy archives automatically").

        Scope discipline: this change is targeted. Cycle-wide refresh (e.g., section-numbering harmonization, full table-of-contents update, AID_LOG sync, .aid/ structure additions) belongs to `/document` per brainstorm next_steps.

        File overwrite: existing-file edit; permitted under project workspace policy.
      </spec>
      <dependencies>C5 (per-file upload behavior must exist for the docs to be accurate)</dependencies>
      <risk>low — documentation edit; correctness is verifiable by re-reading the edits against the locked decisions in the brainstorm.</risk>
      <rollback>restore from git history.</rollback>
    </change>

    <change id="C16" priority="P2" source_item="brainstorm AI9 (INPUT_SPECIFICATION portion)">
      <file path="INPUT_SPECIFICATION.md" action="modify" />
      <description>
        Targeted update to INPUT_SPECIFICATION.md to document the new `s3.upload_max_workers` field and update any reference to the legacy `first_level_out.tar.gz` upload contract. Cycle-wide harmonization deferred to `/document`.
      </description>
      <spec>
        Required edits (read the current file for exact anchors):

          1. **`s3` section field table** (or wherever individual s3 config fields are documented): add a row/entry for `upload_max_workers`:
             - Name: `upload_max_workers`
             - Type: integer
             - Required: no (default 8)
             - Range: [1, 64]
             - Description: "Number of concurrent worker threads used by the per-file S3 upload routine. Boto3 S3 client is thread-safe; one client is shared across workers. Increase for high-throughput links to S3; decrease to limit local memory/network pressure."

          2. **Upload behavior subsection** (if one exists describing the upload step semantics): replace any single-archive description with the per-file contract (mirror layout, sentinel, idempotent resume, auto-migration). Keep the description tight; full prose lives in README.md.

          3. **Sentinel object** (new subsection or note): document that the `_COMPLETE` sentinel object is written under each per-session prefix and that operators must NOT manually place objects named `_COMPLETE` under upload prefixes (the orchestrator interprets them as completion markers).

        Scope discipline: targeted edits only; full INPUT_SPECIFICATION harmonization (cross-references, section renumbering, examples) deferred to `/document`.

        File overwrite: existing-file edit; permitted.
      </spec>
      <dependencies>C7, C5</dependencies>
      <risk>low — documentation edit.</risk>
      <rollback>restore from git history.</rollback>
    </change>

  </changes>

  <execution_order>C3, C1, C2, C7, C4, C5, C6, C10, C13, C11, C12, C9, C8, C14, C15, C16</execution_order>

  <execution_notes>
    <note>
      Dependency chain for the orchestrator_utils.py side: C3 (exception class) → C1 (enumerate) → C2 (sentinel helpers) → C7 (config validator) → C4 (routing) → C5 (upload) → C6 (migrate). C8 and C9 (deletions) come last on the orchestrator_utils.py side, after C11 has swapped the call sites.
    </note>
    <note>
      Dependency chain for the orchestrate_first_level.py side: C10 (_derive_session_status signature) → C13 (import block additions, old symbols still present) → C11 (refactor _process_session, removes upload_to_s3/compress_session_outputs call sites) → C12 (process_participant updates). Then on the orchestrator_utils.py side: C9 → C8 (now safe to delete since no callers remain).
    </note>
    <note>
      Documentation edits (C14, C15, C16) run last. Order within docs is non-strict but C14 (example config) is conventionally first since the README and INPUT_SPECIFICATION may reference the example.
    </note>
    <note>
      No reordering of unrelated content in orchestrator_utils.py: Section A (S3 helpers) is currently sparse; the new helpers (C1, C2, C4, C5, C6) extend it. The section-N "Output Compression" header (line 2970) is removed by C8; section-N becomes "Local Cleanup" containing only `cleanup_local_inputs`.
    </note>
  </execution_notes>

</implement_plan>
