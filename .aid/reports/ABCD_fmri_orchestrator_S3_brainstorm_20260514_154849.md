<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-05-14T15:48:49Z" />

  <context_files>
    <file path="orchestrator_utils.py" relevance="Contains compress_session_outputs (L2973-3062, current tarball assembly with inclusion rules), upload_to_s3 (L507-566, single-archive multipart upload + ContentLength verification), extract_session_archive (L424-504, tarball extraction pattern reusable for migrate path), extract_tissue_signals (L1274-1329, T1 raw tissue signal extraction confirmed present in preproc/), and cleanup_local_inputs (L3065+, current cleanup contract gated by s3.cleanup_after_upload)." />
    <file path="orchestrate_first_level.py" relevance="Contains the session-centric driver (_process_session, L279-811): Step 9 invokes extract_tissue_signals for rest tasks (L486-508), Step 12b writes consolidated session QC JSON (L760-774), Step 13 invokes compress + upload + cleanup (L776-796). main() reads --log-file CLI arg for the per-invocation log (L840-855); log file is outside session_out tree and not uploaded today." />
    <file path="memory/MEMORY.md" relevance="Confirms current testing state (275 unit tests, N=30 cohort test pass), v2.5.0 alignment cycle published in commit 5861ad8, no active known issues; provides context for migration scope (existing on-S3 archives from prior cohorts are real artifacts requiring the migrate path)." />
  </context_files>

  <topics>

    <topic id="T1" title="Raw tissue signal time series in output bundle">
      <summary>
        User-reported concern that raw tissue signal time series (CSF, WM, GS .1D files) were not in the S3 output bundle alongside the upstream-computed temporal derivatives. Investigation against the codebase confirmed that the raw .1D files are extracted by extract_tissue_signals (orchestrator_utils.py:1274-1329, invoked at orchestrate_first_level.py:486-508) into session_out/preproc/sub-{ID}_{run-NN}_{csf,wm,gs}.1D and are included in the current tarball under the {prefix}_preproc/ arcname via the non-.nii.gz inclusion rule at orchestrator_utils.py:3033-3040. The reporter was looking in the wrong subdirectory (expected the raw signals alongside the derivatives in first_level_out/{analysis}/, where the upstream fmri_first_level_proc writes the derivatives when use_tissue_derivs=true; raw signals live separately in preproc/ by design). No code defect identified.
      </summary>
      <research>No external research required; resolved entirely by direct codebase verification.</research>
      <approaches>
        <approach id="T1.A1" label="No action" feasibility="high" risk="low">
          <description>Confirm the file is already in the bundle at the documented location and treat the report as a discoverability misunderstanding rather than a code defect.</description>
          <pros>No code change; current bundling logic is correct.</pros>
          <cons>Raw tissue signals (preproc/) and derivatives (first_level_out/{analysis}/) live in different subdirectories, which can confuse consumers expecting co-location.</cons>
          <statistical_considerations>None — file location does not affect numerical results.</statistical_considerations>
        </approach>
      </approaches>
      <decision status="decided" chosen="T1.A1">
        Resolved as a discoverability misunderstanding, not a code defect. The user confirmed the reporter was looking in the wrong subdirectory. No code change required. The per-file S3 upload migration in T2 naturally preserves the raw tissue signal placement: preproc/sub-{ID}_{run-NN}_{csf,wm,gs}.1D maps to s3://bucket/upload_prefix/sub-{ID}/ses-{NN}A/preproc/sub-{ID}_{run-NN}_{csf,wm,gs}.1D under the mirror layout (T2 Q1).
      </decision>
    </topic>

    <topic id="T2" title="Replace single-archive S3 upload with per-file uploads (with auto-migration of legacy archives)">
      <summary>
        The current orchestrator bundles all session outputs into a single first_level_out.tar.gz, uploads it as a single object to S3, and downstream consumers must download the entire archive to access any single file. This is a documented operational pain point: many use cases need only specific files (e.g., a single analysis-level QC summary, a single residual time series) and the download-extract-prune cycle is a significant wall-time cost. Decision: change the upload contract to per-file uploads at mirrored S3 keys, with an upload-complete sentinel, batched verification, idempotent resume, and an auto-migration path that detects pre-existing tarballs on S3 and rehosts them in the new layout without reprocessing.
      </summary>
      <research>No external research required; the design space (per-file upload + sentinel + batched verification + retry/resume) is well-established boto3 practice. Internal boto3 documentation confirms: (1) the standard retry mode handles ThrottlingException, RequestTimeout, 5xx, and connection resets with exponential backoff by default; (2) s3_client is thread-safe under concurrent.futures.ThreadPoolExecutor; (3) list_objects_v2 returns up to 1000 keys per call with ContentLength and ETag, sufficient for batched session-level verification.</research>
      <approaches>
        <approach id="T2.A1" label="Status quo (single archive)" feasibility="high" risk="low">
          <description>Retain the current first_level_out.tar.gz bundling and single-object upload.</description>
          <pros>Trivial atomicity (one head_object suffices for "is the session done"); zero migration cost; no new code; one verification round-trip.</pros>
          <cons>Forces full-archive download for any selective file access; documented operational pain point; the bundling overhead consumes session wall-time even for cohorts where downstream access is selective.</cons>
        </approach>
        <approach id="T2.A2" label="Per-file upload with mirror layout, sentinel, batched verification, idempotent resume, and self-migration" feasibility="high" risk="low">
          <description>Replace compress_session_outputs and upload_to_s3 with a per-file upload routine that walks session_out/{first_level_out,qc,preproc,concat}/ and uploads each file individually to mirrored S3 keys (e.g., s3://bucket/upload_prefix/sub-{ID}/ses-{NN}A/first_level_out/{analysis}/{file}). Use concurrent.futures.ThreadPoolExecutor(max_workers=8) wrapping s3_client.upload_file. After upload, run one list_objects_v2 to verify the full expected key set is present with matching sizes; on success, write a _COMPLETE sentinel object. On re-entry, idempotent resume via set-diff using the same list_objects_v2. Auto-migration: if a legacy first_level_out.tar.gz exists on S3 but no _COMPLETE sentinel, download, extract, re-upload per-file, delete legacy tarball, write sentinel; no reprocessing.</description>
          <pros>Eliminates the full-archive download cost; selective access becomes a direct aws s3 cp; idempotent resume composes with existing force_recompute discipline; sentinel doubles as cohort-level "is sub/session ready" indicator; self-migration removes the need for a separate migration script and avoids reprocessing the N=30 cohort already published in commit 5861ad8.</cons>
          <cons>N additional PUT requests per session (~$0.06 per 12,000 puts at S3 Standard pricing; negligible); slightly more code surface (sentinel management, set-diff resume, migrate-only path); per-session "session done" check now requires head_object on a sentinel object rather than the previous head_object on the tarball; the routing decision tree at session entry adds branching that requires careful test coverage.</cons>
          <statistical_considerations>None — purely a packaging-and-upload change with no effect on numerical outputs. The migrate-only path is byte-equivalent to the original outputs (no reprocessing, no re-derivation).</statistical_considerations>
        </approach>
        <approach id="T2.A3" label="Hybrid (per-file for small files, archive for large)" feasibility="medium" risk="medium">
          <description>Upload small text/CSV/JSON files individually, but archive large NIfTI files (per-run residuals, intersection masks) into a per-analysis tarball.</description>
          <pros>Reduces total upload-time wall-clock by avoiding many-small-file overhead for the large NIfTI tail; preserves selective access for the most-frequently-accessed files (QC, timing, motion).</pros>
          <cons>Two-tier contract is harder to document, test, and reason about; downstream consumers still need to handle both bundled and unbundled paths; partial-failure semantics complicate (which set lost coherence?); the user explicitly rejected widening the file set in Q2, so the large-NIfTI exclusion remains, eliminating most of the hybrid's value.</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="T2.A2">
        Per-file upload with mirror layout, sentinel, batched verification, idempotent resume, and self-migration. The eight sub-decisions of T2.A2 are locked as follows:

        **Q1 — S3 key layout: mirror local tree.** Keys take the form s3://{bucket}/{upload_prefix}/sub-{ID}/ses-{NN}A/{subdir}/{...}/{file} where {subdir} is one of {first_level_out, qc, preproc, concat}. Direct one-to-one mapping with the local session_out tree; no flattening or re-organization.

        **Q2 — File inclusion rule: identical to current tarball.** first_level_out/ full recursive; qc/ full recursive; preproc/ non-*.nii.gz only; concat/ non-*.nii.gz only. The user explicitly rejected widening the file set: the existing exclusion of intermediate BOLD NIfTIs (re-derivable from fMRIPrep) remains in force.

        **Q3 — Upload-complete sentinel: single zero-byte (or trivial-JSON) marker object.** Path: s3://{bucket}/{upload_prefix}/sub-{ID}/ses-{NN}A/_COMPLETE. Written last on full success only. Must be deleted before any re-attempt of an interrupted session so the sentinel never lies during a retry window.

        **Q4 — Parallelism mechanism: concurrent.futures.ThreadPoolExecutor.** max_workers=8 wrapping s3_client.upload_file. Worker count exposed as s3.upload_max_workers in the orchestrator config (default 8). Boto3 Session/Client is thread-safe; no shared state between workers beyond the client.

        **Q5 — Per-file verification: batched list_objects_v2.** After the worker pool drains, one list_objects_v2 call on the session prefix returns the full remote inventory (sizes + ETags); compare against the locally-built expected map; on any missing key or size mismatch, raise OrchestratorError with the failed subset attached and abort before writing the sentinel.

        **Q6 — Retry / resume semantics: idempotent resume + boto3 standard retry mode.** Within a pass: boto3's default retry config handles transient errors; first per-file failure to clear boto3's retries surfaces to the orchestrator as an exception and aborts the pass. Across invocations: on entry, head_object the sentinel; if present and force_recompute=False, skip the session; otherwise delete the sentinel, run list_objects_v2 to compute the remote inventory, set-diff against the local expected map, upload only the gap. No orchestrator-layer retry on top of boto3.

        **Q7 — Cleanup trigger: sentinel-write success.** cleanup_local_inputs and the session_out/extracted-dir cleanup at orchestrate_first_level.py:786-796 are gated on successful sentinel write. Local input cleanup contract unchanged. Stale local tarballs from prior code paths are left alone (operator's call). Identical cleanup behavior for FRESH and MIGRATE routing paths.

        **Q8 — Self-migration of legacy on-S3 archives.** Detection-and-routing at session entry:
          1. If _COMPLETE exists and force_recompute=False: SKIP (no reprocess, no migrate).
          2. If _COMPLETE exists and force_recompute=True: delete sentinel, take FULL path.
          3. If first_level_out.tar.gz exists and is intact (Q8b-defined criteria): MIGRATE — download tarball, extract to staging, map arcnames {prefix}_{first_level_out,qc,preproc,concat}/ back to canonical local subdirs, hand off to the per-file upload routine, write sentinel, delete the legacy tarball from S3 (Q8a).
          4. If first_level_out.tar.gz exists but extraction fails or the extracted file set fails sanity checks: silent fall-through to FULL processing (Q8b). Rationale: if reprocessing also fails, the failure surfaces a participant-level data issue; if it succeeds, the system self-heals.
          5. If neither sentinel nor tarball: FULL processing.
        No operator override flag (Q8c implicit): the autonomous routing handles all cases.

        **Q8d — QC/log provenance for migrated/skipped sessions.** The per-session orchestrator QC JSON (qc/sub-{ID}_{ses_label}_orchestrator_qc.json) preserves identical schema across all three routing paths so downstream cohort aggregators see uniform data. Behavior matrix:
          - FRESH: writes consolidated session QC JSON normally (current behavior).
          - MIGRATE: extracts the original QC JSON from the legacy tarball verbatim, adds one top-level "migration" key carrying {"status": "migrated_from_archive", "source_tarball_etag": "<ETag>", "migration_timestamp_utc": "<ISO8601>", "orchestrator_version_at_migration": "<x.y.z>"}, uploads to canonical new-layout key. Per-analysis upstream QC summaries (first_level_out/{analysis}/{prefix}_qc_summary.json) preserved verbatim with no modification; session-level migration key is the authoritative provenance record.
          - SKIP: nothing written; original QC JSON already at canonical new-layout key.
        Logging behavior: every session entry emits a routing-decision banner (FRESH/MIGRATE/SKIP) with the relevant remote artifact metadata (sentinel timestamp on SKIP; source tarball ETag and file count on MIGRATE).
      </decision>
    </topic>

  </topics>

  <action_items>

    <item priority="P1" target_mode="implement" description="Add a new orchestrator_utils.py helper enumerate_upload_targets(session_out_dir, sub_id, session) that walks session_out/{first_level_out,qc,preproc,concat}/ and returns a list of (local_path, s3_key_suffix, size_bytes) tuples honoring the existing inclusion rules (preproc/ and concat/ exclude *.nii.gz)." />

    <item priority="P1" target_mode="implement" description="Replace upload_to_s3 (orchestrator_utils.py:507-566) with a new per-file upload routine upload_session_to_s3(s3_config, sub_id, session, session_out_dir, logger) that: (a) runs list_objects_v2 on the session prefix to build the remote inventory; (b) computes the upload gap via set-diff against enumerate_upload_targets output; (c) submits each gap-file to a ThreadPoolExecutor(max_workers=s3_config.get('upload_max_workers', 8)) wrapping s3_client.upload_file; (d) after the pool drains, re-runs list_objects_v2 and verifies full key/size match; (e) on verification success, writes the _COMPLETE sentinel object; (f) on any per-file failure or verification mismatch, raises OrchestratorError with the failed subset attached and does NOT write the sentinel." />

    <item priority="P1" target_mode="implement" description="Add helpers check_session_complete(s3_config, sub_id, session) returning True iff the _COMPLETE sentinel exists, and delete_session_sentinel(s3_config, sub_id, session) used at start of any re-attempt." />

    <item priority="P1" target_mode="implement" description="Add a new routing helper determine_session_routing(s3_config, sub_id, session, force_recompute, logger) returning one of {'SKIP', 'MIGRATE', 'FULL'} per the Q8 decision tree: sentinel-exists-and-not-forcing → SKIP; sentinel-exists-and-forcing → FULL (after sentinel deletion); legacy-tarball-exists-and-intact → MIGRATE; legacy-tarball-exists-and-corrupt → FULL; neither → FULL. 'Intact' means: tarball downloads cleanly, extracts without tarfile.TarError, and the extracted top-level arcname structure matches {prefix}_first_level_out/, {prefix}_qc/, optionally {prefix}_preproc/ and {prefix}_concat/." />

    <item priority="P1" target_mode="implement" description="Add migrate_session_from_archive(s3_config, sub_id, session, session_out_dir, logger): download legacy first_level_out.tar.gz to session_out_dir/legacy_archive.tar.gz, extract to a staging directory, walk the extracted tree and stage files under canonical local subdirs (session_out_dir/{first_level_out,qc,preproc,concat}/) by stripping the {prefix}_ arcname prefix, then delegate to upload_session_to_s3, then add the migration provenance key to the staged session QC JSON BEFORE upload, then on verification+sentinel success delete the legacy tarball object from S3, then cleanup local intermediates per the existing cleanup_after_upload contract." />

    <item priority="P1" target_mode="implement" description="Refactor _process_session in orchestrate_first_level.py (L279-811): replace the unconditional Step 13a/13b/13c block (L778-796) with a routing-aware dispatch. At session entry (before Step 1 download), call determine_session_routing and branch: SKIP returns early after a banner log; MIGRATE invokes migrate_session_from_archive and returns after a banner log; FULL proceeds through the existing Steps 1-13. Each routing path logs a clear banner identifying the path taken and the relevant remote artifact metadata (sentinel timestamp for SKIP; source tarball ETag and file count for MIGRATE)." />

    <item priority="P1" target_mode="implement" description="Add the migration provenance key insertion to the staged session QC JSON during the MIGRATE path. The key is a single top-level 'migration' object: {'status': 'migrated_from_archive', 'source_tarball_etag': '<head_object ETag>', 'migration_timestamp_utc': '<datetime.now(timezone.utc).isoformat()>', 'orchestrator_version_at_migration': '<resolve from package metadata or _orch_ver constant>'}. Insertion happens after extraction, before upload, so the uploaded QC JSON carries the provenance." />

    <item priority="P1" target_mode="implement" description="Remove compress_session_outputs (orchestrator_utils.py:2973-3062) entirely; it has no remaining caller after the routing refactor. The cleanup_local_inputs helper at orchestrator_utils.py:3065+ stays unchanged." />

    <item priority="P1" target_mode="implement" description="Update orchestrator config schema (load_orchestrator_config and its setdefault block at orchestrator_utils.py:2832): add s3.upload_max_workers (default 8, type=int, range [1, 64]). Update validate_proc_template and the README/INPUT_SPECIFICATION accordingly." />

    <item priority="P2" target_mode="implement" description="Update example_orch_config.yaml (if one exists) and any operator-facing docs that reference first_level_out.tar.gz to document the new per-file layout, the _COMPLETE sentinel, and the auto-migration behavior." />

    <item priority="P1" target_mode="test" description="Design unit tests for enumerate_upload_targets covering: (a) all four subdirs present; (b) preproc/ and concat/ *.nii.gz exclusion preserved; (c) missing subdirs handled gracefully; (d) empty subdirs handled gracefully." />

    <item priority="P1" target_mode="test" description="Design unit tests for determine_session_routing covering all five routing branches (Q8 routing table); use moto or boto3 stubber to fake S3 head_object responses and tarball-presence permutations." />

    <item priority="P1" target_mode="test" description="Design integration tests for upload_session_to_s3 covering: (a) full-success path with sentinel written and verification passing; (b) per-file mid-batch failure surfacing as OrchestratorError without sentinel; (c) idempotent resume on re-entry (partial pre-existing remote state, set-diff narrows upload to gap); (d) force_recompute=True deletes sentinel and re-uploads everything; (e) verification mismatch (file uploaded but ContentLength differs) raises before sentinel write." />

    <item priority="P1" target_mode="test" description="Design integration tests for migrate_session_from_archive covering: (a) intact legacy tarball end-to-end migration with QC JSON migration key inserted, legacy tarball deleted from S3, sentinel written; (b) corrupt tarball (tarfile.TarError on extract) falls through to FULL path without raising; (c) intact tarball but missing required arcnames falls through to FULL path; (d) partial-migration interruption (sentinel absent, some files uploaded) resumes correctly on re-entry via set-diff; (e) per-analysis upstream qc_summary.json preserved verbatim (byte-equivalent to extracted content) post-upload." />

    <item priority="P2" target_mode="cr" description="Critical review of the routing decision tree and migrate path for chaos-theoretic 'trivial assumption' risks: (a) tarball arcname structure assumption — what if a prior orchestrator version used a different arcname convention? (b) sentinel-object name collision risk with any pre-existing _COMPLETE keys in the bucket; (c) edge case where force_recompute=True is set mid-migration of a partially-migrated session." />

  </action_items>

  <next_steps>
    Recommended downstream mode: /implement plan. The locked decision space is dense enough to warrant a formal implementation plan before code changes; the plan submodule will surface any remaining ambiguities in helper signatures, error-class boundaries, and test-fixture scope. After plan approval, /implement build executes the changes; /test design surfaces the formal test suite extensions specified in the action items above; /document refreshes README/INPUT_SPECIFICATION/AID_LOG to reflect the new upload contract, sentinel semantics, and migration behavior; /publish promotes the cycle to the GitHub repo. The N=30 cohort already published in commit 5861ad8 is intentionally NOT reprocessed by this change — the auto-migration path rehosts those outputs in the new layout on the next orchestrator run that touches them, with no numerical re-derivation.
  </next_steps>
</brainstorm_report>
