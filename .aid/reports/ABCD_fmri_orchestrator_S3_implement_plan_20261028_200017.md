<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-10-28T20:00:17Z" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_document_20261028_194450.md" mode="document" key_items="3" />
  </input_reports>

  <!-- Resolved decisions (user, 2026-10-28):
       Manifest handling: validate producer manifests only for the sessions that will be processed;
         without --session, behavior is unchanged (an invalid manifest in any session stops the participant).
       Local mode: removed. The orchestrator is S3-only; the s3 section and s3.enabled: true are required.
         This supersedes the report's local-walk fix. The per-function intermediate-file caching that only
         local mode could exercise is removed; study.force_recompute keeps one meaning (delete the sentinel
         and recompute).
       The report's third item (test_s3_enabled_with_v7_templates bucket assertion) targets /test and is not
         part of this build. -->

  <changes>

    <change id="C1" priority="P1" source_item="document report action item 1 (manifest scope)">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Validate producer completion manifests only for the session selected with --session, so an invalid manifest in an unrequested session no longer stops the participant.</description>
      <spec>
orchestrator_utils.py, discover_subject_inputs:
1. Signature becomes discover_subject_inputs(study_config, s3_config, sub_id, task_defs, logger, session_filter=None).
2. After the discovery structure is built from the parsed filenames (after the loop that assigns non-BOLD run files to BOLD slots) and before the producer completion gate:
   - logger.info("Discovery: sessions with input files: %s", sorted(result)).
   - If session_filter is not None: result = {s: v for s, v in result.items() if s == session_filter}. Sessions removed here are never read or validated.
3. The completion gate then runs unchanged over the remaining sessions.
4. Docstring: add session_filter to Parameters ("str or None. When set, only this session code is kept, and only its manifest is read and validated.") and add one sentence to the description stating that other sessions' manifests are not read when a filter is set.

orchestrate_first_level.py, process_participant:
5. Call discover_subject_inputs(study, s3_cfg, sub_id, task_defs, logger, session_filter=session_filter).
6. Replace the empty-result check and the subsequent "Apply session filter" block with:
   if not sessions:
       if session_filter is not None:
           raise OrchestratorError(
               f"Session filter '{session_filter}' not found among sessions with a "
               f"producer completion manifest for sub-{sub_id}."
           )
       raise OrchestratorError(<existing "No sessions discovered" message, unchanged>)
   if session_filter is not None:
       logger.info("Session filter applied: processing only session %s", session_filter)
7. The input-completeness gate keeps operating on the selected sessions: remove selected_discovered and pass discovered directly to check_subject_input_completeness.

orchestrate_first_level.py, _process_session:
8. In the discovered_session is None fallback, call discover_subject_inputs(..., session_filter=session).
      </spec>
      <dependencies>C2 (both edit discover_subject_inputs; apply C2 first so the S3-only body is the base)</dependencies>
      <risk>low - one optional keyword argument with a backward-compatible default; behavior without --session is unchanged. Tests asserting the old "not found in available sessions" message need re-expression in /test.</risk>
      <rollback>Revert the two files.</rollback>
    </change>

    <change id="C2" priority="P1" source_item="document report action item 2 (local walk), resolved as local-mode removal">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Make the orchestrator S3-only: require the s3 section with enabled: true and remove every local-mode branch.</description>
      <spec>
orchestrator_utils.py, load_orchestrator_config:
1. required_sections becomes ["study", "tasks", "analyses", "s3"].
2. After the retired-s3-key check: if not isinstance(s3_cfg, dict) or s3_cfg.get("enabled") is not True, raise OrchestratorError("s3.enabled must be true: this orchestrator reads its inputs from S3 and writes its outputs to S3.").
3. The required-field, bucket, prefix-slash and template-placeholder checks run unconditionally (remove the `if s3_cfg.get("enabled", False):` wrapper and dedent). Required-field message becomes f"Required field 's3.{field}' is null or missing.".
4. Remove config.setdefault("s3", {"enabled": False}); keep the cleanup_after_upload and upload_max_workers defaults and validation.
5. Docstring: add that the s3 section is required and s3.enabled must be true.

orchestrator_utils.py, _read_completion_manifest:
6. Read only from S3 (get_object); remove the local open() branch. Docstring: "Load a producer _complete.json from its S3 key." Catch (ClientError, ValueError) only; remove OSError from the tuple.

orchestrator_utils.py, discover_subject_inputs:
7. Remove the local-mode os.walk branch; the S3 paginated listing runs unconditionally. Update the docstring (single S3 listing) and the inline comment on candidate values (value is the S3 key).

orchestrate_first_level.py:
8. process_participant and _process_session: s3_cfg = config["s3"] (no default dict).
9. _process_session routing: call determine_session_routing unconditionally (remove the `if s3_cfg.get("enabled", False):` wrapper) and remove the routing = "full" / remote_metadata = {} pre-assignments; both are always assigned from routing_result, and nothing reads them earlier (the exception handler does not use them).
10. Session output wipe: `if os.path.isdir(session_out):` (no s3 condition). Replace the two-line comment with one line: "# Recompute from an empty local session output directory."
11. Inputs: always download (remove the else branch and its "S3 disabled; using local files" log line). Update the section banner comment to "Resolve session inputs (download from S3)".
12. Upload block: remove the `if s3_cfg.get("enabled", False):` wrapper and dedent its body.
13. Exception handler: condition becomes `if s3_cfg.get("cleanup_after_upload", True):`; comment "Clean up downloaded files on error".
14. _process_session docstring: drop "(S3 only)" and "(or uses local paths)".
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - removes a mode used by 7 local test files; production behavior (S3) is unchanged. /test must re-express those tests onto the existing S3 client mocks.</risk>
      <rollback>Revert the two files.</rollback>
    </change>

    <change id="C3" priority="P1" source_item="consequence of local-mode removal (user decision)">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Remove the reuse-if-exists intermediate-file caching and its force_recompute parameters. In S3-only operation the session output directory is always wiped before processing, so these branches can never run.</description>
      <spec>
orchestrator_utils.py:
1. remove_initial_trs_bold(bold_path, n_remove, out_dir, out_prefix, logger): remove the force_recompute parameter, the "already exists" early return and the force_recompute removal block. Docstring: delete the "Idempotent: ..." sentence and the force_recompute parameter entry.
2. extract_motion_regressors(confounds_path, n_remove, calc_n_motion_derivs, out_path, logger, expected_n_frames=None): remove force_recompute, the cache-hit early return and the removal block. Docstring: delete the force_recompute entry and "; it is also evaluated on a cache hit" from Returns.
3. compute_tissue_mask_regressor(trimmed_bold_path, brainmask_path, tissue_mask_path, out_path, logger): remove force_recompute, the cache-hit return and the removal block. Returns docstring: "(out_path, n_voxels). n_voxels is the post-intersection non-zero voxel count." (int, never None).
4. fix_nback_cue_labels(events_path, condition_column, out_path, logger): remove force_recompute and the "already exists" early return; docstring entry removed.
5. format_task_timing(events_path, condition_column, conditions_exclude, n_remove, TR, out_path, logger): remove force_recompute and the cache-hit return; Returns docstring drops "Cache hit returns (path, 0, 0)."

orchestrate_first_level.py, _process_session:
6. Drop force_recompute=... from the five call sites (remove_initial_trs_bold, extract_motion_regressors, the three compute_tissue_mask_regressor calls, fix_nback_cue_labels, format_task_timing).
7. The csf/wm/gs voxel counts are now always integers: replace each `if <x>_nvox is not None:` guard with the unconditional assignment.
8. The local force_recompute variable stays (routing only).

study.force_recompute validation is unchanged.
      </spec>
      <dependencies>C2</dependencies>
      <risk>low - the removed branches are unreachable in S3-only operation; five signatures change, so test call sites passing force_recompute need updating in /test.</risk>
      <rollback>Revert the two files.</rollback>
    </change>

    <change id="C4" priority="P1" source_item="documentation consistency for C1-C3">
      <file path="README.md" action="modify" />
      <file path="INPUT_SPECIFICATION.md" action="modify" />
      <file path="example_orchestrator_config.yaml" action="modify" />
      <description>Bring the published documentation and example config in line with S3-only operation and session-scoped manifest validation. Same style rules as the rewrite (no em-dashes, no step labels, generic producer naming, no bucket names).</description>
      <spec>
README.md:
1. Pipeline overview item 2: "Lists the participant's input files on S3 once and builds the session, task and run inventory from the filenames. A session is used only when ..." (drop "one walk of the local input directory").
2. Items 4 and 9: drop "In S3 mode, " and capitalize the next word.
3. Requirements AWS bullet: "AWS credentials (...) with read access ..." (drop "For S3 mode,").
4. Quick start dry-run comment: "(no downloads, no processing; S3 is still listed, so credentials are needed)".
5. Outputs paragraph: "The session is mirrored to s3://..." (drop "In S3 mode").
6. Design notes: replace the "S3 and local modes" bullet with "**S3 only.** Inputs are listed and downloaded from S3, each session is recomputed from an empty local output directory, and outputs are uploaded back to S3; `study.input_dir` and `study.output_dir` are local working directories."
7. Configuration table s3 row: start the Contents with "`enabled` (must be `true`)".

INPUT_SPECIFICATION.md:
8. Inputs at a glance: producer outputs row "Discovered on S3".
9. --skip-first-level row: "...; outputs are still uploaded".
10. study.input_dir: "Absolute path. The download root: each input S3 key is written to `{input_dir}/{key}`".
11. study.force_recompute: "An existing `_COMPLETE` sentinel is deleted and the session recomputed".
12. Orchestrator config intro: required sections `study`, `tasks`, `analyses`, `s3`; optional `qc`.
13. s3 table: enabled row Required "Yes", Default empty, Description "Must be `true`"; the bucket, func_prefix_template, anat_prefix_template and upload_prefix rows Required "Yes".
14. Validation table: "`study`, `tasks`, `analyses`, `s3` present"; new row "`s3.enabled` is true | `s3.enabled must be true: this orchestrator reads its inputs from S3 and writes its outputs to S3.`"; required-fields row "Required S3 fields | `Required field 's3.{field}' is null or missing.`".
15. Discovery paragraph: "Inputs are discovered with one paginated S3 listing per participant, from the prefix described under `s3`. When `--session` is given, only that session is kept, and only its manifest is read and validated. Every listed file is classified by its basename:".
16. Completion manifest bullet: "Any violation stops the participant with an error (exit 1). With `--session`, only the selected session's manifest is read; without it, every session's manifest is validated."
17. S3 routing section: delete "These steps apply in S3 mode only." and delete the "Local mode" paragraph.
18. preprocessing record: "`csf_mask_voxels`, `wm_mask_voxels`, `gs_mask_voxels` (voxels after intersection, when the regressor is computed)".
19. Environment AWS bullet: drop "(S3 mode)".
20. Edge cases: delete the "Local mode with many participants" and "Local-mode rerun with existing intermediates" rows; replace the invalid-manifest row with "| An invalid manifest in a session other than `--session` | Not read; only the selected session's manifest is validated |".

example_orchestrator_config.yaml:
21. input_dir comment: "# Local working directory; each input S3 key is downloaded to {input_dir}/{key}".
22. s3 section: add a comment above enabled: "# Required; must be true (inputs and outputs live on S3)".
      </spec>
      <dependencies>C1, C2, C3</dependencies>
      <risk>low - documentation and comments only</risk>
      <rollback>Revert the three files.</rollback>
    </change>

  </changes>

  <execution_order>C2, C3, C1, C4</execution_order>

  <!-- Handed to /test (not part of this build): re-express test_s3_enabled_with_v7_templates to expect bucket <bucket>;
       re-express the 7 local-mode test files onto the existing S3 client mocks; update call sites that pass
       force_recompute to the five preprocessing functions; update tests asserting the old --session not-found
       message; add tests for session-scoped manifest validation and for the s3.enabled-must-be-true error. -->
</implement_plan>
