<clean_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="clean" timestamp="2026-10-28T13:44:35Z" />
  <scope>Published codebase: orchestrator_utils.py, orchestrate_first_level.py, run_orchestrator.py, environment.yaml, example_orchestrator_config.yaml, example_proc_config.yaml, README.md, INPUT_SPECIFICATION.md, AID_LOG.md, .gitignore and .aid/. Local-only: tests/ (git-ignored). Review focus: unused or dead code, stale comments and docstrings, comment blocks over 3 lines, phase/step narration, stale citations, project- or session-specific markers, inefficient implementations, and anything inappropriate to publish. Every finding below was verified by an independent proposal-verification agent before it was presented, and every disposition is the user's explicit decision.</scope>
  <research_conducted>No external literature research was required. Evidence came from direct code reads, git grep and AST scans of tracked files, git check-ignore runs against the proposed .gitignore, git ls-remote against the upstream repository, and a memory profile of the carpet-plot voxel extraction on a synthetic 2 mm MNI152NLin2009cAsym grid (97x115x97, 145,904-voxel mask, 40 frames) using AFNI 3dmaskdump and 3dBrickStat.</research_conducted>
  <metrics>
    <loc>5003 (orchestrator_utils.py 3343, orchestrate_first_level.py 1161, run_orchestrator.py 499)</loc>
    <files>48 tracked files</files>
    <avg_complexity>Manual AST estimate (radon unavailable): 56 functions, mean cyclomatic complexity about 13; maximum about 106 in orchestrate_first_level.py _process_session. No excessive Big-O complexity was found.</avg_complexity>
  </metrics>
  <findings>
    <finding id="F2" severity="major" category="maintainability">
      <location file=".gitignore" lines="29-36" />
      <description>Ten untracked session reports in the repository root were not covered by .gitignore and could be committed accidentally.</description>
      <current>Narrow per-file patterns that miss several report families.</current>
      <proposed>Replace lines 29-36 with: /ABCD_fmri_orchestrator_S3_*.md, /fmri[-_]first[-_]level[-_]proc[-_]*, /*TODO*, /.aid/reports/*TODO*, /.aid/reports/*_upstream_*, /.aid/reports/fmri[-_]first[-_]level[-_]proc[-_]*. Add .pytest_cache/. Verified with git check-ignore --no-index: all 10 untracked root reports are matched, none is tracked, no published file is newly ignored, and tests/ remains ignored.</proposed>
      <literature>n/a</literature>
      <impact>Prevents publication of session reports, TODO files and upstream-project reports.</impact>
    </finding>
    <finding id="F3" severity="major" category="maintainability">
      <location file="README.md; example_orchestrator_config.yaml; orchestrator_utils.py; .gitignore" lines="README 340; example config 240; orchestrator_utils 605" />
      <description>Production S3 bucket names appear in published files.</description>
      <current>Real bucket names in the README, the example config and a code comment; anchored config patterns in .gitignore.</current>
      <proposed>Replace with your-bucket-name in README.md:340 and example_orchestrator_config.yaml:240. End the orchestrator_utils.py:605 comment at "naming convention." Make the orchestrator/proc config patterns in .gitignore unanchored. History is scrubbed via F1.</proposed>
      <literature>n/a</literature>
      <impact>Removes infrastructure identifiers from the published code.</impact>
    </finding>
    <finding id="F4" severity="major" category="correctness">
      <location file="example_orchestrator_config.yaml" lines="all" />
      <description>The example orchestrator config uses retired keys and fails current validation.</description>
      <current>Stale configuration that does not load under load_orchestrator_config.</current>
      <proposed>Regenerate it to mirror the current production orchestrator config with: bucket your-bucket-name, input_dir /path/to/input, output_dir /path/to/output, upload_max_workers 8 with a comment giving the default and the 1-64 range. No bucket names or dates in comments. The Usage block names both example files (example_orchestrator_config.yaml, example_proc_config.yaml). Comments are at most 3 lines, contain no em-dashes and no step/phase wording. /test adds a test that the example loads.</proposed>
      <literature>n/a</literature>
      <impact>The shipped example becomes a valid starting point.</impact>
    </finding>
    <finding id="F5" severity="minor" category="maintainability">
      <location file="example_proc_config.yaml" lines="all" />
      <description>Stale remove_previous comments and singular CSF_path/WM_path keys.</description>
      <current>Example diverges from the authoritative production proc config.</current>
      <proposed>Mirror the production proc config. template_path becomes /path/to/Schaefer2018_400Parcels_7Networks_order_Tian_Subcortex_S4_3T_MNI152NLin2009cAsym_2mm.nii.gz. The header omits the bucket and points to the upstream INPUT_SPECIFICATION. Remove the "forced to true" comments. Usage lines reference example_proc_config.yaml (see F16).</proposed>
      <literature>n/a</literature>
      <impact>Example matches the proc config contract.</impact>
    </finding>
    <finding id="F6" severity="major" category="maintainability">
      <location file="README.md; INPUT_SPECIFICATION.md" lines="all" />
      <description>Both documents describe the retired archive-based preprocessing pipeline and list removed functions.</description>
      <current>Stale pipeline outline (numbered Step 0-13), letter-indexed function table, retired keys, placeholder participant IDs in an NDAR-like format, em-dashes.</current>
      <proposed>Full /document rewrite from current code: no retired-pipeline, migration or retired-key content; upstream version gate &gt;=4.0.0,&lt;5.0.0; generic producer naming; hyphenated upstream URL (https://github.com/tjkeding/fmri-first-level-proc); no bucket names; no participant references; placeholder IDs in SUBJ01 style; no em-dashes; example filename example_proc_config.yaml; a plain ordered pipeline overview with descriptive headings and no "Step N" labels; any function grouping uses the descriptive banner titles from F18 (no section letters); PRIMARY_OUTPUT_SUFFIX dropped from any function table; a License section (F24).</proposed>
      <literature>n/a</literature>
      <impact>Published documentation matches the code.</impact>
    </finding>
    <finding id="F7" severity="major" category="dependencies">
      <location file="environment.yaml" lines="pip git URL" />
      <description>environment.yaml pins the upstream package at a tag that does not exist, while the code requires &gt;=4.0.0,&lt;5.0.0.</description>
      <current>Pin @v3.2.0; upstream tags currently end at v3.1.0.</current>
      <proposed>DEFERRED by the user until the upstream repository publishes v4.0.0. Planned change: git+https://github.com/tjkeding/fmri-first-level-proc.git@v4.0.0 with a comment stating the &gt;=4.0.0,&lt;5.0.0 window. /publish precondition: git ls-remote --tags confirms the v4.0.0 tag exists. scipy stays out of environment.yaml (test-only).</proposed>
      <literature>n/a</literature>
      <impact>Reproducible environment once the upstream tag exists.</impact>
    </finding>
    <finding id="F8" severity="style" category="redundancy">
      <location file="orchestrator_utils.py" lines="25" />
      <description>Unused import shutil.</description>
      <current>import shutil (never referenced in this module).</current>
      <proposed>Delete it. Keep the import in orchestrate_first_level.py, which uses shutil.rmtree.</proposed>
      <literature>n/a</literature>
      <impact>Removes dead code.</impact>
    </finding>
    <finding id="F9" severity="minor" category="maintainability">
      <location file="orchestrator_utils.py" lines="upload_session_to_s3" />
      <description>The local upload counter is computed but never reported at a useful cadence.</description>
      <current>Counter incremented with debug-only logging.</current>
      <proposed>Loop with for fut in as_completed(future_to_target):. Before the loop compute step = max(1, math.ceil(len(upload_list) / 10)). After n_uploaded += 1, log INFO "%d/%d uploaded (sub-%s ses-%s)" when n_uploaded % step == 0 or n_uploaded == len(upload_list); otherwise keep the existing debug line. Return dict unchanged. /test adds cadence tests for 1, 10, 25 and 200 targets plus a failure case.</proposed>
      <literature>n/a</literature>
      <impact>About ten progress lines per session upload.</impact>
    </finding>
    <finding id="F10" severity="style" category="redundancy">
      <location file="orchestrate_first_level.py" lines="165" />
      <description>Dead local qc_cfg in process_participant.</description>
      <current>Assigned, never read.</current>
      <proposed>Delete line 165. Keep the qc_cfg assignment in _process_session (line 372).</proposed>
      <literature>n/a</literature>
      <impact>Removes dead code.</impact>
    </finding>
    <finding id="F11" severity="minor" category="redundancy">
      <location file="run_orchestrator.py" lines="207, 209, 220, 364-365" />
      <description>The n_skipped counter is initialized and displayed but never incremented.</description>
      <current>Progress line always shows "0 skipped".</current>
      <proposed>Remove n_skipped from the state dict (364-365), from _progress_loop (207, 209) and from the progress text (220). The matching test-fixture key is removed under F23. No live cancelled counter is added.</proposed>
      <literature>n/a</literature>
      <impact>Progress line reports only real outcomes.</impact>
    </finding>
    <finding id="F12" severity="minor" category="redundancy">
      <location file="orchestrator_utils.py" lines="about 2197-2203" />
      <description>PRIMARY_OUTPUT_SUFFIX is referenced only by tests and the README.</description>
      <current>Module-level dict built from upstream frozen_primary at import time; production uses frozen_primary directly.</current>
      <proposed>/implement deletes PRIMARY_OUTPUT_SUFFIX and its 3-line comment. /test replaces the imports and uses in tests/conftest.py (40, 494 docstring, 508), test_simulated_pipeline.py (35) and test_coverage_gaps.py (61, 600) with fmri_first_level_proc.names.frozen_primary(analysis_type, out_file_pre), and adds a contract test that frozen_primary resolves for task_act, task_conn and rest_conn. /test must run immediately after /implement.</proposed>
      <literature>n/a</literature>
      <impact>Removes a test-only symbol from production code.</impact>
    </finding>
    <finding id="F13" severity="minor" category="redundancy">
      <location file="orchestrator_utils.py; orchestrate_first_level.py" lines="see proposed" />
      <description>Four functions accept parameters they never read; three loops bind names that are never read.</description>
      <current>compute_preproc_qc space (2048), build_first_level_config task_defs (2719), validate_task_timing logger (1807), get_peak_memory_usage logger (2361); dead loop names at orchestrator_utils.py 174, 745 and 981.</current>
      <proposed>Remove the four parameters and their docstring entries. Update production calls at orchestrate_first_level.py 577, 654, 835 and orchestrator_utils.py 2558. Delete the now-dead space = study["space"] at orchestrate_first_level.py:377 (study.space stays required; it is used at orchestrator_utils.py 723 and 769). Rename dirs to _dirs at 174 and 745; use for files in runs.values(): at 981. /test updates 24 test call sites and fixes test_motion_and_qc.py 188-197 to pass 9 positional arguments plus anat_mask_path. /test must run immediately after /implement.</proposed>
      <literature>n/a</literature>
      <impact>Signatures reflect actual inputs.</impact>
    </finding>
    <finding id="F14" severity="style" category="redundancy">
      <location file="orchestrator_utils.py" lines="27, 616" />
      <description>OrderedDict is unnecessary; dict preserves insertion order from Python 3.7 and the environment pins Python 3.14.</description>
      <current>INPUT_FILE_PATTERNS = OrderedDict([...]).</current>
      <proposed>Replace with a plain dict literal in the same order and delete the import at line 27.</proposed>
      <literature>Python language reference: dict insertion order is guaranteed from 3.7.</literature>
      <impact>Removes an unneeded import.</impact>
    </finding>
    <finding id="F15" severity="style" category="maintainability">
      <location file="orchestrate_first_level.py; orchestrator_utils.py" lines="481, 731; 997-998" />
      <description>Planning-report identifiers in published code refer to documents the reader cannot see.</description>
      <current>Parenthesized plan identifiers in two comments and one docstring.</current>
      <proposed>orchestrate_first_level.py:481 becomes "# Pre-exclude non-rest runs without events". :731 becomes "# Collect per-run files from the survivor record list". The orchestrator_utils.py:997-998 docstring sentence becomes "Events are NOT checked here; runs missing events are excluded per run during session processing."</proposed>
      <literature>n/a</literature>
      <impact>No dangling references in published code.</impact>
    </finding>
    <finding id="F16" severity="minor" category="maintainability">
      <location file="orchestrator_utils.py; orchestrate_first_level.py; run_orchestrator.py; example_proc_config.yaml" lines="see proposed" />
      <description>Stale header and docstring text.</description>
      <current>"shift/clip" wording; incomplete exit-code text; references to a nonexistent example_config.yaml; NDAR-like placeholder IDs; inconsistent Version and Last updated lines.</current>
      <proposed>(1) orchestrator_utils.py:1810: "post-onset shift/clip" becomes "post-onset shift and negative-onset drop". (2) orchestrate_first_level.py:17-18: "unexpected_error run exclusion" becomes "unexpected_error or producer_fd_inconsistent run exclusion" (this text moves into the --help epilog under F19). (3) example_config.yaml becomes example_proc_config.yaml at orchestrate_first_level.py:25, run_orchestrator.py 14, 256, 261, and example_proc_config.yaml 6-8. (4) The placeholder IDs at orchestrate_first_level.py:27 become SUBJ01 SUBJ02 SUBJ03, and the --subj_id help text at orchestrate_first_level.py:1054 becomes "e.g. SUBJ01". (5) Delete the Last updated lines (orchestrate_first_level.py:31, orchestrator_utils.py:14, run_orchestrator.py:36) and the Version lines (orchestrate_first_level.py:30, run_orchestrator.py:35); keep Author lines. The determine_session_routing "one head_object call" claim is handled by F22.</proposed>
      <literature>n/a</literature>
      <impact>Headers and docstrings match code and repository state.</impact>
    </finding>
    <finding id="F17" severity="style" category="maintainability">
      <location file="orchestrate_first_level.py; orchestrator_utils.py" lines="see proposed" />
      <description>Step/phase narration with inconsistent numbering (Step 0, 1, 4, 5, 6, 7; Steps 2-6; Phase 1-4; Step 1-4). User rule: no arbitrary markers for phases or steps; describe the step briefly instead.</description>
      <current>Numbered labels in comments, a banner, a docstring list and three log messages.</current>
      <proposed>Delete the "Step N: " / "Phase N: " prefixes at orchestrate_first_level.py 169, 438, 751, 962, 982, 993 and orchestrator_utils.py 751, 785, 824, 844, 1522, 1536, 1543, 1568, and "Steps 2-6: " at orchestrate_first_level.py:476, keeping the descriptive text. Log messages: orchestrate_first_level.py:171 "Discovering available sessions...", :467 "Downloading session inputs from S3...", orchestrator_utils.py:401 "Starting upload: sub-%s ses-%s, ..." (arguments unchanged). Replace the numbered "FULL pipeline steps" list in the _process_session docstring (orchestrate_first_level.py:359-367) with: "The FULL path downloads inputs (or uses local paths), preprocesses each task (NSS trim, QC, motion, tissue, timing), collects per-run files, builds the first-level config, runs the analyses, writes the session QC JSON, uploads outputs per file, sweeps stale S3 keys, writes the sentinel unless first-level analysis was skipped or the session failed unexplained, and cleans up local files." AFNI step() expressions in 3dcalc calls are code and stay.</proposed>
      <literature>n/a</literature>
      <impact>No misleading or arbitrary numbering.</impact>
    </finding>
    <finding id="F18" severity="style" category="maintainability">
      <location file="orchestrator_utils.py" lines="banner titles" />
      <description>Banner titles carry non-contiguous section letters (B, C, F, G, G2, H, K, K2-K5, L, M, N) and some titles do not describe their contents.</description>
      <current>Letter-prefixed and partially inaccurate titles.</current>
      <proposed>Retitle all 16 banners, keeping the rule lines: 65 "Exit code constants"; 90 "S3 routing, sentinel and upload"; 538 "Upstream version check"; 601 "Input discovery, completeness check and download"; 1083 "Non-steady-state TR detection and removal"; 1210 "Motion regressors and producer FD check"; 1463 "Tissue and global-signal regressors from masks"; 1604 "Task timing relabeling, conversion and validation"; 1895 "Preprocessing and first-level QC"; 2358 "Peak memory measurement"; 2395 "Outcome classification and exit codes"; 2501 "Session QC consolidation"; 2664 "AFNI unvalidated-version warning filter"; 2716 "First-level config building"; 2897 "Orchestrator config loading and proc-template validation"; 3312 "Local input cleanup".</proposed>
      <literature>n/a</literature>
      <impact>Descriptive navigation without arbitrary identifiers.</impact>
    </finding>
    <finding id="F19" severity="style" category="maintainability">
      <location file="orchestrator_utils.py; orchestrate_first_level.py; run_orchestrator.py" lines="headers; 604-608; 844-848; 424-429" />
      <description>Six comment blocks exceed 3 lines: three module headers (11, 28 and 33 lines) and three inline blocks.</description>
      <current>Long headers with usage, exit codes and input descriptions; verbose inline comments, one of which wrongly calls BOLD/brainmask filenames "bare".</current>
      <proposed>Headers become a title, one description line and the Author line. run_orchestrator.py: "Runs orchestrate_first_level.py over a subject list in a thread pool; see --help." with the subject-list format and exit codes (0/1/2/130) moved into the existing argparse epilog after the Examples. orchestrate_first_level.py: "Processes one participant one session at a time (download, preprocess, analyze, upload, cleanup) to bound local disk use; see --help." with a new RawDescriptionHelpFormatter epilog holding the exit codes and the parallel-invocation example. orchestrator_utils.py: "Helper functions called by orchestrate_first_level.py." (input details live in the README/INPUT_SPECIFICATION and the compute_tissue_mask_regressor docstring). Inline: orchestrator_utils.py:604-608 becomes "Input filename patterns: single edit point for the producer naming convention. / Session directories end in "A" (ses-00A); the captured session code is the / numeric part. BOLD and brainmask carry a space- entity and no desc- entity." orchestrator_utils.py:844-848 becomes "Producer completion gate: _complete.json is written last, so a session / directory without it is still being written and is skipped. Session / directories come from the listing itself; no path template is needed." orchestrate_first_level.py:427-429 becomes "S3 mode wipes existing local session output for a clean recompute; / local mode keeps it for force_recompute-governed caching."</proposed>
      <literature>n/a</literature>
      <impact>Concise comments; usage and exit codes reachable via --help.</impact>
    </finding>
    <finding id="F20" severity="style" category="maintainability">
      <location file="run_orchestrator.py; AID_LOG.md" lines="90, 370, 430; 14 occurrences" />
      <description>Em-dashes in published code and prose.</description>
      <current>Three in run_orchestrator.py, 14 in AID_LOG.md.</current>
      <proposed>run_orchestrator.py:90 becomes "'{s}', skipping."; :370 becomes "(daemon: auto-exits when the main thread ends)"; :430 becomes "Interrupted (Ctrl+C): cancelling pending jobs, ...". Replace each AID_LOG.md em-dash with a comma, colon, parentheses or sentence break as the sentence requires. README.md, INPUT_SPECIFICATION.md and example_orchestrator_config.yaml are covered by their rewrites (F6, F4). Archived reports under .aid/reports/ stay unchanged as historical records.</proposed>
      <literature>n/a</literature>
      <impact>Published prose follows the project style rule.</impact>
    </finding>
    <finding id="F21" severity="major" category="space_complexity">
      <location file="orchestrator_utils.py" lines="about 2006-2021 (generate_carpet_plot)" />
      <description>The carpet plot captures the entire 3dmaskdump text output in memory before keeping 2000 rows.</description>
      <current>subprocess.run(capture_output=True) then strip/split of the full output. Measured on a 145,904-voxel mask at 40 frames: about 8.9 bytes per value and a 202 MB peak-memory increase, which scales to roughly 1.8-1.9 GB per run at 350-380 frames and multiplies with concurrent participants.</current>
      <proposed>Obtain the mask voxel count with 3dBrickStat -count -non-zero; choose the kept row indices exactly as now (all rows if 2000 or fewer, else np.linspace(0, n - 1, 2000, dtype=int)); run 3dmaskdump with the same arguments via subprocess.Popen and keep only those rows while iterating stdout; treat a nonzero exit like the current CalledProcessError path (existing warning, return); if the number of rows read differs from the voxel count, log a warning and skip the plot. Everything from voxel_data onward is unchanged. Measured: 4.5 MB peak increase with an array identical to the current output (np.array_equal). /test adds a Popen-capable mock to tests/mock_subprocess.py and a mismatch-guard test; /test must run immediately after /implement.</proposed>
      <literature>AFNI 3dmaskdump and 3dBrickStat documentation.</literature>
      <impact>About 400x lower transient memory per run, same plot.</impact>
    </finding>
    <finding id="F22" severity="minor" category="performance">
      <location file="orchestrator_utils.py" lines="209-245, 276-338" />
      <description>Routing issues two head_object requests for one sentinel on the skip path, and the routing docstring misstates the exception type.</description>
      <current>check_session_complete returns a bool; determine_session_routing then repeats head_object to read LastModified. The docstring says OrchestratorError, but ClientError propagates.</current>
      <proposed>Rename check_session_complete to get_session_sentinel, returning the head_object response dict when present and None when absent (404/NoSuchKey), re-raising other ClientErrors; update its docstring and the import list. In determine_session_routing, read LastModified from that response; delete the second head_object call and the then-unused s3_client, bucket and sentinel_key locals. Keep "Performs one head_object call (the sentinel probe)"; change Raises to "botocore ClientError for S3 errors other than 404/NoSuchKey". /test adds an assertion that the skip path makes exactly one head_object call.</proposed>
      <literature>n/a</literature>
      <impact>One fewer S3 request per skipped session; accurate docstring.</impact>
    </finding>
    <finding id="F23" severity="style" category="redundancy">
      <location file="tests/ (local only)" lines="see proposed" />
      <description>Dead code and planning identifiers in the unpublished test suite.</description>
      <current>28 unused imports across 11 test modules, two uncalled helpers, a stale n_skipped fixture key, and about 250 planning-report identifiers in comments and docstrings.</current>
      <proposed>Under /test: delete the unused imports (test_coverage_gaps.py shutil, tempfile, compute_preproc_qc, compute_first_level_qc; test_cr_implementation.py copy, fix_nback_cue_labels, format_task_timing, save_qc_json; test_cr_implementation_extended.py re, copy, np, pd, extract_motion_regressors, save_qc_json, create_mock_bold_nifti, create_mock_confounds_tsv; test_events_validation.py yaml; test_motion_and_qc.py copy; test_qc_schema_v4.py patch; test_restart_safety.py OrchestratorError; test_run_orchestrator.py patch; test_simulated_pipeline.py create_mock_nback_events_tsv, create_mock_confounds_tsv, create_mock_bold_nifti, create_mock_mask_nifti, mock_subprocess_run; test_tissue_mask_regressors.py subprocess). Delete mock_data_utils.create_mock_rest_events_tsv and mock_subprocess._parse_args. Delete "n_skipped": 0 from _fresh_state() in test_run_orchestrator.py:40. Replace planning identifiers in test comments and docstrings with brief descriptions. Keep unused tuple-unpacking targets and TestRetiredSymbolsRemoved.</proposed>
      <literature>n/a</literature>
      <impact>Cleaner local test suite; no effect on collection.</impact>
    </finding>
    <finding id="F24" severity="major" category="maintainability">
      <location file="LICENSE (new); README.md" lines="n/a" />
      <description>The public repository has no license.</description>
      <current>No LICENSE file.</current>
      <proposed>Add an MIT LICENSE with "Copyright (c) 2026 Taylor J. Keding" and a License section in the README.</proposed>
      <literature>n/a</literature>
      <impact>Clear reuse terms.</impact>
    </finding>
    <finding id="F25" severity="minor" category="maintainability">
      <location file="orchestrate_first_level.py; orchestrator_utils.py" lines="10; 8, 601, 604, 1291" />
      <description>Code comments name the third-party preprocessing producer and its internal repository.</description>
      <current>Producer name in five code comments.</current>
      <proposed>Replace with generic producer wording (for example "the producer" or "producer per-file outputs"); the banner at 601 is retitled under F18. Substitute the name in reports at /document sync and /publish. The /publish gate adds case-insensitive terms for the producer name, its repository owner's handle and its repository URL, and the security-gate agent brief is extended to cover third-party repositories and owners.</proposed>
      <literature>n/a</literature>
      <impact>No third-party internal names in published material.</impact>
    </finding>
  </findings>
  <summary>
    <critical_count>0</critical_count>
    <major_count>8</major_count>
    <total_findings>24</total_findings>
    <overall_assessment>needs_minor_work</overall_assessment>
  </summary>
  <action_items>
    <item priority="P0" target_mode="implement" finding_ref="F2" description="Apply the revised .gitignore patterns and add .pytest_cache/." />
    <item priority="P0" target_mode="implement" finding_ref="F3" description="Replace bucket names in README, example config and code comment; unanchor config patterns." />
    <item priority="P1" target_mode="implement" finding_ref="F24" description="Add MIT LICENSE." />
    <item priority="P1" target_mode="implement" finding_ref="F4" description="Regenerate example_orchestrator_config.yaml." />
    <item priority="P1" target_mode="implement" finding_ref="F5" description="Regenerate example_proc_config.yaml." />
    <item priority="P1" target_mode="implement" finding_ref="F21" description="Stream 3dmaskdump output in generate_carpet_plot with the row-count guard." />
    <item priority="P1" target_mode="implement" finding_ref="F22" description="Single head_object routing via get_session_sentinel; docstring fixes." />
    <item priority="P1" target_mode="implement" finding_ref="F25" description="Replace the producer name in five code comments." />
    <item priority="P2" target_mode="implement" finding_ref="F8" description="Delete unused shutil import." />
    <item priority="P2" target_mode="implement" finding_ref="F9" description="Upload progress logging at about 10 percent cadence." />
    <item priority="P2" target_mode="implement" finding_ref="F10" description="Delete dead qc_cfg in process_participant." />
    <item priority="P2" target_mode="implement" finding_ref="F11" description="Remove the n_skipped counter." />
    <item priority="P2" target_mode="implement" finding_ref="F12" description="Delete PRIMARY_OUTPUT_SUFFIX." />
    <item priority="P2" target_mode="implement" finding_ref="F13" description="Remove unused parameters and dead loop bindings; update callers." />
    <item priority="P2" target_mode="implement" finding_ref="F14" description="Replace OrderedDict with dict." />
    <item priority="P2" target_mode="implement" finding_ref="F15" description="Remove planning identifiers from code comments and docstring." />
    <item priority="P2" target_mode="implement" finding_ref="F16" description="Fix stale header and docstring text; SUBJ01-style placeholders; delete Version/Last updated lines." />
    <item priority="P2" target_mode="implement" finding_ref="F17" description="Remove step/phase wording from comments, docstring and log messages." />
    <item priority="P2" target_mode="implement" finding_ref="F18" description="Retitle the 16 banners descriptively." />
    <item priority="P2" target_mode="implement" finding_ref="F19" description="Shorten long comment blocks; move usage and exit codes into argparse epilogs." />
    <item priority="P2" target_mode="implement" finding_ref="F20" description="Replace em-dashes in run_orchestrator.py and AID_LOG.md." />
    <item priority="P1" target_mode="test" finding_ref="F12" description="Immediately after /implement: replace PRIMARY_OUTPUT_SUFFIX uses with frozen_primary; add the frozen_primary contract test." />
    <item priority="P1" target_mode="test" finding_ref="F13" description="Immediately after /implement: update 24 call sites; fix test_motion_and_qc.py 188-197." />
    <item priority="P1" target_mode="test" finding_ref="F21" description="Immediately after /implement: Popen-capable 3dmaskdump mock and mismatch-guard test." />
    <item priority="P2" target_mode="test" finding_ref="F4" description="Test that example_orchestrator_config.yaml loads." />
    <item priority="P2" target_mode="test" finding_ref="F9" description="Upload progress cadence tests (1, 10, 25, 200 targets, failure case)." />
    <item priority="P2" target_mode="test" finding_ref="F22" description="Assert exactly one head_object call on the skip path." />
    <item priority="P2" target_mode="test" finding_ref="F23" description="Remove test dead code and planning identifiers." />
    <item priority="P1" target_mode="document" finding_ref="F6" description="Full README.md and INPUT_SPECIFICATION.md rewrite under the constraints listed in F6." />
    <item priority="P1" target_mode="publish" finding_ref="F7" description="Deferred: update the upstream pin to @v4.0.0 only after git ls-remote --tags confirms the tag exists." />
  </action_items>
</clean_report>

Notes outside the schema:

Ordering. /implement must be followed immediately by /test for F12, F13 and F21, because those production changes break existing tests until the tests are updated.

Upstream handoff, recorded here and to be handled in the upstream project's own session: the upstream repository has no LICENSE file.
