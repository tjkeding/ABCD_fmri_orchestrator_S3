<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-10-28T13:51:53Z" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_clean_20261028_094435.md" mode="clean" key_items="24" />
  </input_reports>

  <!-- ============================================================ -->
  <!-- SCOPE NOTES                                                   -->
  <!--                                                               -->
  <!-- F6 (README/INPUT_SPEC rewrite) and F7 (env pin) are deferred -->
  <!--   to /document and /publish respectively. F4/F5 (example      -->
  <!--   configs) are full file rewrites derived from the production -->
  <!--   configs; they are included here.                            -->
  <!--                                                               -->
  <!-- F23 (test dead code) is deferred to /test.                    -->
  <!--                                                               -->
  <!-- /test must run immediately after this build for F12, F13 and  -->
  <!--   F21 (production signature changes that break test calls).   -->
  <!-- ============================================================ -->

  <changes>

    <!-- ============================================================ -->
    <!-- C1: .gitignore (F2 + F3 patterns)                            -->
    <!-- ============================================================ -->
    <change id="C1" priority="P0" source_item="F2, F3">
      <file path=".gitignore" action="modify" />
      <description>Replace lines 17-20 (anchored orch/proc config patterns) with unanchored orch_config_*.yaml and proc_config_*.yaml. Replace lines 29-36 (narrow per-file report patterns) with the six accepted glob patterns. Add .pytest_cache/ immediately after the tests/ line.</description>
      <spec>
The final .gitignore keeps the universal block (lines 1-16: __pycache__/, *.pyc, *.pyo, .DS_Store, ._, *.egg-info/, dist/, build/, .eggs/, *.log, CLAUDE.md, ABCD_S3bucket_file_struct.md, first_level_todo.txt, fmriprep_confounds_file_example.tsv, raw_events_file_example.tsv, raw_motion_file_example.tsv), then the unanchored config patterns (orch_config_*.yaml, proc_config_*.yaml), then the local-only block (.Rhistory, tests/, .pytest_cache/, orchestrator_motion_changes_v2.4.0.md, REFACTOR_PLAN.md), then the six report-family patterns:
  /ABCD_fmri_orchestrator_S3_*.md
  /fmri[-_]first[-_]level[-_]proc[-_]*
  /*TODO*
  /.aid/reports/*TODO*
  /.aid/reports/*_upstream_*
  /.aid/reports/fmri[-_]first[-_]level[-_]proc[-_]*

Remove the six per-file report entries that the glob patterns now subsume.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - verified with git check-ignore in the clean review; no tracked file is newly ignored</risk>
      <rollback>git checkout -- .gitignore</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C3: MIT LICENSE (F24)                                        -->
    <!-- ============================================================ -->
    <change id="C3" priority="P1" source_item="F24">
      <file path="LICENSE" action="create" />
      <description>Create an MIT LICENSE file with "Copyright (c) 2026 Taylor J. Keding".</description>
      <spec>
Standard MIT license text. Year 2026. Copyright holder "Taylor J. Keding". No other modifications.
      </spec>
      <dependencies>none</dependencies>
      <risk>low</risk>
      <rollback>rm LICENSE</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C4: example_orchestrator_config.yaml (F4)                    -->
    <!-- ============================================================ -->
    <change id="C4" priority="P1" source_item="F4">
      <file path="example_orchestrator_config.yaml" action="modify" />
      <description>Full rewrite mirroring orch_config_production.yaml with sanitized values, no em-dashes, no step/phase wording, comments at most 3 lines per key.</description>
      <spec>
Structure mirrors orch_config_production.yaml key for key. Substitutions:
- s3.bucket: "your-bucket-name"
- study.input_dir: "/path/to/input"
- study.output_dir: "/path/to/output"
- s3.upload_max_workers: 8 with comment: "# Concurrent upload threads (default 8, range 1-64)."
- Header: title, one description line, Author line. Usage block names example_orchestrator_config.yaml and example_proc_config.yaml. No bucket names, no dates, no participant IDs, no em-dashes, no step/phase wording in comments.
- s3.func_prefix_template/anat_prefix_template: kept from production (generic S3 path templates with {subject}/{session} placeholders); no producer name.
- No fMRIPrep, mmps_mproc, archive, migration or retired-key content.
- Comments describe what each key does in at most 3 lines; no "steps" or "phases" language.
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - full file rewrite; /test adds a loading validation test</risk>
      <rollback>git checkout -- example_orchestrator_config.yaml</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C5: example_proc_config.yaml (F5)                            -->
    <!-- ============================================================ -->
    <change id="C5" priority="P1" source_item="F5">
      <file path="example_proc_config.yaml" action="modify" />
      <description>Full rewrite mirroring proc_config_production.yaml with sanitized paths and no "forced to true" comments.</description>
      <spec>
Structure mirrors proc_config_production.yaml. Substitutions:
- global.template_path: "/path/to/Schaefer2018_400Parcels_7Networks_order_Tian_Subcortex_S4_3T_MNI152NLin2009cAsym_2mm.nii.gz"
- Header: title, one description line referencing the upstream fmri-first-level-proc INPUT_SPECIFICATION. Author line. No bucket name.
- Remove "forced to true by the orchestrator" comments (remove_previous is just true).
- Usage lines: "fmri-first-level-proc --config example_proc_config.yaml --dry-run" (corrected filename).
- __ORCHESTRATOR_FILLED__ placeholders stay (that is how the orchestrator operates).
- No em-dashes. Comments at most 3 lines.
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - full file rewrite</risk>
      <rollback>git checkout -- example_proc_config.yaml</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C6: Producer name replacement (F25)                          -->
    <!-- ============================================================ -->
    <change id="C6" priority="P1" source_item="F25">
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="orchestrator_utils.py" action="modify" />
      <description>Replace the third-party producer name in five code comments with generic wording.</description>
      <spec>
orchestrate_first_level.py:10 "producer per-file outputs" -> "producer per-file outputs"
orchestrator_utils.py:8 "producer per-file outputs" -> "producer per-file outputs"
orchestrator_utils.py:601 "producer per-file layout" -> "per-file layout" (the banner title is retitled in C10)
orchestrator_utils.py:604 "producer per-file" -> "producer per-file"
orchestrator_utils.py:1291 "the producer producer" -> "the upstream producer"
      </spec>
      <dependencies>none</dependencies>
      <risk>low - comment-only changes</risk>
      <rollback>git checkout -- orchestrate_first_level.py orchestrator_utils.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C7: Unused import and OrderedDict (F8, F14)                  -->
    <!-- ============================================================ -->
    <change id="C7" priority="P2" source_item="F8, F14">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Delete unused import shutil (line 25) and from collections import OrderedDict (line 27). Replace INPUT_FILE_PATTERNS = OrderedDict([...]) (line 616) with a plain dict literal in the same order.</description>
      <spec>
Delete line: import shutil
Delete line: from collections import OrderedDict
Change line 616: INPUT_FILE_PATTERNS = OrderedDict([ -> INPUT_FILE_PATTERNS = {
Change the corresponding closing ]); -> }
Each entry changes from ("key", re.compile(...)), to "key": re.compile(...),
Keep the same key order: bold, brainmask, confounds, events, csf_mask, wm_mask.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - verified: no code uses OrderedDict-specific methods</risk>
      <rollback>git checkout -- orchestrator_utils.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C8: Dead code removal (F10, F11, F12, F15)                   -->
    <!-- ============================================================ -->
    <change id="C8" priority="P2" source_item="F10, F11, F12, F15">
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="run_orchestrator.py" action="modify" />
      <file path="orchestrator_utils.py" action="modify" />
      <description>Remove dead locals, dead counter, dead constant, and planning identifiers from code.</description>
      <spec>
F10: Delete orchestrate_first_level.py:165 (the dead qc_cfg = config.get(...) in process_participant). Keep the one in _process_session at :372.

F11: In run_orchestrator.py:
  - Delete "n_skipped": 0 from the state dict (line 365).
  - Delete nsk = state["n_skipped"] (line 207).
  - Change n_done = ns + nfu + nfk + nsk to n_done = ns + nfu + nfk (line 209).
  - Remove ", {nsk} skipped" from the progress line string (line 220).

F12: In orchestrator_utils.py, delete lines ~2196-2203 (the comment block and PRIMARY_OUTPUT_SUFFIX = {...}).

F15: In orchestrate_first_level.py:
  - Line 481: "# Pre-exclude non-rest runs without events (C4)" -> "# Pre-exclude non-rest runs without events"
  - Line 731: "# Collect per-run files from the survivor record list (T2)" -> "# Collect per-run files from the survivor record list"
In orchestrator_utils.py:
  - Lines 997-998: replace "Events are NOT checked here (C4 run-level exclusion handles missing events downstream)." with "Events are NOT checked here; runs missing events are excluded per run during session processing."
      </spec>
      <dependencies>none</dependencies>
      <risk>low - all verified dead; F12 breaks test imports (resolved by /test immediately after)</risk>
      <rollback>git checkout -- orchestrate_first_level.py run_orchestrator.py orchestrator_utils.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C9: Unused parameters and dead loop bindings (F13)           -->
    <!-- ============================================================ -->
    <change id="C9" priority="P2" source_item="F13">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Remove four unused function parameters (and their docstring entries) and fix three dead loop bindings.</description>
      <spec>
Parameters to remove:
  - compute_preproc_qc: remove "space" (9th positional, line 2048). Update docstring (delete the space parameter entry at ~2079-2080).
  - build_first_level_config: remove "task_defs" (4th positional, line 2719). Update docstring (delete the task_defs parameter entry at ~2737).
  - validate_task_timing: remove "logger" (4th positional, line 1807). Update docstring (delete the logger parameter entry at ~1824).
  - get_peak_memory_usage: remove "logger" (1st positional, line 2361). Update docstring (delete the logger parameter entry at ~2367).

Production call-site updates:
  - orchestrate_first_level.py:577-584: compute_preproc_qc call drops the space argument (currently 9th positional "space").
  - orchestrate_first_level.py:654: validate_task_timing call drops the logger argument (4th positional).
  - orchestrate_first_level.py:835: build_first_level_config call drops the task_defs argument (was 4th: "task_defs"). Shift subsequent args left.
  - orchestrator_utils.py:2558: get_peak_memory_usage(logger) -> get_peak_memory_usage().

Dead local: delete orchestrate_first_level.py:377 "space = study['space']" (study["space"] is still used directly at orchestrator_utils.py:723,769 via the study_config dict).

Dead loop bindings:
  - orchestrator_utils.py:174 "for root, dirs, files" -> "for root, _dirs, files"
  - orchestrator_utils.py:745 "for root, dirs, files" -> "for root, _dirs, files"
  - orchestrator_utils.py:981 "for run_int, files in runs.items():" -> "for files in runs.values():"
      </spec>
      <dependencies>C8 (F12 removal must come first so line offsets are stable within orchestrator_utils.py)</dependencies>
      <risk>medium - signature changes break 24 test call sites; /test must run immediately after this build</risk>
      <rollback>git checkout -- orchestrator_utils.py orchestrate_first_level.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C10: Banner titles and section letters (F18)                 -->
    <!-- ============================================================ -->
    <change id="C10" priority="P2" source_item="F18">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Retitle all 16 section banners with descriptive titles, removing letter prefixes.</description>
      <spec>
Replace the middle line of each three-line banner (keeping the # ==== rules):
  65: "# Exit classification constants (three-way exit code scheme)" -> "# Exit code constants"
  90: "# S3 Helpers" -> "# S3 routing, sentinel and upload"
  538: "# Section B: Upstream Compatibility" -> "# Upstream version check"
  601: "# Section C: File Discovery (producer per-file layout)" -> "# Input discovery, completeness check and download"
  1083: "# Section F: Non-Steady-State TR Handling" -> "# Non-steady-state TR detection and removal"
  1210: "# Section G: Motion and Confounds Extraction" -> "# Motion regressors and producer FD check"
  1463: "# Section G2: Mask-Based Tissue and Global-Signal Regressors" -> "# Tissue and global-signal regressors from masks"
  1604: "# Section H: Task Timing" -> "# Task timing relabeling, conversion and validation"
  1895: "# Section K: QC, Preprocessing" -> "# Preprocessing and first-level QC"
  2358: "# Section K2: Peak Memory" -> "# Peak memory measurement"
  2395: "# Section K3: Terminal Classification and Exit Codes" -> "# Outcome classification and exit codes"
  2501: "# Section K4: Consolidated Session QC" -> "# Session QC consolidation"
  2664: "# Section K5: AFNI Warning Filter" -> "# AFNI unvalidated-version warning filter"
  2716: "# Section L: Config Building" -> "# First-level config building"
  2897: "# Section M: Orchestrator Config Validation" -> "# Orchestrator config loading and proc-template validation"
  3312: "# Section N: Local Cleanup" -> "# Local input cleanup"
      </spec>
      <dependencies>C6 (the producer name at 601 is handled by C6; this change replaces the banner text around it)</dependencies>
      <risk>low - comment-only; no code or test references banner titles</risk>
      <rollback>git checkout -- orchestrator_utils.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C11: Step/phase narration removal (F17)                      -->
    <!-- ============================================================ -->
    <change id="C11" priority="P2" source_item="F17">
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="orchestrator_utils.py" action="modify" />
      <description>Remove all step/phase prefixes from comments, banners and log messages.</description>
      <spec>
orchestrate_first_level.py comments (delete prefix, keep description):
  169: "# Step 0: Discover available sessions ..." -> "# Discover available sessions ..."
  438: "# Step 1: Resolve session inputs ..." -> "# Resolve session inputs ..."
  476: "# Steps 2-6: Per-task preprocessing" -> "# Per-task preprocessing"
  751: "# Step 4: Build config ..." -> "# Build config ..."
  962: "# Step 5: Consolidated session QC JSON" -> "# Consolidated session QC JSON"
  982: "# Step 6: Classify session exit category" -> "# Classify session exit category"
  993: "# Step 7: Per-file upload ..." -> "# Per-file upload ..."

orchestrate_first_level.py log messages:
  171: "Step 0: Discovering available sessions..." -> "Discovering available sessions..."
  467: "Step 1: Downloading session inputs from S3..." -> "Downloading session inputs from S3..."

orchestrator_utils.py comments (delete prefix, keep description):
  751: "# Phase 1: collect all matches, ..." -> "# Collect all matches, ..."
  785: "# Phase 2: build the discovery structure" -> "# Build the discovery structure"
  824: "# Phase 3: assign non-BOLD ..." -> "# Assign non-BOLD ..."
  844: "# Phase 4: producer completion gate. ..." -> "# Producer completion gate. ..."
  1522: "# Step 1: Resample tissue mask ..." -> "# Resample tissue mask ..."
  1536: "# Step 2: Intersect with brainmask" -> "# Intersect with brainmask"
  1543: "# Step 3: Count non-zero voxels ..." -> "# Count non-zero voxels ..."
  1568: "# Step 4: Extract mean time course" -> "# Extract mean time course"

orchestrator_utils.py log message:
  401: "Upload phase: sub-%s ses-%s, ..." -> "Starting upload: sub-%s ses-%s, ..."

Replace the _process_session docstring list (orchestrate_first_level.py:359-367, the numbered "FULL pipeline steps" list) with:
  "The FULL path downloads inputs (or uses local paths), preprocesses each task (NSS trim, QC, motion, tissue, timing), collects per-run files, builds the first-level config, runs the analyses, writes the session QC JSON, uploads outputs per file, sweeps stale S3 keys, writes the sentinel unless first-level analysis was skipped or the session failed unexplained, and cleans up local files."
      </spec>
      <dependencies>C8 (some lines shift after dead-code removal)</dependencies>
      <risk>low - only comments and two log message strings change; no test depends on them</risk>
      <rollback>git checkout -- orchestrate_first_level.py orchestrator_utils.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C12: Stale docstrings and headers (F16)                      -->
    <!-- ============================================================ -->
    <change id="C12" priority="P2" source_item="F16">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="run_orchestrator.py" action="modify" />
      <file path="example_proc_config.yaml" action="modify" />
      <description>Fix stale docstring wording, wrong filenames, placeholder IDs, and delete Version/Last updated lines.</description>
      <spec>
1. orchestrator_utils.py validate_task_timing docstring (~1810): "post-onset shift/clip" -> "post-onset shift and negative-onset drop"
2. orchestrate_first_level.py exit-code header (~17-18): "unexpected_error run exclusion" -> "unexpected_error or producer_fd_inconsistent run exclusion" (this text moves to the epilog in C13)
3. Replace "example_config.yaml" with "example_proc_config.yaml" at:
   - orchestrate_first_level.py:25
   - run_orchestrator.py:14, 256, 261
   - example_proc_config.yaml:6, 7, 8
4. orchestrate_first_level.py:27: "<individual_id> <individual_id> <individual_id>" -> "SUBJ01 SUBJ02 SUBJ03"
5. orchestrate_first_level.py:1054 help text: "e.g. <individual_id>" -> "e.g. SUBJ01"
6. Additional NDAR-style placeholders to replace with SUBJ01-style:
   - orchestrator_utils.py:128 "(e.g. \"<individual_id>\")" -> "(e.g. \"SUBJ01\")"
   - orchestrator_utils.py:707 "(e.g. \"<individual_id>\")" -> "(e.g. \"SUBJ01\")"
   - orchestrator_utils.py:2078 "(e.g., \"sub-<individual_id>\")" -> "(e.g., \"sub-SUBJ01\")"
7. Delete Version and Last updated lines:
   - orchestrate_first_level.py:30 "# Version: 4.0.0" and :31 "# Last updated: 10/16/26"
   - run_orchestrator.py:35 "# Version: 2.0" and :36 "# Last updated: 02/19/26"
   - orchestrator_utils.py:14 "# Last updated: 10/16/26"
      </spec>
      <dependencies>none (can run in parallel with C6-C11, but targets the same files; sequence after C11 for clarity)</dependencies>
      <risk>low - only comments, docstrings, help text; no code logic</risk>
      <rollback>git checkout -- orchestrator_utils.py orchestrate_first_level.py run_orchestrator.py example_proc_config.yaml</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C13: Verbose comment blocks (F19)                            -->
    <!-- ============================================================ -->
    <change id="C13" priority="P2" source_item="F19">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <file path="run_orchestrator.py" action="modify" />
      <description>Shorten module headers to 3 lines; shorten inline comment blocks; move usage/exit-code info to argparse epilogs.</description>
      <spec>
Module headers (between the # ==== lines):

run_orchestrator.py (currently 33 non-rule lines -> 3):
  # PARALLEL BATCH RUNNER FOR fMRI FIRST-LEVEL ORCHESTRATOR
  # Runs orchestrate_first_level.py over a subject list in a thread pool; see --help.
  # Author: Taylor J. Keding, Ph.D.
Move the subject-list format and exit-code list (0/1/2/130) into the existing argparse epilog, appended after the Examples block. Use the same indentation style.

orchestrate_first_level.py (currently 28 non-rule lines -> 3):
  # PER-PARTICIPANT ORCHESTRATOR FOR fMRI FIRST-LEVEL PROCESSING (ABCD)
  # Processes one participant one session at a time (download, preprocess, analyze, upload, cleanup) to bound local disk use; see --help.
  # Author: Taylor J. Keding, Ph.D.
Add formatter_class=argparse.RawDescriptionHelpFormatter and an epilog to main()'s parser containing the exit-code list (0/1/2, with the producer_fd_inconsistent addition from C12) and the parallel-invocation example (using SUBJ01/SUBJ02/SUBJ03 from C12 and example_proc_config.yaml from C12).

orchestrator_utils.py (currently 11 non-rule lines -> 3):
  # ORCHESTRATOR UTILITIES FOR fMRI FIRST-LEVEL PROCESSING
  # Helper functions called by orchestrate_first_level.py.
  # Author: Taylor J. Keding, Ph.D.

Inline comment blocks:

orchestrator_utils.py:604-608 (5 non-rule lines -> 3):
  # Input filename patterns: single edit point for the producer naming convention.
  # Session directories end in "A" (ses-00A); the captured session code is the
  # numeric part. BOLD and brainmask carry a space- entity and no desc- entity.

orchestrator_utils.py:844-848 (5 non-rule lines -> 3):
  # Producer completion gate: _complete.json is written last, so a session
  # directory without it is still being written and is skipped. Session
  # directories come from the listing itself; no path template is needed.

orchestrate_first_level.py:424-429 (banner + 3 comment lines -> 2):
  # S3 mode wipes existing local session output for a clean recompute;
  # local mode keeps it for force_recompute-governed caching.
(The banner line "# FULL path: prepare local output directories" is kept as the section banner above these two lines.)
      </spec>
      <dependencies>C11, C12 (those changes alter the header content that this change replaces)</dependencies>
      <risk>low - only comments and --help output change; no test checks --help</risk>
      <rollback>git checkout -- orchestrator_utils.py orchestrate_first_level.py run_orchestrator.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C14: Em-dash removal (F20)                                   -->
    <!-- ============================================================ -->
    <change id="C14" priority="P2" source_item="F20">
      <file path="run_orchestrator.py" action="modify" />
      <file path="AID_LOG.md" action="modify" />
      <description>Replace all em-dashes in run_orchestrator.py (3) and AID_LOG.md (14).</description>
      <spec>
run_orchestrator.py:
  ~90: "'{s}' — skipping." -> "'{s}', skipping."
  ~370: "(daemon — auto-exits when main thread ends)" -> "(daemon: auto-exits when the main thread ends)"
  ~430: "Interrupted (Ctrl+C) — cancelling pending jobs," -> "Interrupted (Ctrl+C): cancelling pending jobs,"

AID_LOG.md: replace each of its 14 em-dashes with a comma, colon, parentheses or sentence break as the sentence requires. No semantic changes.

Leave .aid/reports/ files unchanged (historical records).
      </spec>
      <dependencies>none</dependencies>
      <risk>low - the two printed strings have no test assertions</risk>
      <rollback>git checkout -- run_orchestrator.py AID_LOG.md</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C15: Upload progress cadence (F9)                            -->
    <!-- ============================================================ -->
    <change id="C15" priority="P2" source_item="F9">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Replace the every-10th-file upload progress with a percentage-cadence log.</description>
      <spec>
In upload_session_to_s3, before the ThreadPoolExecutor block (~line 448):
  Add: step = max(1, math.ceil(len(upload_list) / 10))

Replace the existing loop (for i, fut in enumerate(as_completed(...), start=1):) with:
  for fut in as_completed(future_to_target):
    exp_key, target = future_to_target[fut]
    try:
        fut.result()
        n_uploaded += 1
        if n_uploaded % step == 0 or n_uploaded == len(upload_list):
            logger.info(
                "%d/%d uploaded (sub-%s ses-%s)",
                n_uploaded, len(upload_list), sub_id, session
            )
        else:
            logger.debug("Uploaded: %s", exp_key)
    except Exception as e:
        failures.append((target, e))

This produces about 10 evenly spaced INFO lines plus the final one.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - logging cadence only; error handling unchanged</risk>
      <rollback>git checkout -- orchestrator_utils.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C16: Carpet plot streaming (F21)                             -->
    <!-- ============================================================ -->
    <change id="C16" priority="P1" source_item="F21">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Stream 3dmaskdump output to eliminate the 200+ MB transient memory spike per carpet plot.</description>
      <spec>
In generate_carpet_plot, replace lines ~2006-2021 (the subprocess.run block through voxel_data construction) with:

    max_voxels = 2000
    try:
        # Get voxel count from mask
        count_result = subprocess.run(
            ["3dBrickStat", "-count", "-non-zero", mask_path],
            capture_output=True, text=True, check=True,
        )
        n_voxels = int(float(count_result.stdout.split()[0]))
    except (subprocess.CalledProcessError, ValueError):
        logger.warning("Could not determine mask voxel count for carpet plot; skipping.")
        return

    # Compute which rows to keep
    if n_voxels <= max_voxels:
        keep = None  # keep all
    else:
        keep = set(np.linspace(0, n_voxels - 1, max_voxels, dtype=int).tolist())

    rows = []
    try:
        proc = subprocess.Popen(
            ["3dmaskdump", "-mask", mask_path, "-noijk", "-quiet", bold_path],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        for i, line in enumerate(proc.stdout):
            if keep is None or i in keep:
                rows.append(list(map(float, line.split())))
        proc.wait()
        if proc.returncode != 0:
            logger.warning("3dmaskdump returned non-zero (%d) for carpet plot; skipping.", proc.returncode)
            return
    except (OSError, ValueError):
        logger.warning("Could not extract voxel data for carpet plot; skipping.")
        return

    # Guard: row count mismatch
    expected_rows = min(n_voxels, max_voxels)
    if len(rows) != expected_rows:
        logger.warning(
            "Carpet plot row count mismatch: expected %d, got %d; skipping.",
            expected_rows, len(rows)
        )
        return

    voxel_data = np.array(rows)

Everything from voxel_data onward (z-scoring, plotting) stays the same.
      </spec>
      <dependencies>none</dependencies>
      <risk>medium - changes subprocess invocation pattern; needs Popen mock in tests; /test must run immediately after</risk>
      <rollback>git checkout -- orchestrator_utils.py</rollback>
    </change>

    <!-- ============================================================ -->
    <!-- C17: Single head_object routing (F22)                        -->
    <!-- ============================================================ -->
    <change id="C17" priority="P1" source_item="F22">
      <file path="orchestrator_utils.py" action="modify" />
      <file path="orchestrate_first_level.py" action="modify" />
      <description>Rename check_session_complete to get_session_sentinel, return the head_object response (or None), and eliminate the second HEAD request in determine_session_routing.</description>
      <spec>
1. Rename def check_session_complete -> def get_session_sentinel. Update docstring:
   - Returns: "dict or None. The head_object response dict when the sentinel exists; None when absent (404/NoSuchKey)."
   - Body: instead of "return True" after head_object, return the response dict. Instead of "return False" on 404/NoSuchKey, return None.

2. In determine_session_routing:
   - Delete s3_client = _get_s3_client() and bucket = s3_config["bucket"] (lines ~309-310).
   - Change: sentinel_present = check_session_complete(...) -> sentinel_resp = get_session_sentinel(...)
   - Change: if sentinel_present: -> if sentinel_resp is not None:
   - On the skip path: delete the second head_object call and sentinel_key rebuild (~lines 316-317). Instead: last_modified_iso = sentinel_resp["LastModified"].isoformat()
   - Docstring Raises section: "OrchestratorError" -> "botocore ClientError for S3 errors other than 404/NoSuchKey."
   - Keep: "Performs one head_object call (the sentinel probe)"

3. orchestrate_first_level.py imports: change check_session_complete -> get_session_sentinel in the import list if it is imported (check: it is NOT directly imported; only determine_session_routing is imported). No change needed there.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - single caller; existing tests mock head_object.return_value as a dict, which is now the returned value</risk>
      <rollback>git checkout -- orchestrator_utils.py</rollback>
    </change>

  </changes>

  <execution_order>C1, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16, C17</execution_order>

  <!-- ============================================================ -->
  <!-- DEFERRED TO OTHER SKILLS                                      -->
  <!-- ============================================================ -->
  <!-- F6 (README + INPUT_SPEC rewrite): /document                   -->
  <!-- F7 (environment.yaml pin): /publish precondition              -->
  <!-- F23 (test dead code + planning IDs): /test                    -->
  <!--                                                               -->
  <!-- /test must run immediately after this build for:              -->
  <!--   F12 (PRIMARY_OUTPUT_SUFFIX removal breaks test imports)     -->
  <!--   F13 (signature changes break 24 test call sites)            -->
  <!--   F21 (Popen mock needed for carpet plot tests)               -->

</implement_plan>
