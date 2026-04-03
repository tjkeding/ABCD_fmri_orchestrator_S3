<brainstorm_report>
  <meta project="ABCD_fmri_orchestrator_S3" mode="brainstorm" timestamp="2026-03-11T18:55:00Z" />
  <context_files>
    <file path="fmri_first_level_proc/task_conn_first_level.py" relevance="Primary file containing BUG-001 (sub-brick ordering) and BUG-002 (condition-drop onset files)" />
    <file path="fmri_first_level_proc/first_level_utils.py" relevance="Contains BUG-003 (notch_filter_motion 1D dimension handling)" />
    <file path="fmri_first_level_proc/rest_conn_first_level.py" relevance="Caller of notch_filter_motion; rest_conn pipeline affected by BUG-003" />
    <file path="fmri_first_level_proc/task_act_first_level.py" relevance="Reference implementation for condition handling; verified NOT affected by BUG-001/002" />
    <file path="orch_config_final.yaml" relevance="Orchestrator config used for real-world testing" />
    <file path="proc_config_final.yaml" relevance="Proc config defining nback_conn cond_beta_labels and contrasts" />
  </context_files>
  <topics>
    <topic id="T1" title="rest_conn universal crash: notch_filter_motion 1D dimension misinterpretation">
      <summary>AFNI interprets .1D files as rows=spatial, cols=time. A 378x6 motion file is seen as 378 voxels x 6 time points. 3dTproject requires >= 9 time points, so it crashes. The old ' transpose was needed but its removal in v2.2.0 introduced this crash. The ' is necessary but the output must be transposed back.</summary>
      <approaches>
        <approach id="A1" label="Restore transpose + post-transpose output" feasibility="high" risk="low">
          <description>Restore the ' on -input, then read output with np.loadtxt, transpose if needed, and re-save.</description>
          <pros>Minimal code change; stays within AFNI ecosystem; well-understood behavior</pros>
          <cons>Relies on numpy I/O round-trip; must handle AFNI output format</cons>
        </approach>
        <approach id="A2" label="Pre/post physical transpose" feasibility="high" risk="low">
          <description>Write transposed input (6x378), let 3dTproject process it, then transpose output back.</description>
          <pros>No AFNI operator needed; fully controlled I/O</pros>
          <cons>Two extra file writes; more code</cons>
        </approach>
        <approach id="A3" label="Replace with 1dBport + matrix math" feasibility="med" risk="med">
          <description>Use 1dBport for stopband regressors, project out via numpy.</description>
          <pros>Cleanest semantically; avoids 1D ambiguity entirely</pros>
          <cons>More complex; deviates from AFNI-native approach</cons>
        </approach>
        <approach id="A4" label="Replace with scipy filtfilt" feasibility="med" risk="med">
          <description>Use scipy bandstop Butterworth filter instead of AFNI.</description>
          <pros>No AFNI dependency; simple</pros>
          <cons>Introduces scipy dependency; must match AFNI filtering for reproducibility</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="A1">A1 is the most parsimonious fix. Restoring the transpose and adding a post-transpose step is minimal, well-understood, and keeps the AFNI notch filter (reproducible, literature-standard).</decision>
    </topic>
    <topic id="T2" title="task_conn condition-drop onset file crash">
      <summary>get_stim_data() writes onset files based on cond_beta_labels before condition-drop. Dropped conditions transition from beta to nuisance but their onset files are never written. gen_design_matrix() then references non-existent files.</summary>
      <approaches>
        <approach id="B1" label="Move condition-drop before get_stim_data()" feasibility="high" risk="low">
          <description>Reorder run() flow: validate timing → check trial survival → drop conditions → call get_stim_data() with filtered labels.</description>
          <pros>Clean; onset files written correctly; dropped conditions become nuisance from the start</pros>
          <cons>Requires splitting validation from onset writing; more refactoring</cons>
          <statistical_considerations>Dropped conditions modeled as nuisance -stim_times regressors is statistically preferable to leaving them unmodeled (reduces residual noise, prevents correlated variance from contaminating surviving conditions' beta estimates).</statistical_considerations>
        </approach>
        <approach id="B4" label="Exclude dropped conditions entirely" feasibility="high" risk="med">
          <description>Don't include dropped conditions in the design matrix at all.</description>
          <pros>Simplest code change</pros>
          <cons>Statistically suboptimal — unmodeled variance increases residual noise</cons>
          <statistical_considerations>Unmodeled task variance from dropped conditions can inflate noise estimates and bias beta series for surviving conditions, particularly if the dropped conditions' HRFs overlap temporally with surviving conditions' trials.</statistical_considerations>
        </approach>
      </approaches>
      <decision status="decided" chosen="B1">B1 is preferred. Moving condition-drop before onset file writing ensures correct file generation. The explicit validation-then-filter flow is cleaner and statistically sound.</decision>
    </topic>
    <topic id="T3" title="DISCOVERED: Silent sub-brick ordering mismatch in gen_beta_series()">
      <summary>Critical P0 finding discovered during edge-case analysis of T2. get_stim_data() accumulates onsets in sorted_df['CONDITION'].unique() order (first-appearance). AFNI preserves this file-line order. gen_beta_series() extracts sub-bricks in np.unique() order (alphabetical). These differ for all non-trivial datasets. Empirically verified: 0/13 conditions correctly mapped on a test session. All task_conn connectivity matrices are silently wrong.</summary>
      <approaches>
        <approach id="C1" label="Return beta_cond_order from get_stim_data, thread through" feasibility="high" risk="low">
          <description>get_stim_data() returns the beta condition accumulation order. gen_beta_series() uses this order instead of np.unique().</description>
          <pros>Explicit contract; no implicit ordering assumptions; robust to future changes</pros>
          <cons>Requires modifying function signatures</cons>
        </approach>
        <approach id="C2" label="Alphabetical accumulation in get_stim_data()" feasibility="high" risk="low">
          <description>Change get_stim_data() line 153 to use np.unique() (alphabetical) to match gen_beta_series().</description>
          <pros>Minimal change; no signature changes</pros>
          <cons>Relies on implicit agreement; fragile if either function changes independently</cons>
        </approach>
      </approaches>
      <decision status="decided" chosen="C1">C1 is preferred for robustness. Making the ordering contract explicit prevents future regressions and makes the code self-documenting.</decision>
    </topic>
  </topics>
  <action_items>
    <item priority="P0" target_mode="implement" description="Fix sub-brick ordering mismatch in task_conn gen_beta_series() — return and use explicit beta_cond_order (BUG-001)" />
    <item priority="P0" target_mode="implement" description="Fix notch_filter_motion() 1D transpose: restore ' on input, add output post-transpose (BUG-003)" />
    <item priority="P1" target_mode="implement" description="Reorder task_conn run() flow: move condition-drop before get_stim_data() (BUG-002)" />
    <item priority="P1" target_mode="implement" description="Fix orchestrator session-success reporting when analyses fail" />
    <item priority="P2" target_mode="test" description="After upstream fixes, re-run a test participant and verify all 10+ analyses pass with correct data" />
    <item priority="P2" target_mode="test" description="Add integration test for sub-brick ordering correctness (distinctive signal per condition)" />
  </action_items>
  <next_steps>Fix all three upstream bugs in fmri_first_level_proc (separate session using FIX_fromOrch.txt as guide). Then return to orchestrator for retest via /run-local and proceed to /cr and /publish.</next_steps>
</brainstorm_report>
