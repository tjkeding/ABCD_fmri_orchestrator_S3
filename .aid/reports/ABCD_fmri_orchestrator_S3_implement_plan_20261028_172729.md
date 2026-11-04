<implement_plan>
  <meta project="ABCD_fmri_orchestrator_S3" mode="implement" submodule="plan" timestamp="2026-10-28T17:27:29Z" />
  <input_reports>
    <report path="ABCD_fmri_orchestrator_S3_test_20261028_172404.md" mode="test" key_items="1" />
  </input_reports>
  <changes>
    <change id="C1" priority="P2" source_item="test report action_items[0]">
      <file path="orchestrator_utils.py" action="modify" />
      <description>Replace 8 numbered inline comments in upload_session_to_s3 with descriptive text, per the project's no-arbitrary-markers rule. Comment-only change; no code, logic, or whitespace changes.</description>
      <spec>
Replace each numbered comment with a descriptive equivalent. Exact mapping:

  "# 1. Enumerate expected targets"                             -> "# Enumerate expected targets"
  "# 2. Build expected-keys map"                                -> "# Build expected-keys map"
  "# 3. Probe remote inventory"                                 -> "# Probe remote inventory"
  "# 4. Compute upload gap"                                     -> "# Compute upload gap"
  "# 5. Parallel upload via ThreadPoolExecutor"                 -> "# Parallel upload via ThreadPoolExecutor"
  "# 6. Batched verification"                                   -> "# Batched verification"
  "# 7. Sweep stale remote keys (runs regardless of write_sentinel)" -> "# Sweep stale remote keys (runs regardless of write_sentinel)"
  "# 8. Conditionally write sentinel"                           -> "# Conditionally write sentinel"

Each replacement strips only the leading "N. " prefix; the rest of the comment text is unchanged.
      </spec>
      <dependencies>none</dependencies>
      <risk>low - comment-only change; no functional code is modified</risk>
      <rollback>Restore the "N. " prefix to each of the 8 comments.</rollback>
    </change>
  </changes>
  <execution_order>C1</execution_order>
</implement_plan>
