# AI Development Log

This document discloses the use of AI-assisted development tools in the creation of the **ABCD_fmri_orchestrator_S3** analysis pipeline, in accordance with emerging best practices for transparency in scientific software development.

---

## 1. Purpose

This document provides a structured disclosure of AI tool usage during the development of the ABCD_fmri_orchestrator_S3 pipeline. The disclosure follows the AI Disclosure (AID) Framework (Weaver, 2025) and adheres to recommendations for responsible AI use in scientific computing (Bridgeford et al., 2025; Nussberger et al., 2024; Jamieson et al., 2024). The intent is to ensure that reviewers, collaborators, and end users can assess the nature and extent of AI involvement in the development process.

## 2. Scope

AI assistance was utilized for **analysis pipeline development**, encompassing:

- Code architecture and design
- Statistical methodology review and validation
- Implementation of pipeline modules
- Test suite development and validation
- Documentation authoring and refinement

AI was **not** used for:

- Running analyses on real data
- Interpreting scientific results from pipeline outputs
- Making domain-specific methodological decisions (e.g., selection of FD thresholds, choice of nuisance regressors, or study-specific analytical choices)

ABCD_fmri_orchestrator_S3 is a software pipeline, not an analysis. The AI-assisted work covers only the development of that pipeline — the application of the pipeline to real ABCD Study data (subject selection, parameter choices, interpretation of results) is performed independently by the researcher without AI involvement.

## 3. Tools Used

Development utilized **Claude Code** (Anthropic), employing two model tiers:

| Model | Role | Tasks |
|-------|------|-------|
| Claude Opus 4 | Analytical and review | Critical review of statistical methods, brainstorming sessions, code quality audits, risk assessment, and architectural decisions |
| Claude Sonnet 4 | Implementation | Code generation, test implementation, documentation drafting, and file management |

This dual-model approach ensured that analytical depth (Opus) was applied to decisions with statistical or methodological consequences, while implementation efficiency (Sonnet) was used for well-specified coding tasks under explicit human direction.

## 4. Development Workflow

The pipeline was developed through an iterative, mode-based workflow with the following stages:

1. **Brainstorm** — Structured discussion of design decisions, trade-offs, and alternative approaches. Brainstorm sessions produced reports with explicit decision records (accepted, rejected, deferred), including architectural decisions such as the session-centric processing model and the delegation of motion processing to the upstream `fmri_first_level_proc` library.

2. **Critical Review (CR)** — Formal review of the codebase for statistical correctness, robustness, reproducibility, and defensive coding practices. Each finding was classified by severity (P0/P1/P2) and required explicit human triage (accept, reject, or modify). Two rounds of critical review were conducted (19 findings and 15 findings respectively).

3. **Implement (Plan + Build)** — Implementation proceeded in two sub-phases: (a) a technical specification mapping each approved change to specific code modifications with risk assessment, and (b) execution of the specification. All plans required human approval before code generation began.

4. **Test** — Comprehensive test suite development covering unit, integration, edge-case, and statistical invariant tests. Tests were designed prior to implementation where feasible (test-first methodology). The test suite comprises 274 tests (0 failures, 12 skipped) validated against simulated end-to-end pipeline runs (N=19 simulated sessions) and real ABCD data (N=30 subjects, 98.5% analysis success rate across 133 sessions).

5. **Clean** — Code quality review for consistency, style, and maintainability.

6. **Document** — Authoring and updating of user-facing documentation and machine-readable technical specifications.

Key properties of this workflow:

- All decisions required **explicit human approval** before implementation.
- The pipeline was developed with a **test-first** approach where feasible.
- Every statistical and algorithmic choice was subjected to **formal critical review**, with findings documented and triaged individually.

## 5. Human Oversight

The researcher maintained full oversight and decision authority throughout the development process:

- **(a)** Defined all statistical methodology and analytical approach, including the motion data sourcing strategy (raw mmps_mproc motion.tsv files rather than fMRIPrep confounds), the delegation of FD computation and censoring to `fmri_first_level_proc`, and the per-analysis FD threshold design.

- **(b)** Triaged every critical review finding with explicit accept/reject/modify decisions, documented in brainstorm reports with rationale for each determination.

- **(c)** Approved all implementation plans (technical specifications) before any code generation was executed.

- **(d)** Validated all test results and ensured test coverage aligned with the statistical guarantees required by the pipeline, including real-world validation against N=30 ABCD subjects.

- **(e)** Made all domain-specific decisions regarding pipeline architecture, algorithmic choices, and analytical strategy, including the session-centric processing model, the partial-success session status model, and the QC consolidation approach.

## 6. Audit Trail

A complete record of the structured development process is available in the `.aid/reports/` directory within this repository. The audit trail includes:

- **Brainstorm reports** — Records of design discussions, decision rationale, and trade-off analyses.
- **Critical review reports** — Formal findings with severity classifications and human triage decisions.
- **Implementation plans** — Technical specifications mapping approved changes to code modifications.
- **Implementation build reports** — Records of executed changes with deviation notes.
- **Test reports** — Test suite results and coverage summaries.
- **Code quality reviews** — Clean-pass reports on style and consistency.
- **Documentation reports** — Records of documentation updates and revisions.

The project-level configuration file used to guide AI interactions is preserved as `.aid/project_claude.md`.

Raw session transcripts are excluded for privacy reasons. The structured reports above capture all substantive technical decisions, rationale, and implementation details.

## 7. References

- Bridgeford, E. W., et al. (2025). Ten simple rules for AI-assisted coding in science. *arXiv preprint*, arXiv:2510.22254.

- Jamieson, A. J., et al. (2024). Protecting scientific integrity in an age of generative AI. *Proceedings of the National Academy of Sciences*, 121(41), e2407886121.

- Nussberger, A.-M., et al. (2024). Ten simple rules for using large language models in science. *PLOS Computational Biology*, 20(7), e1012291.

- Weaver, J. B. (2025). The AI Disclosure (AID) Framework. *arXiv preprint*, arXiv:2408.01904v2.

## 8. Version History

- **2026-04-03**: Initial AID_LOG.md created. `.aid/` directory initialized with sanitized `project_claude.md` and 25 development reports (brainstorm, critical review, clean review, implementation plans/builds, test reports, documentation reports). Orchestrator updated for `fmri_first_level_proc` >= 2.4.0 alignment (motion contract, new config parameters, documentation).
