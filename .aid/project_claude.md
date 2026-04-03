# CLAUDE.md (sanitized for public archive)

> This file is a sanitized copy of the project CLAUDE.md preserved per the AI Disclosure (AID)
> Framework (Weaver, 2025). Local filesystem paths and usernames have been replaced with
> `<local_path>` and `<user>` placeholders.

## Project Overview

This project is a dataset-specific implementation for running first-level fMRI analyses using
fmri_first_level_proc (https://github.com/tjkeding/fmri_first_level_proc). The GitHub repo for
this orchestrator is https://github.com/tjkeding/ABCD_fmri_orchestrator_S3. Additional
information on the orchestrator can be found in README.md and INPUT_SPECIFICATION.md. Additional
information on fmri_first_level_proc can be found in README.md and INPUT_SPECIFICATION.md from
its GitHub repo.

## Location for Placing Temporary Data

A sandbox directory at `<local_path>/AFNI_orchestrator_testing` was used for all temporary
downloads, outputs, and intermediate files during development. Read, write, and execute
permissions within that directory were granted to the AI development tool.
