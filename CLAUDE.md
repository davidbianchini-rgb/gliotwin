# CLAUDE.md

## Purpose

This file defines the project rules that coding LLMs must follow when working on Gliotwin.

It is not user documentation.
It is not a clinical specification.
It is not a generic project overview.

Its purpose is to keep coding agents aligned on:

- the correct mental model of the project
- the logical separation of pipeline phases
- the expected terminology
- the current architecture baseline
- the constraints that must guide code changes

## Core Project Model

Gliotwin must not be treated as a single monolithic pipeline.

The project must be modeled as a state-driven workflow over:

`patient -> timepoint -> session -> series -> structures`

Operational note:

- in this project, `timepoint` and `session` are effectively the same working unit
- each timepoint/session advances through explicit steps
- each step must expose a clear status
- a step is either acceptable for progression or blocked
- when blocked, the reason must be explicit
- when completed, the completion path should be traceable

The system should support selecting subjects by state and advancing all cases that are ready for a given step.
Do not assume the only valid execution mode is full end-to-end processing of one patient at a time.

## Mandatory Logical Phases

The project is organized around these logical phases:

- `IMPORT`
- `PREPROCESSING`
- `SEGMENTATION`
- `ANALYSIS`
- `EXPORT`

These are logical boundaries first.
They may or may not map to separate software modules, but they must not be mixed conceptually in the codebase.

## Phase Definitions

### `IMPORT`

Responsibilities:

- ingest data from known source datasets
- read imaging, structures, and clinical data
- apply dataset-specific import rules
- apply dataset-specific anomaly correction rules
- expose imported data as available for preprocessing

Rules:

- each dataset may require dedicated import logic
- each dataset may require dedicated exceptions
- structures also need import rules, not only images
- ambiguous cases must be routed to manual review

Official datasets:

- `IRST`
- `MU-Glioma-Post`
- `UCSD-PTGBM`
- `RHUH-GBM`
- `QIN-GBM-TREATMENT-RESPONSE`
- `GLIS-RT`

Product expectation:

- these datasets should appear as selectable import sources in the application
- backend logic must define the import/conversion actions for each supported dataset

### `PREPROCESSING`

Responsibilities:

- take recognized and imported data
- identify patient, timepoint/session, required sequences, and linked structures
- convert data into a standard representation usable by segmentation engines
- move images into the canonical space required for visualization and segmentation

Typical operations inside preprocessing:

- required sequence recognition
- dedicated handling for special sequences such as `APT`
- image and structure conversion to `NIfTI` when needed
- registration/alignment into canonical space
- skull stripping or equivalent preparation steps
- signal remapping / intensity preparation

Rules:

- intermediate data does not need to dominate the user workflow
- however, the system must allow running steps in sequence and recovering intermediates when needed
- retained transformations are currently needed mainly to bring structures back to native DICOM space for export
- current relevant transform assumptions are rigid, not deformable

Do not hard-code unverified export assumptions as guaranteed behavior.

### `SEGMENTATION`

Responsibilities:

- run segmentation engines as separate modules
- consume canonical preprocessing outputs
- produce structures compatible with the project data model

Rules:

- segmentation is a separate phase, not part of preprocessing
- segmentation engines are consumers of standardized input
- segmentation engines are producers of structures
- produced structures must coexist with imported structures and future manual structures

Future note:

- structure algebra / derived structures are planned
- they are not the immediate design priority

### `ANALYSIS`

Responsibilities:

- longitudinal analysis on a single patient over time
- cohort analysis
- statistics
- filtering by subjects, sequences, and structures

Rules:

- analysis is a separate area of the product
- frontend and backend shape are still to be refined
- use the current project structure as reference instead of inventing a disconnected subsystem

### `EXPORT`

Responsibilities:

- export structures as `DICOM RTSTRUCT`
- associate the export with a native imaging series

Rules:

- current export target is only `DICOM RTSTRUCT` on native series
- do not present untested reversibility details as already guaranteed

## Canonization Rules

Canonization means standardization of heterogeneous data.
It is not only file conversion.

The goal is to transform imaging, structures, and clinical data from multiple sources into a representation that can be compared and processed consistently.

Canonization must eliminate differences that block:

- comparison across subjects
- comparison across timepoints
- consistent preprocessing
- consistent segmentation input
- consistent analysis

## State and Checklist Model

Each timepoint/session should be represented by a checklist-like progression model.

Minimal logical example:

- dataset recognized
- patient recognized
- timepoint/session recognized
- imaging series recognized
- structures recognized
- ambiguities resolved or routed to manual review
- import completed
- preprocessing ready
- preprocessing completed
- segmentation runnable
- segmentation completed
- analysis available
- export available

Each checklist item should store:

- status
- blocking problem when present
- trace of how the step was completed

The workflow should support selecting subjects by status and advancing all ready cases for a specific phase.

## Manual Review Rules

Do not silently resolve ambiguous identity or mapping problems.

Examples:

- same ID with different names
- same name with different IDs
- uncertain imaging series classification
- unclear structure origin
- unclear mapping between imported structures and reference data

These cases must go to manual review.

## Structure Model Rules

Do not create deep or artificial hierarchies for structures unless truly necessary.

The key organizing concept is structure origin.
Keep origins at the same conceptual level, for example:

- imported from source dataset
- computed by engine A
- computed by engine B
- manually created

This classification should stay simple.

## Constraints For Coding Agents

When changing code, always preserve these boundaries:

- do not mix `IMPORT` and `PREPROCESSING`
- do not mix `PREPROCESSING` and `SEGMENTATION`
- do not collapse `ANALYSIS` into preprocessing or segmentation logic
- do not assume `EXPORT` is just another preprocessing side effect

When designing data flow:

- think in terms of `timepoint/session` progression
- preserve batch advancement by state
- preserve explicit blocking reasons
- preserve manual review paths

When designing structures:

- classify by origin first
- avoid unnecessary hierarchy
- allow coexistence of imported, computed, and future manual structures

When dealing with ambiguous requirements:

- prefer explicit state and review mechanisms over silent heuristics
- do not promote untested assumptions to hard requirements

When modifying existing code:

- start from the current architecture already present in the repository
- do not redesign the whole system unless the task explicitly requires it

## Current Repository Baseline

Coding agents should assume the repository already contains a partial implementation of this model.

Observed baseline:

- a DB schema already exists for subjects, sessions, sequences, radiological structures, computed structures, clinical events, and processing jobs
- `session_label` and `timepoint` already exist as active concepts
- a serialized per-subject/per-timepoint pipeline state already exists
- dataset-specific import pipelines already exist

Therefore:

- extend the current system
- align new code with the existing structure
- avoid replacing working concepts with a brand-new abstraction layer unless necessary

## What Is Still Not Fully Defined

The following areas are still open and should be treated as design work, not as settled facts:

- a complete unified checklist model for all timepoints
- complete advancement rules for batch progression by state
- complete import rules for all official datasets
- complete structure import rules for heterogeneous structure sources
- final standardized representation of structure origin in storage and APIs
- final frontend/backend shape of the analysis area
- tested and verified end-to-end behavior for native-space RTSTRUCT export

## Working Style Expected From Coding Agents

When implementing features or fixes:

- use the existing project as the source of truth
- preserve logical phase separation
- make state transitions explicit
- keep failure modes inspectable
- route ambiguity to manual review
- avoid inventing extra hierarchy without need
- avoid claiming capabilities that are not verified by tests

If a requested change touches import, preprocessing, segmentation, analysis, or export, explicitly identify which phase is being changed and avoid bleeding responsibilities across phases.
