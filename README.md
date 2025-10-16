# coding-sample

This repository contains code for a research project examining the division of parenting labor among Taiwanese physician families.  
It demonstrates the data cleaning and construction processes used to build analysis-ready datasets from population-level administrative data.

## File Overview

- `00_1_collect_all_doc_id.R`  
  Extracts all valid physician identifiers from outpatient visit datasets across medical systems.

- `00_2_build_doc_child_relation.R`  
  Identifies children whose parents are both physicians and constructs the parent–child linkage table.  
  Also produces annual summaries of doctor-parent births.

- `01_1_build_doc_child_opdte.R`  
  Loads outpatient visit records for doctor-parent children and compiles yearly visit lists.

- `01_2_build_doc_child_ipdte.R`  
  Harmonizes inpatient records for doctor-parent children and expands hospitalization episodes into daily-level panels.

- `03_1_build_doc_diary.R`  
  Constructs a daily-level panel for physician parents by integrating outpatient, inpatient, birth, marriage, and child illness records.  
  Adds demographic attributes and event indicators related to caregiving and work activity.

- `04_construct_analysisData.R`  
  Builds the main analysis dataset by expanding events into ±30-day windows, defining treatment and control groups, and preparing a clean, analysis-ready table.

---

Each script is modular and should be executed in order to reproduce the full workflow—from raw data extraction to the creation of analysis-ready datasets.