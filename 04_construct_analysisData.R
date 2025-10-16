#' @title   Build analysisData: treated vs. control panels
#' @author  Chen-An Lien
#' @created 2025-05-13
#' @updated 2025-MM-DD
#'  
#' @description  
#' This script reads in `doc_diary.parquet`,  
#' expands each “event” record into a ±'date_window'-day panel,  
#' then constructs two groups (treated vs. control)  
#' with stratified oversampling. It finally returns  
#' one combined data.table, `analysisData`,  
#' ready for downstream process.  
#'  
#' @details  
#' **Inputs**  
#' - `doc_diary.rds`: doctor-level daily diary,  
#'    must contain the following columns:  
#'    `ID`, `DATE`, `SICK_C`, `PARENT`,  
#'    `TOTAL_DOT`, `N_VISIT`, `WORK_DAY`,  
#'    `LAST_C_AGE`, `FUNC_TYPE`, `N_DOC`,  
#'    `DOC_STATUS`, `FEMALE`  
#'  
#' **Outputs**  
#' - `analysisData` (data.table):  
#'     `ID`, `DATE`, `SICK_C` (0/1)  
#'     all original measurement columns  
#'     `relative_date` (defined by 'date_window')  
#'     group indicator (`treated` vs. `control`)  
#'  
#' **Key steps**  
#' 1. Filter parents and split into treated (`treatment_var==1`)  
#'    and candidate controls (`treatment_var==0`).  
#' 2. Compute per-stratum sample sizes from treated,  
#'    oversample controls accordingly.  
#' 3. Expand each event into ±'date_window'-day panel via CJ.  
#' 4. Join back all measurement columns in one shot.  
#' 5. Combine treated + control into `analysisData`.  
#'  

# Setups ------------------------------------------------------------------

rm(list = ls()); gc()

# working directory

# WORKING_DIR_PATH <- fs::path("E:/H113038/Ahn/Doctor Mom/")
#
# if (fs::dir_exists(WORKING_DIR_PATH)) {
#   setwd(WORKING_DIR_PATH)
#   message(sprintf("Successfully set the cwd to %s", WORKING_DIR_PATH))
# } else {
#   message(sprintf("%s not exists, please check the directory path.", WORKING_DIR_PATH))
# }

# setwd(here::here())

# Packages ----------------------------------------------------------------

library(data.table)
library(arrow)
library(dplyr)
library(lubridate)
library(ggplot2)

# ─────────────────────────────────────────────────────────────
# Parameters
# ─────────────────────────────────────────────────────────────

doc_diary <- readRDS("data/doc_diary.rds")
doc_diary <- doc_diary[
  PARENT == 1,
  .(ID, DATE, PARENT, AGE, ID_S,
    TOTAL_DOT, N_VISIT, WORK_DAY,
    LAST_C_AGE, FUNC_TYPE, N_DOC,
    DOC_STATUS, FEMALE, CASE_TYPE_C,
    SICK_C, INPATIENT_C, IN_DATE, EMER_C,
    ICD_10, CCS)
]
setorder(doc_diary, DATE, ID)

doc_couple_dot <- readRDS("data/doc_couple_yearly_dot.RDS")
doc_diary[, BF_YEAR := year(DATE)-1]
doc_diary[doc_couple_dot, on = c(ID = "ID", BF_YEAR = "YEAR"), DGM := i.DGM]
doc_diary[, BF_YEAR := NULL]

doc_couple_type <- readRDS("data/doc_couple_type_ear_is_child.RDS")
doc_diary[, BF_YEAR := year(DATE)-1]
doc_diary[doc_couple_type, on = c(ID = "ID", BF_YEAR = "YEAR"), COUPLE_TYPE := i.COUPLE_TYPE]
doc_diary[, BF_YEAR := NULL]

# reproducible sampling
set.seed(123)

# time-window setup
window_start <- -30
window_end <- 30
date_window <- seq(window_start, window_end)

# columns from doc_diary to analysisData
join_cols <- c(
  "TOTAL_DOT", "N_VISIT", "WORK_DAY",
  "LAST_C_AGE", "FUNC_TYPE", "N_DOC",
  "DOC_STATUS", "PARENT", "FEMALE", "COUPLE_TYPE"
)

# you can choose from: SICK_C, INPATIENT_C, EMER_C, MINOR_C
treatment_var <- "INPATIENT_C"  # change this var for treatment def.

# Decide control type never or sick_c==0
USE_NEVER <- T

# Use inpatient or not
USE_INPATIENT <- ifelse("INPATIENT_C" == treatment_var, TRUE, FALSE)

outcome_var <- c("TOTAL_DOT", "N_VISIT", "WORK_DAY")

# which columns define your sampling strata
# add `DGM` if need heterogoneity analysis
stratify_cols <- c("DATE", "LAST_C_AGE", "N_DOC", "COUPLE_TYPE")

if (USE_INPATIENT) {
  selected_cols <- c("ID", treatment_var, stratify_cols, "IN_DATE")
} else {
  selected_cols <- c("ID", treatment_var, stratify_cols)
}

# how many control units do you want per treated unit?
oversample_mul <- 5

# Functions -----------------------------------------------------------------------------------

make_panel <- function(events_dt, ori_dt, window, join_cols) {
  # events_dt: dt of (ID, DATE, SICK_C)
  # ori_dt:  full diary dt
  # window: integer vector of days
  # join_cols:   character vector to join back
  
  # 1) Cartesian expand: each event × each relative_day
  panel <- events_dt[
    CJ(ID = unique(ID), relative_day = window),
    on = "ID", allow.cartesian = TRUE
  ][, DATE := DATE + relative_day]
  panel[, WEEKDAY := as.factor(wday(DATE, week_start = 1))]
  
  # 2) Join back all desired measurements in one go
  panel[
    ori_dt,
    on = .(ID, DATE),
    (join_cols) := mget(sprintf("i.%s", join_cols))
  ]
  
  return(panel)
}

# Main ----------------------------------------------------------------------------------------
# ─────────────────────────────────────────────────────────────
# 1) Build treated panel
# ─────────────────────────────────────────────────────────────

# original dataset from doc_diary
treated_events <- doc_diary[get(treatment_var) == 1, ..selected_cols]

# Only When Using INPATIENT as Event
if (USE_INPATIENT) {
  treated_events <- treated_events[
    !duplicated(treated_events[, .(ID, IN_DATE)])
    ]
}

# expand treated_events by date_window
treated_panel <- make_panel(
  events_dt   = treated_events,
  ori_dt    = doc_diary,
  window = date_window,
  join_cols   = join_cols
)
treated_panel[, GROUP := "Treated"] 

# ─────────────────────────────────────────────────────────────
# 2) Build control panel with stratified oversampling
# ─────────────────────────────────────────────────────────────

# 2a) define control candidate pool

if (USE_NEVER) {
  control_candidates <- doc_diary[!ID %in% treated_panel$ID, ..selected_cols]
  message("Now using never treated group as control group")
} else {
  control_candidates <- doc_diary[SICK_C == 0, ..selected_cols]
  message("Now using sick_c == 0 group as control group")
}

# 2b) compute how many to sample per stratum from treated
strata_counts <- treated_events[
  , .N, by = stratify_cols
][
  , N := N * oversample_mul
]

# 2c) perform stratified random sampling
setkeyv(control_candidates, stratify_cols)
setkeyv(strata_counts, stratify_cols)

control_sampled <- strata_counts[
  control_candidates,    # left join ensures only strata present in treated are sampled
  nomatch = 0
][
  ,
  .SD[sample(.N, min(N[1], .N))],        # cap at available rows
  by = stratify_cols
][
  , N := NULL                              # drop temporary N column
]

# 2d) expand panel and join
control_panel <- make_panel(
  events_dt     = control_sampled[, .(ID, DATE)],
  ori_dt      = doc_diary,
  window = date_window,
  join_cols     = join_cols
)
control_panel[, GROUP := "Control"]   # mark as control
control_panel[, (treatment_var) := 0]

# ─────────────────────────────────────────────────────────────
# 3) Combine & cleanup
# ─────────────────────────────────────────────────────────────

# ensure columns align
if (USE_INPATIENT) {
  treated_panel[, IN_DATE := NULL]
}

merge_col <- c(colnames(control_panel))

# final analysis dataset
analysisData <- rbindlist(
  list(treated_panel[, ..merge_col], control_panel),
  use.names = TRUE
)

# remove intermediates
rm(
  treated_events, treated_panel,
  control_candidates, strata_counts,
  control_sampled, control_panel,
  doc_couple_type
)
