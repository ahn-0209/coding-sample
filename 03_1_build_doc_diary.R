#' @title   Build physician-day panel (doc_diary) from linked administrative datasets
#' @created 2025-05-25
#' @updated 2025-MM-DD
#'
#' @description
#' This script constructs a daily-level physician panel (`doc_diary`) by linking and harmonizing 
#' multiple administrative datasets on outpatient visits, child illness, hospitalizations, 
#' physician demographics, and family structure. The resulting dataset traces each physician's 
#' work activities, parental status, and family-related health events from 2000 to 2022.
#'
#' @details
#' **Inputs**
#' - `doc_child.rds`: child–parent linkage file identifying doctor-parent households  
#' - `DOC_PRSN_INFO.rds`: physician demographic information  
#' - `doc_opdte.rds`: daily outpatient service summaries  
#' - `doc_child_opdte_list.rds`: outpatient visits of doctors’ children  
#' - `doc_child_ipdte_daily.rds`: daily child hospitalization records  
#' - `reference/icd.csv`: ICD9–ICD10 crosswalk table  
#' - `utils_doc_diary.R`: modular utility functions for tagging and mapping
#'
#' **Processing Steps**
#' 1. Create a full physician–date skeleton (2000–2022).  
#' 2. Merge outpatient workload summaries to define workdays and practice types.  
#' 3. Link child illness and inpatient events to tag daily parental caregiving indicators.  
#' 4. Add demographic attributes, first/last child timing, and spouse identifiers.  
#' 5. Map ICD codes, tag emergency visits, and adjust for self-child visits.  
#' 6. Flag physicians’ first active year, hospital affiliation, and multi-hospital workdays.
#'
#' **Outputs**
#' - `doc_diary.rds`: daily-level panel of physicians containing demographics, workload, 
#'   family status, and child health interactions
#'
#' @notes
#' The resulting panel supports analyses of labor supply, caregiving behavior, and 
#' work–family tradeoffs among physician households.

# Setups ------------------------------------------------------------------
rm(list = ls()); gc()
library(data.table)
library(arrow)
library(dplyr)
library(lubridate)
library(zoo)
library(tidyverse)
library(tidyr)
source("code/utility/utils_doc_diary.R")
icd <- fread("reference/icd.csv")

# Load data ---------------------------------------------------------------

doc_child <- readRDS("data/doc_child.rds")

doc_prsn_info <- readRDS("data/DOC_PRSN_INFO.rds")

doc_child_opdte <- readRDS("data/doc_child_opdte_list.rds")

doc_child_opdte <- rbindlist(doc_child_opdte, use.names = T, fill = T)

doc_child_opdte <- doc_child_opdte[, .(ID, PRSN_ID, HOSP_ID,
                                       FUNC_DATE, CASE_TYPE, FUNC_TYPE,
                                       GAVE_KIND, ICD9CM_1, T_DOT, 
                                       DATA_YEAR, DATA_MONTH)]

doc_child_opdte[, MAIN_CASE_TYPE := {
  if ("02" %in% CASE_TYPE) {
    "02"
  } else {
    CASE_TYPE[which.max(T_DOT)]
  }
}, by = .(ID, FUNC_DATE)]

doc_child_opdte <- doc_child_opdte[CASE_TYPE == MAIN_CASE_TYPE]

doc_opdte <- readRDS("data/doc_opdte.rds")
doc_opdte[, ":="(
  TOTAL_DOT = sum(total_dots),
  TOTAL_PART_DOT = sum(total_partial_dots),
  N_VISIT = sum(n_visits),
  WORK_DAY = 1),
  by = .(PRSN_ID, FUNC_DATE)
]

doc_opdte <- doc_opdte[, .(PRSN_ID, HOSP_ID, FUNC_DATE,
                           DATA_YEAR, DATA_MONTH,
                           MAIN_FUNC_TYPE, TOTAL_DOT,
                           TOTAL_PART_DOT, N_VISIT)][!duplicated(doc_opdte[, .(PRSN_ID, FUNC_DATE)])]

doc_child_ipdte_daily <- readRDS("data/doc_child_ipdte_daily.rds")

# Generate diary skeleton -------------------------------------------------

doctor_id <- unique(doc_opdte$PRSN_ID)
dates <- generate_date_seq(start_date = "2000-01-01", end_date = "2022-12-31")
doc_diary <- CJ(ID = doctor_id, DATE = dates)
doc_diary[, `:=`(
  DATA_YEAR = year(DATE),
  DATA_MONTH = month(DATE),
  WDAY = lubridate::wday(DATE, week_start = 1)
)]
doc_diary[, WEEK_DAY := ifelse(WDAY %in% 6:7, 0, 1)]

# Add parent demographics -------------------------------------------------

doc_diary[doc_prsn_info, on = .(ID), BIRTH_YEAR := i.BIRTH_Y]
doc_diary[ID %in% doc_child$ID_M, FEMALE := 1L]
doc_diary[ID %in% doc_child$ID_F, FEMALE := 0L]
doc_diary[, `:=`(
  AGE = DATA_YEAR - BIRTH_YEAR,
  AGE_SQ = (DATA_YEAR - BIRTH_YEAR)^2
)]

# Add outpatient records summary ------------------------------------------

doc_opdte_daily <- doc_opdte[!duplicated(doc_opdte, by = c("PRSN_ID", "FUNC_DATE"))]
doc_diary[doc_opdte_daily, on = .(ID = PRSN_ID, DATE = FUNC_DATE), `:=`(
  TOTAL_DOT = i.TOTAL_DOT,
  TOTAL_PART_DOT = i.TOTAL_PART_DOT,
  N_VISIT = i.N_VISIT,
  FUNC_TYPE = i.MAIN_FUNC_TYPE,
  HOSP_ID = i.HOSP_ID
)]
nrow(doc_diary[is.na(FUNC_TYPE)])
doc_diary[, FUNC_TYPE := na.locf(FUNC_TYPE, na.rm = F), by = ID]  # fill empty func_type
doc_diary[, WORK_DAY := fifelse(!is.na(TOTAL_DOT), 1L, 0L)]
doc_diary[WORK_DAY == 0L, `:=`(TOTAL_DOT = 0, TOTAL_PART_DOT = 0, N_VISIT = 0)]

# Tag child illness -------------------------------------------------------

doc_child_opdte[doc_child, on = .(ID = ID_C), `:=`(ID_M = i.ID_M, ID_F = i.ID_F)]
doc_diary[doc_child_opdte, on = c(ID = "ID_M", DATE = "FUNC_DATE"),
          ":="(
            SICK_C = 1L,
            SICK_C_ID = i.ID,
            ICD9CM_1_C = i.ICD9CM_1,
            CASE_TYPE_C = i.CASE_TYPE,
            FUNC_TYPE_C = i.FUNC_TYPE,
            T_DOT_C = i.T_DOT)
         ]
doc_diary[doc_child_opdte, on = c(ID = "ID_F", DATE = "FUNC_DATE"),
          ":="(
            SICK_C = 1L,
            SICK_C_ID = i.ID,
            ICD9CM_1_C = i.ICD9CM_1,
            CASE_TYPE_C = i.CASE_TYPE,
            FUNC_TYPE_C = i.FUNC_TYPE,
            T_DOT_C = i.T_DOT)
]
doc_diary[is.na(SICK_C), SICK_C := 0L]

# ICD Mapping -------------------------------------------------------------

doc_diary <- map_icd_codes(doc_diary, icd_table = icd, icd_col = "ICD9CM_1_C", year_col = "DATA_YEAR")
doc_diary[CCS %in% c("1.5", "5.5", "17.2") | ICD_10 %in% c("H52.13", "H52.10", "H52.7"), SICK_C := 0L]
doc_diary[, MINOR_C := 0]  # document some minor illness, e.g. CCS = 8.1
doc_diary[CCS == 8.1, MINOR_C := 1]

# Tag first & last child, parenthood ----------------------------------------

setorder(doc_child, Yr)

birth_order <- tag_first_last_child_m(doc_child)
doc_diary[birth_order$first, on = .(ID = ID_M), FIRST_C := i.ID_C]
doc_diary[birth_order$last, on = .(ID = ID_M), LAST_C := i.ID_C]

birth_order <- tag_first_last_child_f(doc_child)
doc_diary[birth_order$first, on = .(ID = ID_F), FIRST_C := i.ID_C]
doc_diary[birth_order$last, on = .(ID = ID_F), LAST_C := i.ID_C]

doc_diary[doc_child, on = c(FIRST_C = "ID_C"), FIRST_C_BIRTH := i.Yr]
doc_diary[, `:=`(
  FIRST_C_AGE = as.numeric(DATA_YEAR) - as.numeric(FIRST_C_BIRTH),
  PARENT = fifelse(DATA_YEAR >= FIRST_C_BIRTH, 1L, 0L)
)]

doc_diary[doc_child, on = c(LAST_C = "ID_C"), LAST_C_BIRTH := i.Yr]
doc_diary[, `:=`(
  LAST_C_AGE = as.numeric(DATA_YEAR) - as.numeric(LAST_C_BIRTH)
)]

# Tag Spouse ID -----------------------------------------------------------

doc_diary[doc_child, on = c(LAST_C = "ID_C", ID = "ID_M"), ID_S := i.ID_F]
doc_diary[doc_child, on = c(LAST_C = "ID_C", ID = "ID_F"), ID_S := i.ID_M]

# Tag Child Inpatient -----------------------------------------------------------

doc_diary[doc_child_ipdte_daily, on = c(ID = "ID_M", DATE = "DATE"),
          ":="(
            INPATIENT_C = i.INPATIENT_C,
            IN_DATE = i.IN_DATE
          )]
doc_diary[doc_child_ipdte_daily, on = c(ID = "ID_F", DATE = "DATE"),
          ":="(
            INPATIENT_C = i.INPATIENT_C,
            IN_DATE = i.IN_DATE
          )]
doc_diary[is.na(INPATIENT_C), INPATIENT_C := 0L]

# Indicate Emergency Visit ------------------------------------------------

doc_diary[, EMER_C := 0]
doc_diary[CASE_TYPE_C == "02", EMER_C := 1]

# Adjust for self-child visits --------------------------------------------

doc_child_opdte[, `:=`(
  DOC_IS_MOM = as.integer(PRSN_ID == ID_M),
  DOC_IS_DAD = as.integer(PRSN_ID == ID_F)
)]
doc_diary[doc_child_opdte, on = .(ID = ID_F, DATE = FUNC_DATE), DOC_IS_DAD := i.DOC_IS_DAD]
doc_diary[doc_child_opdte, on = .(ID = ID_M, DATE = FUNC_DATE, ID_S = ID_F), DOC_IS_DAD := i.DOC_IS_DAD]
doc_diary[doc_child_opdte, on = .(ID = ID_M, DATE = FUNC_DATE), DOC_IS_MOM := i.DOC_IS_MOM]
doc_diary[doc_child_opdte, on = .(ID = ID_F, DATE = FUNC_DATE, ID_S = ID_M), DOC_IS_MOM := i.DOC_IS_MOM]
doc_diary[, DOC_IS_PARENT := sum(DOC_IS_MOM, DOC_IS_DAD, na.rm = T), by = .(ID, DATE)]
doc_diary <- adjust_self_visit(doc_diary)

# Remap fine-grained specialties ------------------------------------------
doc_diary <- remap_func_types(doc_diary)


# Tag first year becoming a doctor ----------------------------------------

setorder(doc_opdte, FUNC_DATE)
first_time_doc <- doc_opdte[!duplicated(doc_opdte$PRSN_ID)]
doc_diary[first_time_doc, on = c(ID = "PRSN_ID"), FIRST_TIME := i.FUNC_DATE]
doc_diary[, DOC_STATUS := ifelse(DATE < FIRST_TIME, 0, 1)] 
doc_diary[, DOC_Y := year(FIRST_TIME)]


# Bring back HOSP_ID ------------------------------------------------------

doc_diary <- doc_diary %>% 
  group_by(ID) %>% 
  fill(HOSP_ID, .direction = "down")
setDT(doc_diary)

doc_diary <- doc_diary %>% 
  group_by(ID) %>% 
  fill(HOSP_ID, .direction = "up")
setDT(doc_diary)

doc_diary[DOC_STATUS == 0, HOSP_ID := NA]


# Multiple Hospital or Not ------------------------------------------------

doc_opdte_multiple <- doc_opdte[, c("FUNC_DATE", "HOSP_ID", "PRSN_ID")]
doc_opdte_multiple[, N_DOC := .N, by = .(FUNC_DATE, HOSP_ID)]
doc_opdte_multiple <- doc_opdte_multiple[N_DOC > 1]

doc_diary[doc_opdte_multiple, on = c(HOSP_ID = "HOSP_ID", DATE = "FUNC_DATE"),
          "N_DOC" := i.N_DOC]
doc_diary[!is.na(N_DOC), N_DOC := 1]
doc_diary[is.na(N_DOC), N_DOC := 0]


# Save output by year -----------------------------------------------------
# for (yr in 2000:2021) {
#   write_parquet(doc_diary[DATA_YEAR == yr], paste0("R data/doc_diary/doc_diary_", yr, ".parquet"))
# }

# Optional: save full RDS
saveRDS(doc_diary, "data/doc_diary.rds")

rm(list = ls()[!(ls() %in% c("doc_diary"))]); gc()


