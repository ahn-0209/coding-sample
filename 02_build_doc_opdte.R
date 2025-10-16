#' @title   Build doctor-parent weekly panel
#' @author  Chen-An Lien
#' @created 2025-05-24
#' @updated 2025-MM-DD
#'
#' @description
#' This script loads parental IDs from `doc_child`,
#' collects enrollment and outpatient visit records from 2000–2021,
#' and constructs a weekly panel for all doctor-parents.
#' Enrollment includes birth year and monthly insured salary;
#' outpatient data includes visit dates, departments, and hospitals.
#'
#' @details
#' **Inputs**
#' - `doc_child`: includes columns `ID`, `ID_M`, `ID_F`, `Yr`
#' - enrollment files (parquet): from western, Chinese, and dental doctors
#' - outpatient visit files (parquet): same three specialties
#'
#' **Outputs**
#' - `doc_enroll` (data.table): doctor-level monthly panel with birth year, salary
#' - `doc_opdte` (data.table): doctor-level visit panel with visit year/month
#' the result is by `ID, FUNC_DATE, HOSP_ID, FUNC_TYPE` to aggregate 
#' in order to calculate the correct daily presentation
#' need to sum up again by ID, FUNC_DATE

# Setups ------------------------------------------------------------------

rm(list = ls()); gc()
library(data.table)
library(arrow)
library(lubridate)
library(dplyr)

# setwd(here::here())

# Load child-parent linkage --------------------------------------------------

doc_child <- readRDS("data/doc_child.rds")  # contains ID_M, ID_F, ID_C
doc_child <- doc_child[Yr %in% 2000:2022]
parent_ids <- unique(c(doc_child$ID_M, doc_child$ID_F))

prsn_info <- open_dataset("E:/H114028/CleanData/processed_data/PersInfo.parquet") %>% 
  filter(ID %in% parent_ids) %>% 
  select(ID, ID_S, ID_BIRTHDAY) %>% 
  collect() %>% 
  setDT()

prsn_info <- prsn_info[, BIRTH_Y := year(ymd(ID_BIRTHDAY))][, .(ID, ID_S, BIRTH_Y)]

saveRDS(prsn_info, "data/DOC_PRSN_INFO.rds")

# Load enrollment files ------------------------------------------------------

# enroll_dirs <- list(
#   western = "R Data/doctor-enrol",
#   chinese = "E:/H113038/liangcheng/data/processed/chinese-doctor-enrol",
#   dentist = "E:/H113038/liangcheng/data/processed/dentist-enrol"
# )
# 
# enroll_files <- unlist(lapply(enroll_dirs, list.files, full.names = TRUE))
# 
# doc_enroll <- rbindlist(
#   lapply(enroll_files, function(fp) {
#     open_dataset(fp) %>%
#       filter(ID %in% parent_ids) %>%
#       collect() %>%
#       setDT()[, `:=`(
#         DATA_YEAR  = year(ym(PREM_YM)),
#         DATA_MONTH = month(ym(PREM_YM)),
#         BIRTH_YEAR = year(ymd(ID_BIRTHDAY))
#       )]
#   }),
#   fill = TRUE
# )
# 
# doc_enroll[, ID1_MARK := as.integer(ID == ID1)]



# Load outpatient visit files ------------------------------------------------

opd_dirs <- list(
  western = "E:/H114028/CleanData/processed_data/doctor-opdte/western/",
  chinese = "E:/H114028/CleanData/processed_data/doctor-opdte/chinese/",
  dentist = "E:/H114028/CleanData/processed_data/doctor-opdte/dentist/"
)

opd_files <- unlist(lapply(opd_dirs, list.files, full.names = TRUE))

doc_opdte <- rbindlist(
  lapply(opd_files, function(fp) {
    open_dataset(fp) %>%
      filter(PRSN_ID %in% parent_ids) %>%
      collect() %>%
      setDT()
  }),
  fill = TRUE
)

doc_opdte[, `:=`(
  DATA_YEAR  = year(ymd(FUNC_DATE)),
  DATA_MONTH = month(ymd(FUNC_DATE))
)]

doc_opdte <- doc_opdte[DATA_YEAR %in% 2000:2022]
doc_opdte[, FUNC_DATE := ymd(FUNC_DATE)]
doc_opdte[, MAIN_FUNC_TYPE := FUNC_TYPE[which.max(total_dots)], by = .(PRSN_ID, FUNC_DATE)]

# Save intermediate files if needed
# save(doc_enroll, file = "R Data/doc_enroll.RData")
saveRDS(doc_opdte, file = "data/doc_opdte.rds")

# the result is by `ID, FUNC_DATE, HOSP_ID, FUNC_TYPE` to aggregate 
# in order to calculate the correct daily presentation
# need to sum up again by ID, FUNC_DATE