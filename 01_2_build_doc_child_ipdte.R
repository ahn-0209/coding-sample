#' @title   Extract IPD records for doctor children and create daily stay panel
#' @author  Chen-An Lien
#' @created 2025-05-25
#' @updated 2025-MM-DD
#'
#' @description
#' This script loads inpatient (IPD) records for children whose parents are both doctors,
#' harmonizes field names across years, calculates hospitalization length,
#' and produces both episode-level and day-level IPD panels.
#'
#' @details
#' **Inputs**
#' - `doc_child.RData`: child-parent linkage table, includes `ID_C`, `ID_M`, `ID_F`
#' - `H_NHI_IPDTE[year].parquet`: yearly inpatient datasets from NHI database
#'
#' **Outputs**
#' - `doc_child_ipdte.rds`: cleaned episode-level IPD records
#' - `doc_child_ipdte_daily.rds`: daily-level stay panel from IN_DATE to OUT_DATE

# Setups ------------------------------------------------------------------

rm(list = ls()); gc()
library(data.table)
library(arrow)
library(lubridate)

doc_child <- readRDS("data/doc_child.rds")  # contains ID_C, ID_M, ID_F

# Function to standardize field names -------------------------------------
standardize_ipdte_cols <- function(dt) {
  setnames(dt, old = c("icd9cm_1", "icd_op_code1", "med_dot", "part_dot"),
           new = c("ICD9CM_1", "ICD_OP_CODE1", "MED_DOT", "PART_DOT"),
           skip_absent = TRUE)
  return(dt)
}

# Load and harmonize IPD data ---------------------------------------------

ipd_years <- 89:111
ipd_list <- vector("list", length(ipd_years))

for (i in seq_along(ipd_years)) {
  y <- ipd_years[i]
  message(sprintf("Start %s data", y))
  
  ipd_dt <- open_dataset(sprintf("E:/H114028/data/parquet/H_NHI_IPDTE%d.parquet", y)) %>%
    filter(ID %in% doc_child$ID_C) %>%
    select(ID, PRSN_ID, HOSP_ID, IN_DATE, OUT_DATE,
           HOSP_ID
           # , icd9cm_1, icd_op_code1, DRUG_DOT,
           # med_dot, part_dot, ICD9CM_1, ICD_OP_CODE1, MED_DOT, PART_DOT
           ) %>%
    collect() %>%
    setDT()
  ipd_dt[, DATA_YEAR := y + 1911]
  
  ipd_list[[i]] <- standardize_ipdte_cols(ipd_dt)
  message(sprintf("Finished %s data", y))
}

# Combine and clean IPD data ---------------------------------------------

doc_child_ipdte <- rbindlist(ipd_list, use.names = TRUE, fill = TRUE)
doc_child_ipdte <- doc_child_ipdte[!duplicated(doc_child_ipdte[, .(ID, IN_DATE)], fromLast = TRUE)]
doc_child_ipdte[, IN_DATE := ymd(IN_DATE)]
doc_child_ipdte[, OUT_DATE := ymd(OUT_DATE)]

doc_child_ipdte[doc_child, on = c(ID = "ID_C"), `:=`(ID_M = i.ID_M, ID_F = i.ID_F)]
doc_child_ipdte <- doc_child_ipdte[!is.na(OUT_DATE)]
saveRDS(doc_child_ipdte, "data/doc_child_ipdte.rds")

# Expand to daily-level stay panel ----------------------------------------

daily_list <- vector("list", nrow(doc_child_ipdte))

for (i in seq_len(nrow(doc_child_ipdte))) {
  rec <- doc_child_ipdte[i]
  stay_dates <- seq(rec$IN_DATE, rec$OUT_DATE, by = "day")
  daily_list[[i]] <- data.table(ID = rec$ID, DATE = stay_dates, IN_DATE = rec$IN_DATE)
}

doc_child_ipdte_daily <- rbindlist(daily_list)
doc_child_ipdte_daily[doc_child, on = c(ID = "ID_C"), `:=`(ID_M = i.ID_M, ID_F = i.ID_F)]
doc_child_ipdte_daily[, INPATIENT_C := 1]
saveRDS(doc_child_ipdte_daily, "data/doc_child_ipdte_daily.rds")
