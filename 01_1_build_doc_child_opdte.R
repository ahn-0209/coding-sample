#' @title   Identify doctor-parent children and build OPD visit list
#' @author  Chen-An Lien
#' @created 2025-05-25
#' @updated 2025-MM-DD
#'
#' @description
#' This script identifies children whose parents are both doctors
#' and computes annual statistics of such doctor-parent births.
#' It also loads outpatient visit records for these children from 2004 to 2021.
#'
#' @details
#' **Inputs**
#' - `H_MOHW_MCHD109.csv`: birth registry with IDs of mother and father
#' - Outpatient visit parquet files: `H_NHI_OPDTE[year][month]_10.parquet`
#' - `all_doc_id`: vector of all valid doctor IDs
#'
#' **Outputs**
#' - `doc_child`: children whose both parents are doctors
#' - `child_parent_number_by_year.csv`: per-year summary of counts
#' - `doc_child_opdte`: list of children’s visit data by year

# Setups ------------------------------------------------------------------

rm(list = ls()); gc()
# setwd(here::here())

# Load Package ---------------------------------------------------------------

library(data.table)
library(dplyr)
library(arrow)
library(lubridate)

# Load Data ---------------------------------------------------------------

doc_child <- readRDS("data/doc_child.rds")

# Parameter ---------------------------------------------------------------

year_span <- 2000:2022
roc_year_span <- 89:111
parquet_path <- "E:/H114028/data/parquet/"

# Function: Load OPD visit records by year-month --------------------------------

load_child_opdte <- function(id_vector, years, base_path, pattern_func) {
  opd_list <- list()
  
  for (i in seq_along(years)) {
    y <- years[i]
    this_yr_record <- list()
    message(sprintf("Start %s data", y))
    
    for (m in sprintf("%02d", 1:12)) {
      parquet_path <- file.path(base_path, pattern_func(y, m))
      this_yr_record[[m]] <- open_dataset(parquet_path) %>%
        filter(ID %in% id_vector) %>%
        mutate(FUNC_DATE := ymd(FUNC_DATE)) %>%
        collect() %>%
        setDT()
      message(sprintf("Finished %s%s data", y, m))
    }
    
    opd_list[[i]] <- rbindlist(this_yr_record, use.names = TRUE)
  }
  
  return(opd_list)
}

# Load OPD visit records for each child -----------------------------------------
sick_child <- doc_child[Yr %in% roc_year_span]$ID_C
doc_child_opdte <- load_child_opdte(
  id_vector = sick_child,
  years = year_span,
  base_path = parquet_path,
  pattern_func = function(y, m) paste0("H_NHI_OPDTE", y, m, "_10.parquet")
)
# doc_child_opdte[, FUNC_DATE := ymd(FUNC_DATE)]

saveRDS(doc_child_opdte, file = "data/doc_child_opdte_list.rds")


