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
#' - `doc_child` (data.table): list of children born to doctor couples with key identifiers
#' - `child_parent_number_by_year.csv`: annual count of such children and unique doctor-parent pairs

# Setups ------------------------------------------------------------------

rm(list = ls()); gc()
# setwd(here::here())

# Load Package ---------------------------------------------------------------
library(data.table)
library(dplyr)
library(arrow)
library(lubridate)

# Load and filter children with doctor parents ----------------------------------

all_doc_id <- readRDS("data/all_doc_id.rds")

# Using ENROL_RELATION
enrol_rela <- open_dataset("E:/H114028/CleanData/processed_data/enrol_relation.parquet") %>% 
  filter(F_ID %in% all_doc_id & M_ID %in% all_doc_id) %>% 
  collect() %>% 
  setDT()

# Read PRSN_INFO
prsn_info <- open_dataset("E:/H114028/CleanData/processed_data/PersInfo.parquet") %>% 
  filter(ID %in% c(enrol_rela$ID, enrol_rela$F_ID, enrol_rela$M_ID)) %>% 
  collect() %>% 
  setDT()

doc_child <- enrol_rela[, .(ID, F_ID, M_ID)]
colnames(doc_child) <- c("ID_C", "ID_F", "ID_M")
doc_child[prsn_info, on = c(ID_C = "ID"), BIRTH_C := i.ID_BIRTHDAY]
doc_child[prsn_info, on = c(ID_F = "ID"), BIRTH_F := i.ID_BIRTHDAY]
doc_child[prsn_info, on = c(ID_M = "ID"), BIRTH_M := i.ID_BIRTHDAY]
doc_child[prsn_info, on = c(ID_C = "ID"), ID_C_S := i.ID_S]
doc_child[, Yr := year(ymd(doc_child$BIRTH_C))]

saveRDS(doc_child, "data/doc_child.rds")
# birth_data <- fread("E:/H113038/data/H_MOHW_MCHD109.csv")
# birth_data <- birth_data[Yr %in% 2004:2020]
# load("R Data/all_doc_id.RData")
# 
# doc_child <- birth_data[ID_M %in% all_doc_id & ID_F %in% all_doc_id]
# saveRDS(doc_child, file = "R Data/doc_child.rds")

# Basic statistics --------------------------------------------------------------

n_doctor_parents <- doc_child %>% distinct(ID_M, ID_F) %>% nrow()

summary_by_year <- as.data.table(table(doc_child$Yr))
setnames(summary_by_year, c("Year", "Child_N"))
summary_by_year <- summary_by_year[Year %in% 2000:2022]

for (y in 2000:2022) {
  parent_count <- doc_child[Yr == y] %>% distinct(ID_M, ID_F) %>% nrow()
  summary_by_year[Year == y, Parent_N := parent_count]
}

fwrite(summary_by_year, "csv/child_parent_number_by_year.csv")
