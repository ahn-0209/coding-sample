#' @title   Compile comprehensive list of practicing doctors across medical systems
#' @created 2025-05-25
#' @updated 2025-MM-DD
#'
#' @description
#' This script extracts unique doctor identifiers from outpatient visit datasets
#' across three medical systems—western medicine, dentistry, and Chinese medicine—
#' to construct a comprehensive list of all practicing physicians in Taiwan.
#'
#' @details
#' **Inputs**
#' - Outpatient visit parquet directories under `E:/H114028/CleanData/processed_data/doctor-opdte/`
#'   containing subfolders:
#'   - `western/`
#'   - `dentist/`
#'   - `chinese/`
#'
#' **Processing Steps**
#' 1. Load all parquet files from the three directories.
#' 2. Select the `PRSN_ID` column representing unique doctor identifiers.
#' 3. Deduplicate IDs across all files.
#'
#' **Outputs**
#' - `all_doc_id.rds`: vector of all unique doctor IDs used for downstream analyses
#'
#' @notes
#' This object serves as a master reference for identifying doctor-parent households
#' and linking physician-related datasets in subsequent scripts.

#opdte files

files <- c(list.files("E:/H114028/CleanData/processed_data/doctor-opdte/dentist/", full.names = T),
           list.files("E:/H114028/CleanData/processed_data/doctor-opdte/chinese/", full.names = T),
           list.files("E:/H114028/CleanData/processed_data/doctor-opdte/western/", full.names = T))

all_doc_id <- list()

for (i in 1:length(files)) {
  all_doc_id[[i]] <- open_dataset(files[i]) %>% 
    select(PRSN_ID) %>% 
    unique() %>% 
    collect() %>% 
    setDT()
}

all_doc_id <- rbindlist(all_doc_id)
all_doc_id <- unique(all_doc_id)
all_doc_id <- all_doc_id$PRSN_ID

saveRDS(all_doc_id, "data/all_doc_id.rds")



