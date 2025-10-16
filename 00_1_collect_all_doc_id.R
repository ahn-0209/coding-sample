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

