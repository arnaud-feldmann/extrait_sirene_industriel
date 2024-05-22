library(here)
library(arrow)
library(dplyr)

etabs <- file.path(tempdir(),"StockEtablissement_utf8.zip")
# Sirene stock etab (https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)
download.file("https://www.data.gouv.fr/fr/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576",
              etabs,
              method = "wget",
              quiet = TRUE)
unzip(etabs,
      exdir = here("input"))


sirene <-
  here("input", "StockEtablissement_utf8.csv") %>%
  read_csv_arrow(col_select = schema(siret = string(),
                                     siren = string(),
                                     trancheEffectifsEtablissement = string(),
                                     activitePrincipaleEtablissement = string(),
                                     codePostalEtablissement = string(),
                                     etatAdministratifEtablissement = string(),
                                     enseigne1Etablissement = string(),
                                     enseigne2Etablissement = string(),
                                     enseigne3Etablissement = string(),
                                     denominationUsuelleEtablissement = string()),
                 as_data_frame = FALSE) %>%
  filter((is.na(etatAdministratifEtablissement) | etatAdministratifEtablissement == "A") &
           trancheEffectifsEtablissement != "NN" &
           str_sub(codePostalEtablissement, 1L, 2L) %in% c("75", "77", "78", "91", "92", "93", "94", "95")) %>%
  mutate(across(starts_with("enseigne"), ~if_else(is.na(.x),"",.x))) %>%
  mutate(enseigneEtablissement = str_c(enseigne1Etablissement,
                                       enseigne2Etablissement,
                                       enseigne3Etablissement)) %>%
  select(-c(etatAdministratifEtablissement,
            enseigne1Etablissement,
            enseigne2Etablissement,
            enseigne3Etablissement))
