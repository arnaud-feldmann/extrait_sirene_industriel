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

activites_industrielles <- c("18.13Z" = "Activités de pré-presse",
                             "18.12Z" = "Autre imprimerie (labeur)",
                             "10.71C" = "Boulangerie et boulangerie-pâtisserie",
                             "33.20C" = "Conception d'ensemble et assemblage sur site industriel d'équipements de contrôle des processus industriels",
                             "30.20Z" = "Construction de locomotives et d'autre matériel ferroviaire roulant",
                             "25.50B" = "Découpage, emboutissage",
                             "29.32Z" = "Fabrication d'autres équipements automobiles",
                             "28.29B" = "Fabrication d'autres machines d'usage général",
                             "25.73Z" = "Fabrication d'autres outillages",
                             "28.23Z" = "Fabrication de machines et d'équipements de bureau (à l'exception des ordinateurs et équipements périphériques",
                             "27.12Z" = "Fabrication de matériel de distribution et de commande électrique",
                             "32.50A" = "Fabrication de matériel médico-chirurgical et dentaire",
                             "28.11Z" = "Fabrication de moteurs et turbines, à l'exception des moteurs d'avions et de véhicules",
                             "27.11Z" = "Fabrication de moteurs, génératrices et transformateurs électriques",
                             "20.42Z" = "Fabrication de parfums et de produits pour la toilette",
                             "21.20Z" = "Fabrication de préparations pharmaceutiques",
                             "26.40Z" = "Fabrication de produits électroniques grand public",
                             "14.14Z" = "Fabrication de vêtements de dessous",
                             "14.13Z" = "Fabrication de vêtements de dessus",
                             "26.30Z" = "Fabrication d'équipements de communication",
                             "28.29A" = "Fabrication d'équipements d'emballage, de conditionnement et de pesage",
                             "29.31Z" = "Fabrication d'équipements électriques et électroniques automobiles",
                             "26.51B" = "Fabrication d'instrumentation scientifique et technique",
                             "10.71A" = "Fabrication industrielle de pain et de pâtisserie fraîche",
                             "18.11Z" = "Imprimerie de journaux",
                             "33.20B" = "Installation de machines et équipements mécaniques",
                             "25.62B" = "Mécanique industrielle",
                             "24.45Z" = "Métallurgie des autres métaux non ferreux",
                             "10.71D" = "Pâtisserie",
                             "10.13A" = "Préparation industrielle de produits à base de viande",
                             "25.61Z" = "Traitement et revêtement des métaux",
                             "10.83Z" = "Transformation du thé et du café",
                             "10.11Z" = "Transformation et conservation de la viande de boucherie")

sirene <-
  here("input", "StockEtablissement_utf8.csv") %>%
  read_csv_arrow(col_select =  c("siret", "siren", "trancheEffectifsEtablissement", "activitePrincipaleEtablissement", "codePostalEtablissement", "etatAdministratifEtablissement",
                                 "enseigne1Etablissement", "enseigne2Etablissement", "enseigne3Etablissement", "denominationUsuelleEtablissement"),
                 col_types =  schema(siret = string(),
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
           str_sub(codePostalEtablissement, 1L, 2L) %in% c("75", "77", "78", "91", "92", "93", "94", "95") &
           activitePrincipaleEtablissement %in% names(activites_industrielles)) %>%
  mutate(across(starts_with("enseigne"), ~if_else(is.na(.x),"",.x))) %>%
  mutate(enseigneEtablissement = str_c(enseigne1Etablissement,
                                       enseigne2Etablissement,
                                       enseigne3Etablissement)) %>%
  select(-c(etatAdministratifEtablissement,
            enseigne1Etablissement,
            enseigne2Etablissement,
            enseigne3Etablissement))
