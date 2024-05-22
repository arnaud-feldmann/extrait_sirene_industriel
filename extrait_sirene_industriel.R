library(here)
library(arrow)
library(dplyr)
library(readxl)
library(stringr)

temp_dir <- tempdir()
etabs <- file.path(temp_dir,"StockEtablissement_utf8.zip")
ent <- file.path(temp_dir,"StockUniteLegale_utf8.zip")

# Sirene stock etab (https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)
download.file("https://www.data.gouv.fr/fr/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576",
              etabs,
              method = "wget",
              quiet = TRUE)
unzip(etabs,
      exdir = here("input"))

download.file("https://www.data.gouv.fr/fr/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047",
              ent,
              method = "wget",
              quiet = TRUE)
unzip(ent,
      exdir = here("input"))

download.file("https://www.insee.fr/fr/statistiques/fichier/2120875/naf2008_liste_n5.xls",
              here("input", "naf2008_liste_n5.xls"),
              method = "wget",
              quiet = TRUE)

activites_industrielles <-
  tribble(~activitePrincipaleUniteLegale, ~activitePrincipaleUniteLegaleLabel,
          "18.13Z", "Activités de pré-presse",
          "18.12Z", "Autre imprimerie (labeur)",
          "10.71C", "Boulangerie et boulangerie-pâtisserie",
          "33.20C", "Conception d'ensemble et assemblage sur site industriel d'équipements de contrôle des processus industriels",
          "30.20Z", "Construction de locomotives et d'autre matériel ferroviaire roulant",
          "25.50B", "Découpage, emboutissage",
          "29.32Z", "Fabrication d'autres équipements automobiles",
          "28.29B", "Fabrication d'autres machines d'usage général",
          "25.73Z", "Fabrication d'autres outillages",
          "28.23Z", "Fabrication de machines et d'équipements de bureau (à l'exception des ordinateurs et équipements périphériques",
          "27.12Z", "Fabrication de matériel de distribution et de commande électrique",
          "32.50A", "Fabrication de matériel médico-chirurgical et dentaire",
          "28.11Z", "Fabrication de moteurs et turbines, à l'exception des moteurs d'avions et de véhicules",
          "27.11Z", "Fabrication de moteurs, génératrices et transformateurs électriques",
          "20.42Z", "Fabrication de parfums et de produits pour la toilette",
          "21.20Z", "Fabrication de préparations pharmaceutiques",
          "26.40Z", "Fabrication de produits électroniques grand public",
          "14.14Z", "Fabrication de vêtements de dessous",
          "14.13Z", "Fabrication de vêtements de dessus",
          "26.30Z", "Fabrication d'équipements de communication",
          "28.29A", "Fabrication d'équipements d'emballage, de conditionnement et de pesage",
          "29.31Z", "Fabrication d'équipements électriques et électroniques automobiles",
          "26.51B", "Fabrication d'instrumentation scientifique et technique",
          "10.71A", "Fabrication industrielle de pain et de pâtisserie fraîche",
          "18.11Z", "Imprimerie de journaux",
          "33.20B", "Installation de machines et équipements mécaniques",
          "25.62B", "Mécanique industrielle",
          "24.45Z", "Métallurgie des autres métaux non ferreux",
          "10.71D", "Pâtisserie",
          "10.13A", "Préparation industrielle de produits à base de viande",
          "25.61Z", "Traitement et revêtement des métaux",
          "10.83Z", "Transformation du thé et du café",
          "10.11Z", "Transformation et conservation de la viande de boucherie") %>%
  union_all(here("input", "naf2008_liste_n5.xls") %>%
              read_xls(skip = 2L) %>%
              rename(activitePrincipaleUniteLegale = Code,
                     activitePrincipaleUniteLegaleLabel = Libellé) %>%
              filter(str_sub(activitePrincipaleUniteLegale, 1L, 2L) >= "10" &
                       str_sub(activitePrincipaleUniteLegale, 1L, 2L) <= "33")) %>%
  as_arrow_table()

tranches_concernees <-
  tribble(
    ~trancheEffectifsUniteLegale, ~trancheEffectifsUniteLegaleLabel,
    "21", "50 à 99 salariés",
    "22", "100 à 199 salariés",
    "31", "200 à 249 salariés",
    "32", "250 à 499 salariés"
  )

sirene_ent <-
  here("input", "StockUniteLegale_utf8.csv") %>%
  read_csv_arrow(col_select =  c("siren", "trancheEffectifsUniteLegale", "activitePrincipaleUniteLegale", "etatAdministratifUniteLegale"),
                 col_types =  schema(siren = string(),
                                     trancheEffectifsUniteLegale = string(),
                                     activitePrincipaleUniteLegale = string(),
                                     etatAdministratifUniteLegale = string()),
                 as_data_frame = FALSE) %>%
  filter(is.na(etatAdministratifUniteLegale) | etatAdministratifUniteLegale == "A") %>%
  select(-activitePrincipaleUniteLegale) %>%
  inner_join(tranches_concernees, by = "trancheEffectifsUniteLegale") %>%
  inner_join(activites_industrielles, by = "activitePrincipaleUniteLegale")

sirene_etab <-
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
  mutate(dep =  str_sub(codePostalEtablissement, 1L, 2L)) %>%
  filter((is.na(etatAdministratifEtablissement) | etatAdministratifEtablissement == "A") &
           trancheEffectifsEtablissement != "NN" &
           dep %in% c("75", "77", "78", "91", "92", "93", "94", "95")) %>%
  mutate(across(starts_with("enseigne"), ~if_else(is.na(.x),"",.x)),
         enseigneEtablissement = str_c(enseigne1Etablissement,
                                       enseigne2Etablissement,
                                       enseigne3Etablissement)) %>%
  select(c(dep, siret, siren, trancheEffectifsEtablissementLabel, activitePrincipaleEtablissementLabel,
           enseigneEtablissement, denominationUsuelleEtablissement)) %>%
  compute()

production_dep <- function(depcode) {
  dir.create(here("output"), showWarnings = FALSE)
  
  sirene %>%
    filter(dep == depcode) %>%
    select(-dep) %>%
    arrange(siret) %>%
    write_csv_arrow(here("output", paste0("extraction_etablissements", depcode, ".csv")))
}

production_dep("75")
production_dep("77")
production_dep("78")
production_dep("91")
production_dep("92")
production_dep("93")
production_dep("94")
production_dep("95")
