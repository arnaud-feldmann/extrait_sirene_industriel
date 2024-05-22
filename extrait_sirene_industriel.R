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

naf <-
  here("input", "naf2008_liste_n5.xls") %>%
  read_xls(skip = 2L) %>%
  rename(activitePrincipale = Code,
         activitePrincipaleLabel = Libellé)

activites_industrielles <-
  naf %>%
  filter(str_sub(activitePrincipale, 1L, 2L) >= "10" &
           str_sub(activitePrincipale, 1L, 2L) <= "33" |
           activitePrincipale %in% c("18.13Z", #Activités de pré-presse
                                     "18.12Z", #Autre imprimerie (labeur)
                                     "10.71C", #Boulangerie et boulangerie-pâtisserie
                                     "33.20C", #Conception d'ensemble et assemblage sur site industriel d'équipements de contrôle des processus industriels
                                     "30.20Z", #Construction de locomotives et d'autre matériel ferroviaire roulant
                                     "25.50B", #Découpage, emboutissage
                                     "29.32Z", #Fabrication d'autres équipements automobiles
                                     "28.29B", #Fabrication d'autres machines d'usage général
                                     "25.73Z", #Fabrication d'autres outillages
                                     "28.23Z", #Fabrication de machines et d'équipements de bureau (à l'exception des ordinateurs et équipements périphériques
                                     "27.12Z", #Fabrication de matériel de distribution et de commande électrique
                                     "32.50A", #Fabrication de matériel médico-chirurgical et dentaire
                                     "28.11Z", #Fabrication de moteurs et turbines, à l'exception des moteurs d'avions et de véhicules
                                     "27.11Z", #Fabrication de moteurs, génératrices et transformateurs électriques
                                     "20.42Z", #Fabrication de parfums et de produits pour la toilette
                                     "21.20Z", #Fabrication de préparations pharmaceutiques
                                     "26.40Z", #Fabrication de produits électroniques grand public
                                     "14.14Z", #Fabrication de vêtements de dessous
                                     "14.13Z", #Fabrication de vêtements de dessus
                                     "26.30Z", #Fabrication d'équipements de communication
                                     "28.29A", #Fabrication d'équipements d'emballage, de conditionnement et de pesage
                                     "29.31Z", #Fabrication d'équipements électriques et électroniques automobiles
                                     "26.51B", #Fabrication d'instrumentation scientifique et technique
                                     "10.71A", #Fabrication industrielle de pain et de pâtisserie fraîche
                                     "18.11Z", #Imprimerie de journaux
                                     "33.20B", #Installation de machines et équipements mécaniques
                                     "25.62B", #Mécanique industrielle
                                     "24.45Z", #Métallurgie des autres métaux non ferreux
                                     "10.71D", #Pâtisserie
                                     "10.13A", #Préparation industrielle de produits à base de viande
                                     "25.61Z", #Traitement et revêtement des métaux
                                     "10.83Z", #Transformation du thé et du café
                                     "10.11Z")) %>% #Transformation et conservation de la viande de boucherie"
  as_arrow_table()

tranches_effectifs <-
  tribble(
    ~trancheEffectifs, ~trancheEffectifsLabel,
    "00", "0 salarié",
    "01", "1 ou 2 salariés",
    "02", "3 ou 5 salariés",
    "03", "6 à 9 salariés",
    "11", "10 à 19 salariés",
    "12", "20 à 49 salariés",
    "21", "50 à 99 salariés",
    "22", "100 à 199 salariés",
    "31", "200 à 249 salariés",
    "32", "250 à 499 salariés",
    "41", "500 à 999 salariés",
    "42", "1 000 à 1 999 salariés",
    "51", "2 000 à 4 999 salariés",
    "52", "5 000 à 9 999 salariés",
    "53", "10 000 salariés et plus"
  )

sirene_ent <-
  here("input", "StockUniteLegale_utf8.csv") %>%
  read_csv_arrow(col_select =  c("siren", "trancheEffectifsUniteLegale", "activitePrincipaleUniteLegale", "etatAdministratifUniteLegale", "denominationUniteLegale"),
                 col_types =  schema(siren = string(),
                                     trancheEffectifsUniteLegale = string(),
                                     activitePrincipaleUniteLegale = string(),
                                     etatAdministratifUniteLegale = string(),
                                     denominationUniteLegale = string()),
                 as_data_frame = FALSE) %>%
  filter((is.na(etatAdministratifUniteLegale) | etatAdministratifUniteLegale == "A") &
           trancheEffectifsUniteLegale %in% c("21", "22", "31", "32")) %>%
  inner_join(tranches_effectifs %>% rename_with(~paste0(.x, "UniteLegale")), by = "trancheEffectifsUniteLegale") %>%
  inner_join(activites_industrielles  %>% rename_with(~paste0(.x, "UniteLegale")), by = "activitePrincipaleUniteLegale") %>%
  select(siren, trancheEffectifsLabelUniteLegale, activitePrincipaleLabelUniteLegale, denominationUniteLegale) %>%
  compute()

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
           ! trancheEffectifsEtablissement %in% c("NN", "00") &
           dep %in% c("75", "77", "78", "91", "92", "93", "94", "95")) %>%
  mutate(across(starts_with("enseigne"), ~if_else(is.na(.x),"",.x)),
         enseigneEtablissement = str_c(enseigne1Etablissement,
                                       enseigne2Etablissement,
                                       enseigne3Etablissement)) %>%
  inner_join(tranches_effectifs %>% rename_with(~paste0(.x, "Etablissement")), by = "trancheEffectifsEtablissement") %>%
  inner_join(naf  %>% rename_with(~paste0(.x, "Etablissement")), by = "activitePrincipaleEtablissement") %>%
  select(c(dep, siret, siren, trancheEffectifsLabelEtablissement, activitePrincipaleLabelEtablissement,
           enseigneEtablissement, denominationUsuelleEtablissement)) %>%
  compute()

sirene <-
  sirene_etab %>%
  inner_join(sirene_ent, by = "siren") %>%
  compute()

production_dep <- function(depcode) {
  dir.create(here("output"), showWarnings = FALSE)
  
  sirene %>%
    filter(dep == depcode) %>%
    select(-dep) %>%
    arrange(siret) %>%
    write_csv_arrow(here("output", paste0("extraction_", depcode, ".csv")))
}

production_dep("75")
production_dep("77")
production_dep("78")
production_dep("91")
production_dep("92")
production_dep("93")
production_dep("94")
production_dep("95")
