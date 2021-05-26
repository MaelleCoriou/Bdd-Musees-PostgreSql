#!/usr/bin/env python

# Librairie Os pour les chemins de fichiers et variables d'environnement
import os
# Librairie csv pour créer et lire le format csv
import csv
import pandas as pd
import numpy as np

# Lecture fichier Csv to dataframe Pandas, extraction inventaire Joconde : Peintures
Df_Museo = pd.read_csv('C:/Users/utilisateur/SIMPLON\DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Sources/base_museofile.csv', sep=';', engine='python', encoding='utf-8', dtype='object')

## NETTOYAGE CODES POSTAUX

# Compléter la valeur Nulle
Df_Museo.loc[Df_Museo.ref == 'M0592', 'cp_m'] = '81000'

# Extraire codes départements
Df_Museo['code_dpt'] = Df_Museo.cp_m
Df_Museo.code_dpt = Df_Museo.code_dpt.str[:2]

# Modifier les codes postaux Corse 201 / 202 et Dom-Tom
Df_Museo.loc[Df_Museo.dpt == 'Corse-du-Sud', 'code_dpt'] = '201'
Df_Museo.loc[Df_Museo.dpt == 'Haute-Corse', 'code_dpt'] = '202'
Df_Museo.loc[Df_Museo.dpt == 'Guadeloupe', 'code_dpt'] = '971'
Df_Museo.loc[Df_Museo.dpt == 'Guyane', 'code_dpt'] = '973'
Df_Museo.loc[Df_Museo.dpt == 'Martinique', 'code_dpt'] = '972'
Df_Museo.loc[Df_Museo.dpt == 'Mayotte', 'code_dpt'] = '976'
Df_Museo.loc[Df_Museo.dpt == 'Réunion', 'code_dpt'] = '974'
Df_Museo.loc[Df_Museo.dpt == 'Saint-Pierre-et-Miquelon', 'code_dpt'] = '975'
Df_Museo.loc[Df_Museo.dpt == 'Ain', 'code_dpt'] = '1'
Df_Museo.loc[Df_Museo.dpt == 'Aisne', 'code_dpt'] = '2'
Df_Museo.loc[Df_Museo.dpt == 'Allier', 'code_dpt'] = '3'
Df_Museo.loc[Df_Museo.dpt == 'Alpes-de-Haute-Provence', 'code_dpt'] = '4'
Df_Museo.loc[Df_Museo.dpt == 'Hautes-Alpes', 'code_dpt'] = '5'
Df_Museo.loc[Df_Museo.dpt == 'Alpes-Maritimes', 'code_dpt'] = '6'
Df_Museo.loc[Df_Museo.dpt == 'Ardèche', 'code_dpt'] = '7'
Df_Museo.loc[Df_Museo.dpt == 'Ardennes', 'code_dpt'] = '8'
Df_Museo.loc[Df_Museo.dpt == 'Ariège', 'code_dpt'] = '9'

# Remplacer les ; par ,
Museo = Df_Museo.stack().str.replace(';',', ').unstack()

Museo = Museo.replace(to_replace=(" ,  "), value=', ', regex=True)

# Nettoyage données Url sans www.
Museo["url_m"] = Museo["url_m"].replace(to_replace=("https://"), value='', regex=True)
Museo["url_m"] = Museo["url_m"].replace(to_replace=("www."), value='', regex=True)
Museo["url_m"] = Museo["url_m"].apply(lambda x: "{}{}".format('www.', x))

# Remplacer les valeurs nulles de la colonne "url_m" par Non Renseignée
Museo["url_m"] = Museo["url_m"].replace(to_replace=("www.nan"), value='Non renséignée', regex=True)

# Créer colonne x et y pour permettre la géolocalisation
Museo[["Geo_x", "Geo_y"]] = Museo.geolocalisation.str.split(",", expand=True, n=1)

Museo.to_csv(r'C:/Users/utilisateur/SIMPLON\DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Musees.csv', index = False, sep = ';', encoding='utf-8')

## CREATION TABLE DEPARTEMENTS

Liste_Dpt = Museo[["dpt", "code_dpt"]]

# Tri des données pour effectuer la suppression des doublons selon les arguments de ce tri
sorted_doublon = Liste_Dpt.sort_values(by=["code_dpt"], ascending=True, na_position='last')

# Suppression des doublons
sorted_doublon = sorted_doublon.drop_duplicates(subset=["dpt"], keep="first")

# Export Dataframe sous fichier Csv
sorted_doublon.to_csv(r'C:/Users/utilisateur/SIMPLON\DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Departements.csv', index = True, sep = ';', encoding='utf-8')

## CREATION TABLE REGION

Liste = Museo[["code_dpt", "dpt", "region"]]
Liste_sorted = Liste.sort_values(by=["code_dpt", "dpt", "region"], ascending=True, na_position='last')

Doublons = Liste_sorted.drop_duplicates(subset=["code_dpt","dpt","region"], keep="first")

# Suppression de la valeur nulle cde_dpt
Doublons = Liste_sorted.dropna(subset=["code_dpt"])

Doublons.to_csv(r'C:/Users/utilisateur/SIMPLON\DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Check_Dpt_Region.csv', index = False, sep = ';', encoding='utf-8')

# Fichier Dpt-région renseigné avec les codes région manuellement
Dpt_Region = pd.read_csv('C:/Users/utilisateur/SIMPLON\DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Dpt_Region_Code_Region.csv', sep=';', engine='python', encoding='utf-8')

Dpt_Region = Dpt_Region[['code_region', 'region']]

Doublons_region = Dpt_Region.drop_duplicates(subset=["code_region","region"], keep="first")
Doublons_region.to_csv(r'C:/Users/utilisateur/SIMPLON\DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Table_Region.csv', index = False, sep = ';', encoding='utf-8')

