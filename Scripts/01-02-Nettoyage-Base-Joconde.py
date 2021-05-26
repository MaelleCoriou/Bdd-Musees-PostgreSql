#!/usr/bin/env python

# Librairie Os pour les chemins de fichiers et variables d'environnement
import os
# Librairie csv pour créer et lire le format csv
import csv
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import re
import unicodedata
import string

# Update des noms de fichiers pour chaque lecture ou sauvegarde
Lire = 'sculptures'

# Lecture fichiers Csv to dataframe Pandas, extraction inventaire Joconde
Df_Peintures = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Sources/base-joconde-'+Lire+'.csv', 
                            sep=';', engine='python', encoding='utf-8')

# Tri des données
sorted_df = Df_Peintures.sort_values(by=["Référence", "Auteur", "Date acquisition", "Dénomination", "Domaine", "Epoque", "Identifiant Museofile", "Mesures", "Titre", "Millésime de création", "Date de mise à jour", "Date de création"], na_position='last')

# Suppression de colonnes
sorted_df = sorted_df.drop(columns=["Ancien dépôt","Appellation", "Date de dépôt","Découverte / collecte", "Lieu de dépôt", "Mesures", 
                            "Date sujet représenté", "Lieu historique", "N°Inventaire", "Appellation « musée de France »", 
                            "Localisation", "Millésime d’utilisation", "Nom officiel du musée", "Onomastique", 
                            "Période de l’original copié", "Période d’utilisation", "Région", "Source représentation", 
                            "Statut juridique", "Utilisation / Destination", "Ville", "Lien site associé", "geolocalisation_ville"])

# Suppression des musées erronés :
mask = sorted_df["Identifiant Museofile"].isin(['M5202', 'M05019','M0122','B9060', 'M1073', 'M0173', 'M1144', 'M0921', 'M5036', 'M9036', '5027', 'M0716', '0', 0])
sorted_df = sorted_df[~mask]

# Correction Id Museo erroné
sorted_df["Identifiant Museofile"] = sorted_df["Identifiant Museofile"].replace(to_replace=("M05019", "M5202"), value=("M5019", "M5052"), regex=True)


# Remplacer les caractères Iso non traduits en UTF-8
sorted_df = sorted_df.replace(to_replace=("Ã©", "Ã¯", "Ã¨", "Ã", "Ã´", "Ã§", "Ãª", "Ã¹", "Ã¼", "Ã«", "Å", "à§", "ãª", "à‰", "àˆ", "àª"), 
                                value=("é", "ï", "è", "à", "ô", "ç", "ê", "ù", "ü", "ë", "œ", "ç", "ê", "É", "ê", "ê"), regex=True)


# Remplacer les ; par ,
sorted_df = sorted_df.stack().str.replace(";",",").unstack()


# Remplacer les / par -
sorted_df = sorted_df.stack().str.replace("/","-").unstack()


# Transformer format des chaînes de caractères capitalize
sorted_df["Précision auteur"] = sorted_df["Précision auteur"].str.capitalize()
sorted_df.Dénomination = sorted_df.Dénomination.str.capitalize()
# Transformer chaînes de caractères en majuscules
sorted_df.Epoque = sorted_df.Epoque.str.upper()
sorted_df["Millésime de création"] = sorted_df["Millésime de création"].str.upper()
sorted_df["Identifiant Museofile"] = sorted_df["Identifiant Museofile"].str.upper()


# Création d'une nouvelle colonne pour nettoyage date d'acquisition
# Remplacer tous les caractères sauf chiffres de 0 à 9, les  \ et - par un -, 
# supprimer les -* par un seul -
sorted_df[["Date_Acquisition_1"]] = sorted_df["Date acquisition"].replace(regex=True, to_replace=(r'[^0-9.\-]', r'[^0-9.](-)*'), value=(r'-', r'-')).str.strip('-')


# Nettoyage données Epoque
# Création d'une nouvelle colonne Epoque_1 pour appliquer les modifications
sorted_df[["Epoque_1"]] = sorted_df.Epoque.replace(to_replace=(" - ", "ÉPOQUE ", "STYLE ", "\(\?\)", "\?"), value=("-", "", "", "", ""), regex=True)


# Nettoyage données Dénomination
# Création d'une nouvelle colonne Dénomination_1 pour appliquer les modifications
sorted_df[["Dénomination_1"]] = sorted_df.Dénomination.replace(to_replace=("\(\?\)", "\(", "\)", "\?"), value=("", "", "", ""), regex=True)

# Nettoyage données Ecole-pays
# Création de la colonne Ecole_Pays_1 où les modifications seront appliquées
# Suppression des informations entre parenthèses
sorted_df[["Ecole_pays_1"]] = sorted_df["Ecole-pays"].replace(regex=True, to_replace=r"\(.*?\)", value=r'').str.upper()
# Retirer les accents car les accents ne sont pas mis à chaque enregistrement
# Format capitales, suppression des espaces en fin de lignes
sorted_df.Ecole_pays_1 = sorted_df.Ecole_pays_1.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')


# Précision Auteur : retirer les nom d'auteurs en début de ligne, nouvelle liste dans colonne P1
sorted_df[["P1"]] = sorted_df["Précision auteur"].str.split(":", n=1).str[-1]


# Précision Auteur, copiés dans nouvelle colonne P_Dates pour extraire uniquement les dates
# Supprimer le texte de Précision Auteur
sorted_df[["P_Dates"]] = sorted_df.P1.replace(regex=True, to_replace=r'[^0-9\-]', value=r'-')


# Supprimer les -* par un seul -
sorted_df.P_Dates = sorted_df.P_Dates.replace(regex=True, to_replace=r'[^0-9.](-)*', value=r'-').str.strip('-')


# Retirer les accents, ne sont pas mis à chaque enregistrement
# Transformer chaînes de caractères en majuscule
# Données modifiées dans nouvelle colonne Titre_1
sorted_df[["Titre_Oeuvre"]] = sorted_df.Titre.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()


# Nettoyage données Auteur
# Création de la colonne Auteur_1 où les modifications seront appliquées
# Données modifiées dans nouvelle colonne Auteur_1
# Suppression des informations entre parenthèses
sorted_df[["Auteur_1"]] = sorted_df.Auteur.replace(regex=True, to_replace=(r"\(.*?\)", r"\(*", r"\)*"), value=(r'', r'', r''))

# Transformer chaînes de caractères en majuscule
# Retirer les accents, ne sont pas mis à chaque enregistrement
sorted_df.Auteur_1 = sorted_df.Auteur_1.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.upper()
# Nettoyage données Auteur
sorted_df.Auteur_1 = sorted_df.Auteur_1.replace(to_replace=("BENOAT","FRANAOIS", "FRANCOIS","-", "\."), 
                                                value=("BENOIT","FRANÇOIS", "FRANÇOIS", " ", ""), regex=True)
# Remplacer les valeurs nulles de la colonne Auteur_1 par Inconnu
sorted_df.Auteur_1.fillna("INCONNU", inplace=True)


# Split nom auteur à partir de la première ,
df_split = sorted_df.Auteur_1.str.split(",", expand=True)
# Nommer les colonnes splittées Auteur1, Auteur2 ...
df_split.columns = ['Auteur-{}'.format(x+1) for x in df_split.columns]
# Join des nouvelles colonnes dans df initial
df_join = sorted_df.join(df_split)


# Création de la colonne Domaine_1 avec les données de Domaine
# Modification affectuée aux données copiées dans nouvelle colonne
df_join[["Domaine_1"]] = sorted_df.Domaine.replace(to_replace=("QING"), value=("ÉPOQUE QING"), regex=True).str.upper()
# Split nom Domaine à partir de la première ,
df_split_domaine = df_join.Domaine_1.str.split(",", expand=True)
# Nommer les colonnes splittées Domaine1, Domaine2 ...
df_split_domaine.columns = ['Domaine-{}'.format(x+1) for x in df_split_domaine.columns]
# Join des nouvelles colonnes dans df initial
df_join_domaine = df_join.join(df_split_domaine)


# Nettoyage des virgules avec espaces mutiples et doubles virgules
df_join_domaine = df_join_domaine.replace(regex=True, to_replace=(r" *?, +", r" *?, *, +"), value=(r", ", r", "))


# Supprimer les espaces en début et fin de cellules
df_join_domaine = df_join_domaine.applymap(lambda x: x.strip() if type(x)==str else x)


df_join_domaine.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Update_check_'+Lire+'.csv', 
                        index = False, sep = ';', encoding='utf-8')


df_doublons = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Update_check_'+Lire+'.csv', 
                        sep=';', engine='python', encoding='utf-8')


# Vérification des doublons
df_doublons.duplicated(["Référence", "Identifiant Museofile", "Auteur_1", "Titre_Oeuvre"]).sum()


# Suppression des doublons, conserver la première occurence
df_doublons = df_doublons.drop_duplicates(subset=["Référence", "Identifiant Museofile", "Auteur_1", "Titre_Oeuvre"], keep="first")

# Suppression des oeuvres sans id_Museo
df_doublons = df_doublons.dropna(subset=["Identifiant Museofile"])

# Export Dataframe sous fichier Csv
df_doublons.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Inventaire_'+Lire+'.csv', 
                    sep = ';', encoding='utf-8', index=True, index_label = 'Id')


#---------------------------------------------------------------------------------------------------------------------------#

## ---------------- NETTOYAGE DONNEES POUR TABLE AUTEURS / DOMAINES / DATES CREATION + MAJ --------------------------------##

#---------------------------------------------------------------------------------------------------------------------------#


###       AUTEURS       ###

# Lecture fichier Csv to dataframe Pandas, extraction inventaire Joconde : Peintures
Df_AD = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Inventaire_'+Lire+'.csv', sep=';', engine='python', encoding='utf-8')


# Filtre sur les auteurs avec infos dates école pays
# Ces informations correspondent à l'auteur 1
auteur1 = Df_AD[["Auteur-1", "P1", "Ecole_pays_1", "P_Dates", "Id"]]


# Tri des données pour effectuer la suppression des doublons selon les arguments de ce tri
auteur1 = auteur1.sort_values(by=["Auteur-1", "P_Dates", "Ecole_pays_1"], na_position='last')


# Suppression des doublons, conserver la première occurence
doublons_auteur1 = auteur1.drop_duplicates(subset=["Auteur-1"], keep="first")


# Sélection des colonnes pour la table Auteurs
# Auteur 1 sera à joindre à la liste finale
auteurs2 = Df_AD.loc[:, Df_AD.columns.str.startswith(("Auteur-", "Id"))]
auteurs2 = auteurs2.drop(columns=['Auteur-1'])

# Création df pivot, lister tous les auteurs avec Id
auteurs2 = pd.wide_to_long(auteurs2, stubnames='Auteur-', i="Id", j='N_Auteur')


# Export Dataframe sous fichier Csv
auteurs2.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Pivot_Auteurs_'+Lire+'.csv', 
                sep = ';', encoding='utf-8', index=True)


# Lecture fichier Csv to dataframe Pandas
# Permet d'avoir un df avec noms de colonnes au même niveau
auteurs2 = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Pivot_Auteurs_'+Lire+'.csv', 
                        sep=';', engine='python', encoding='utf-8')


# Tri des données pour effectuer la suppression des doublons selon les arguments de ce tri
auteurs2 = auteurs2.sort_values(by=["Auteur-"], na_position='last')


# Suppression des doublons, conserver la première occurence
doublons_auteurs = auteurs2.drop_duplicates(subset=["Auteur-"], keep="first")

# Ajout des champs pour concatener Auteur1
auteurs = doublons_auteurs.reindex(columns = doublons_auteurs.columns.tolist() + ["P1", "Ecole_pays_1", "P_Dates"])

# Sélection des 5 champs
auteurs = auteurs[["Auteur-", "P1", "Ecole_pays_1", "P_Dates", "Id"]]

# Renommage colonne Auteur1
doublons_auteur1 = doublons_auteur1.rename(columns={'Auteur-1':'Auteur'})
auteurs = auteurs.rename(columns={'Auteur-':'Auteur'})

# Concatenation des 2 df auteurs
auteurs = pd.concat([auteurs, doublons_auteur1])

# Tri des données pour effectuer la suppression des doublons selon les arguments de ce tri
auteurs = auteurs.sort_values(by=['Auteur', 'P1', 'Ecole_pays_1', 'P_Dates', 'Id'], na_position='last')

# Suppression des doublons, conserver la première occurence
auteurs = auteurs.drop_duplicates(subset=["Auteur"], keep="first")
# Suppression des lignes dont la valeur est nulle pour Auteur
auteurs = auteurs.dropna(subset=['Auteur'])

# Export Dataframe sous fichier Csv
auteurs.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Auteurs_'+Lire+'.csv', 
            index = False, sep = ';', encoding='utf-8')



###       DOMAINES       ###


# Sélection des colonnes pour la table Domaine
domaine = Df_AD.loc[:, Df_AD.columns.str.startswith(("Domaine-", "Id"))]

# Création df pivot, lister tous les Domaines avec Id
domaine = pd.wide_to_long(domaine, stubnames='Domaine-', i="Id", j='N_Domaine')

# Export Dataframe sous fichier Csv
domaine.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Pivot_Domaines_'+Lire+'.csv', 
                sep = ';', encoding='utf-8', index=True)

# Lecture fichier Csv to dataframe Pandas
domaine = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Pivot_Domaines_'+Lire+'.csv', 
                sep=';', engine='python', encoding='utf-8')


domaine = domaine.rename(columns={'Domaine-':'Domaine'})

# Tri des données pour effectuer la suppression des doublons selon les arguments de ce tri
domaine = domaine.sort_values(by=["Domaine"], na_position='last')

domaine = domaine[["Domaine"]]

# Suppression des doublons, conserver la première occurence
domaine = domaine.drop_duplicates(subset=["Domaine"], keep="first")

# Export Dataframe sous fichier Csv
domaine.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Domaines_'+Lire+'.csv', 
                index = False, sep = ';', encoding='utf-8')



###       DATES CREA / MAJ       ###

# Sélection des colonnes pour la table
crea_maj = Df_AD[['Id', 'Date de création', 'Date de mise à jour']]

crea_maj = crea_maj.rename(columns = {'Date de création': 'Date1', 'Date de mise à jour': 'Date2'})

# Création df pivot, lister tous les Domaines avec Id
crea_maj = pd.wide_to_long(crea_maj, stubnames='Date', i="Id", j='Crea_Maj')


# Export Dataframe sous fichier Csv
crea_maj.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Pivot_DateCrea_'+Lire+'.csv', 
                sep = ';', encoding='utf-8', index=True)


# Lecture fichier Csv to dataframe Pandas
crea_maj = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Maj/Pivot_DateCrea_'+Lire+'.csv', 
                        sep=';', engine='python', encoding='utf-8')


# Suppression des doublons, conserver la première occurence
crea_maj = crea_maj.drop_duplicates(subset=['Date', 'Crea_Maj'], keep="first")

crea_maj = crea_maj.dropna()

# Export Dataframe sous fichier Csv
crea_maj.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Inventaire_'+Lire+'.csv', index = False, sep = ';', encoding='utf-8')


# Renommage des colonnes pour faciliter l'insertion des données
Noms_Col = df_doublons.rename(columns = {'Référence' : 'reference', 'Auteur' : 'auteur', 'Dénomination' : 'denomination', 
                            'Date de mise à jour' : 'date_de_mise_a_jour', 'Date de création' : 'date_de_creation', 
                            'Domaine' : 'domaine', 'Ecole-pays' : 'ecole_pays', 'Epoque' : 'epoque', 'Lieu de création/utilisation' : 'lieu_de_creation_utilisation', 
                            'Millésime de création' : 'millesime_de_creation', 'Identifiant Museofile' : 'identifiant_museofile', 
                            'Précision auteur' : 'precision_auteur', 'Période de création' : 'periode_de_creation', 
                            'Sujet représenté' : 'sujet_represente', 'Matériaux – techniques' : 'materiaux_techniques', 
                            'Titre' : 'titre'})

# Export Dataframe sous fichier Csv
Noms_Col.to_csv(r'C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Oeuvres_'+Lire+'.csv', 
                sep = ';', encoding='utf-8', index=True)


