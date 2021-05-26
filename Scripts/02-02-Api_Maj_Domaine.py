#!/usr/bin/env python

# Librairie Os pour les chemins de fichiers et variables d'environnement
import os
# Librairie csv pour créer et lire le format csv
import csv
# Lien au fichier Python pour exécuter ses fonctions
import functions_PgSql
# Librairie connexion Postgresql
import psycopg2
# Librairies manipulation des données df
import pandas as pd
import numpy as np
# Librairie time temps d'exécution
import time
# Librairie pour requêtes API
import requests
import json


#--------------------- MISE A JOUR BDD VIA API ---------------------#

# Temps d'exécution du programme
start_time = time.time()

# Spécifier le nom de la base de données créée au préalable dans PgAdmin
ma_base_donnees = "Musees_V3"
utilisateur = "postgres"

# Méthode os.environ.get pour utiliser le mot de passe 
# enregistré au préalable dans une variable d'environnement
mot_passe = os.environ.get('pg_psw')

# Fonction conn appelle la fonction ouvrir_connexion du fichier annexe Python
conn = functions_PgSql.ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)

# Connexion à Api Culture gouv base Joconde
response = requests.get('https://data.culture.gouv.fr/api/records/1.0/search/?dataset=base-joconde-extrait&q=&facet=domaine')


# Vérification de la connexion
if response:
    print(response.status_code, 'Response OK')
else:
    print(response.status_code, 'Response Failed')


# Récupération des noms de domaines via Requests 
# Atteindre la clé facets récupérer chaque valeur name --> nom domaine
# Données intégrées sous liste data
data = []
response_1 = json.loads(response.text)

for i in range(len(response_1['facet_groups'][0]['facets'])):
    data.append([response_1['facet_groups'][0]['facets'][i]['name']])


# Liste Data sous df, formater en minuscule au même format que la Bdd
Domaine_Api = pd.DataFrame(data, columns=['domaine'])
Domaine_Api.domaine = Domaine_Api.domaine.str.upper().str.rstrip(' ')

# Remplacer les caractères Iso non traduits en UTF-8
Domaine_Api = Domaine_Api.replace(
            to_replace=("Ã©", "Ã¯", "Ã¨", "Ã", "Ã´", "Ã§", "Ãª", "Ã¹", "Ã¼", "Ã«", "Å", "à§", "ãª", "à‰", "àˆ", "àª"), 
            value=("é", "ï", "è", "à", "ô", "ç", "ê", "ù", "ü", "ë", "œ", "ç", "ê", "É", "ê", "ê"), 
            regex=True
            )
print('Result Api request', Domaine_Api.shape[0])

# Total avant insertion

Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM CATEGORIE;", conn) 

print("Total Table avant insertion : ", Total_Table)

# Insertion des données, ON CONFLICT sur la contrainte unique domaine pass
# Evite les doublons
Insertion_Donnees = ''' 
         INSERT INTO CATEGORIE
         (Domaine) 
         VALUES (%(domaine)s)
         ON CONFLICT (domaine)
         DO NOTHING;
      '''
functions_PgSql.inserer_df(conn, Insertion_Donnees, Domaine_Api)

# Total après insertion
Total_Table_2 = pd.read_sql_query("SELECT COUNT(*) FROM CATEGORIE;", conn)

print ("Total insertion CATEGORIE : ", Total_Table_2 - Total_Table)

print("Insertion Api V2 : \n","--- %s seconds ---" % (time.time() - start_time))

conn.close()