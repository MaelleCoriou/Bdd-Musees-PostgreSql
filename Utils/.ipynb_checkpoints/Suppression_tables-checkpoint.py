#!/usr/bin/env python

# Librairie Os pour les chemins de fichiers et variables d'environnement
import os
# Librairie csv pour créer et lire le format csv
import csv
# Lien au fichier Python pour exécuter ses fonctions
import functions_PgSql
# import Donnees
import psycopg2
import pandas as pd
import numpy as np

 
# Spécifier le nom de la base de données créée au préalable dans PgAdmin
# Bdd V1 Musees Bdd V2 Musees_Fr
ma_base_donnees = "Musees"
utilisateur = "postgres"

# Méthode os.environ.get pour utiliser le mot de passe 
# enregistré au préalable dans une variable d'environnement
mot_passe = os.environ.get('pg_psw')

# Fonction conn appelle la fonction ouvrir_connexion du fichier annexe Python
conn = functions_PgSql.ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)

def Suppression_Tables_Musees_Fr():
    sql_suppression_table = '''
        DROP TABLE IF EXISTS ARTISTE, CATEGORIE, DEPARTEMENT, INVENTAIRE, MUSEE, 
        MUSEE_INVENTAIRE, OEUVRE, OEUVRE_ARTISTE, OEUVRE_CATEGORIE, OEUVRE_INVENTAIRE, REGION 
        CASCADE;
        '''
    functions_PgSql.supprimer_table(conn, sql_suppression_table)

# Lorsque le fichier est appelé directement on exécute la fonction
if __name__ == "__main__":
   Suppression_Tables_Musees_Fr()
