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
ma_base_donnees = "Musees_Fr"
utilisateur = "postgres"

# Méthode os.environ.get pour utiliser le mot de passe 
# enregistré au préalable dans une variable d'environnement
mot_passe = os.environ.get('pg_psw')

# Fonction conn appelle la fonction ouvrir_connexion du fichier annexe Python
conn = functions_PgSql.ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)

# Suppression colonnes Domaine et Auteur
Suppression_Colonnes = '''
ALTER TABLE OEUVRE 
DROP COLUMN Auteur1,
DROP COLUMN Auteur2,
DROP COLUMN Auteur3,
DROP COLUMN Auteur4,
DROP COLUMN Auteur5,
DROP COLUMN Auteur6,
DROP COLUMN Auteur7,
DROP COLUMN Auteur8,
DROP COLUMN Domaine1,
DROP COLUMN Domaine2,
DROP COLUMN Domaine3,
DROP COLUMN Domaine4,
DROP COLUMN Domaine5,
DROP COLUMN Domaine6,
DROP COLUMN Domaine7,
DROP COLUMN Domaine8,
DROP COLUMN Domaine9;
'''
functions_PgSql.modification_table(conn, Suppression_Colonnes)