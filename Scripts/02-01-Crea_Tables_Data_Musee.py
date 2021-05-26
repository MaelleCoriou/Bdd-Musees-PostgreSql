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

# Spécifier le nom de la base de données créée au préalable dans PgAdmin
ma_base_donnees = "Musees_V3"
utilisateur = "postgres"

# Méthode os.environ.get pour utiliser le mot de passe 
# enregistré au préalable dans une variable d'environnement
mot_passe = os.environ.get('pg_psw')

# Fonction conn appelle la fonction ouvrir_connexion du fichier annexe Python
conn = functions_PgSql.ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)


## -------------------- CREATION TABLES ET INSERTION DONNEES MUSEES ----------------------##


#------------------------------- CREATION TABLES Musees_V3 --------------------------------#

def creation_tables_Musees_Fr():
   # Fonction SQL paramétres de la table Localisation
   sql_creation_table = """
         CREATE TABLE IF NOT EXISTS CATEGORIE(
            Id_Categorie SERIAL,
            Domaine TEXT UNIQUE,
            PRIMARY KEY(Id_Categorie)
         );

         CREATE TABLE IF NOT EXISTS ARTISTE(
            Id_Artiste SERIAL,
            Nom_Artiste TEXT UNIQUE,
            Precision_Auteur TEXT,
            Ecole_pays TEXT,
            PRIMARY KEY(Id_Artiste)
         );

         CREATE TABLE IF NOT EXISTS REGION(
            Id_Region TEXT NOT NULL,
            Nom_Region TEXT UNIQUE,
            PRIMARY KEY(Id_Region)
         );

         CREATE TABLE IF NOT EXISTS DEPARTEMENT(
            Id_Dpt INT NOT NULL,
            Nom_Departement TEXT UNIQUE,
            Id_Region TEXT NOT NULL,
            PRIMARY KEY(Id_Dpt),
            FOREIGN KEY(Id_Region) REFERENCES REGION(Id_Region)
         );

         CREATE TABLE IF NOT EXISTS INVENTAIRE(
            Id_Inventaire SERIAL,
            Crea_Maj INT,
            Date_Inv DATE,
            PRIMARY KEY(Id_Inventaire),
            UNIQUE (Crea_Maj, Date_Inv)
         );

         CREATE TABLE IF NOT EXISTS MUSEE(
            Id_Museo TEXT,
            Nom_Officiel TEXT,
            Nom_d_Usage TEXT,
            Adresse TEXT,
            Code_Postal TEXT,
            Ville TEXT,
            Lieu TEXT,
            Telephone TEXT,
            Themes TEXT,
            Personnage_Phare TEXT,
            Artistes TEXT,
            Atout TEXT,
            Histoire_Musee TEXT,
            Protection_Espace TEXT,
            Categorie TEXT,
            Domaine_Thematique TEXT,
            Url_Site TEXT,
            Geo_x DOUBLE PRECISION,
            Geo_y DOUBLE PRECISION,
            Code_Dpt INT NOT NULL,
            PRIMARY KEY(Id_Museo),
            FOREIGN KEY(Code_Dpt) REFERENCES DEPARTEMENT(Id_Dpt)
         );

         CREATE TABLE IF NOT EXISTS OEUVRE(
            Id_Oeuvre INT NOT NULL,
            Reference TEXT,
            Titre_Oeuvre TEXT,
            Sujet_Represente TEXT,
            Denomination TEXT,
            Materiaux_Techniques TEXT,
            Periode_Creation TEXT,
            Millesime_de_creation TEXT,
            Epoque TEXT,
            Lieu_Creation_Utilisation TEXT,
            Date_Acquisition TEXT,
            Id_Museo TEXT,
            PRIMARY KEY(Id_Oeuvre),
            FOREIGN KEY(Id_Museo) REFERENCES MUSEE(Id_Museo)
         );

         CREATE TABLE IF NOT EXISTS OEUVRE_ARTISTE(
            Id_Oeuvre INT,
            Id_Artiste INT,
            PRIMARY KEY(Id_Oeuvre, Id_Artiste),
            FOREIGN KEY(Id_Oeuvre) REFERENCES OEUVRE(Id_Oeuvre),
            FOREIGN KEY(Id_Artiste) REFERENCES ARTISTE(Id_Artiste)
         );

         CREATE TABLE IF NOT EXISTS OEUVRE_CATEGORIE(
            Id_Oeuvre INT,
            Id_Categorie INT,
            PRIMARY KEY(Id_Oeuvre, Id_Categorie),
            FOREIGN KEY(Id_Oeuvre) REFERENCES OEUVRE(Id_Oeuvre),
            FOREIGN KEY(Id_Categorie) REFERENCES CATEGORIE(Id_Categorie)
         );

         CREATE TABLE IF NOT EXISTS OEUVRE_INVENTAIRE(
            Id_Oeuvre INT,
            Id_Inventaire INT,
            PRIMARY KEY(Id_Oeuvre, Id_Inventaire),
            FOREIGN KEY(Id_Oeuvre) REFERENCES OEUVRE(Id_Oeuvre),
            FOREIGN KEY(Id_Inventaire) REFERENCES INVENTAIRE(Id_Inventaire)
         );
      """
   functions_PgSql.creer_table(conn, sql_creation_table)


# ---------------------------- INSERTION DONNEES TABLES PRINCIPALES -------------------------#


def Insertion_Data_Region():
   # Temps d'exécution du programme
   start_time = time.time()

   data_region = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Regions.csv', sep=';', engine='python', encoding='utf-8')
   print('Total Region à insérer :', data_region.shape[0])

   Insertion_Donnees = ''' 
         INSERT INTO REGION
         (Id_Region, Nom_Region) 
         VALUES (%(code_region)s, %(region)s);
      '''
   functions_PgSql.inserer_df(conn, Insertion_Donnees, data_region)
   print ("Total Table REGION : ", pd.read_sql_query("SELECT COUNT(*) FROM REGION;", conn))
   print("Insertion REGION : \n","--- %s seconds ---" % (time.time() - start_time))
   

def Insertion_Data_Departement():
   # Temps d'exécution du programme
   start_time = time.time()
   
   data_departement = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Departements.csv', sep=';', engine='python', encoding='utf-8')
   print('Total dépatement à insérer :', data_departement.shape[0])

   Insertion_Donnees = ''' 
         INSERT INTO DEPARTEMENT
         (Id_Dpt, Nom_Departement, Id_Region) 
         VALUES (%(code_dpt)s, %(dpt)s, %(code_region)s);
      '''
   functions_PgSql.inserer_df(conn, Insertion_Donnees, data_departement)

   print("Total Table DEPARTEMENT : ", pd.read_sql_query("SELECT COUNT(*) FROM DEPARTEMENT;", conn))
   print("Insertion DEPARTEMENT : \n","--- %s seconds ---" % (time.time() - start_time))



def Insertion_Data_Musee():
   # Temps d'exécution du programme
   start_time = time.time()

   data_musees = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Musees.csv', sep=';', engine='python', encoding='utf-8')
   print('Total Musee à insérer :', data_musees.shape[0])
   
   Insertion_Donnees = ''' 
         INSERT INTO MUSEE
         (Id_Museo, Nom_Officiel, Nom_d_Usage, Adresse, Code_Postal, Ville, Lieu, 
         Telephone, Themes, Personnage_Phare, Artistes, Atout, Histoire_Musee, 
         Protection_Espace, Categorie, Domaine_Thematique, Url_Site, Geo_x, Geo_y, Code_Dpt)
         VALUES (%(ref)s, %(nomoff)s, %(nomusage)s, %(adrl1_m)s, %(cp_m)s,
         %(ville_m)s, %(lieu_m)s, %(tel_m)s, %(themes)s, %(phare)s, %(artiste)s, %(atout)s,
         %(hist)s, %(prot_esp)s, %(categ)s, %(dompal)s, %(url_m)s, %(Geo_x)s, %(Geo_y)s, %(code_dpt)s);
      '''
   functions_PgSql.inserer_df(conn, Insertion_Donnees, data_musees)

   print ("Total Table MUSEE : ", pd.read_sql_query("SELECT COUNT(*) FROM MUSEE;", conn))  
   print("Insertion MUSEE : \n","--- %s seconds ---" % (time.time() - start_time))


#------------------------------------------ END ----------------------------------------- #


# Lorsque le fichier est appelé directement on exécute la fonction
if __name__ == "__main__":
   # Création Tables
   creation_tables_Musees_Fr()
   # Insertion des données, ordre d'insertion à respecter selon Fk
   Insertion_Data_Region()
   Insertion_Data_Departement()
   Insertion_Data_Musee()