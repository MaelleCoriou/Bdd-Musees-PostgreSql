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

# -------------------------------- DATAFRAMES --------------------------------#

Lire = 'sculptures'
Table_Artistes = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Auteurs_'+Lire+'.csv', sep=';', engine='python', encoding='utf-8')
Table_Domaines = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Domaines_'+Lire+'.csv', sep=';', engine='python', encoding='utf-8')
Table_Inventaire = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Inventaire_'+Lire+'.csv', sep=';', engine='python', encoding='utf-8')
Table_Oeuvres = pd.read_csv('C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data/Csv_Bdd/Table_Oeuvres_'+Lire+'.csv', sep=';', engine='python', encoding='utf-8')


# ---------------------- INSERTION DONNEES TABLES PRINCIPALES ----------------#


def Insertion_Data_Artiste():
   # Temps d'exécution du programme
   start_time = time.time()

   print('Total ARTISTE à insérer :', Table_Artistes.shape[0])
   
   # Total avant insertion
   Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM ARTISTE;", conn)
   print("Total Table avant insertion : ", Total_Table)

   Insertion_Donnees = ''' 
         INSERT INTO ARTISTE
         (Nom_Artiste, Precision_Auteur, Ecole_pays) 
         VALUES (%(Auteur)s, %(P1)s, %(Ecole_pays_1)s)
         ON CONFLICT (Nom_Artiste)
         DO NOTHING;
      '''
   functions_PgSql.inserer_df(conn, Insertion_Donnees, Table_Artistes)
   
   # Total après insertion
   Total_Table_2 = pd.read_sql_query("SELECT COUNT(*) FROM ARTISTE;", conn)

   print ("Total : ", Total_Table_2 - Total_Table)
   print("Insertion ARTISTE : ","--- %s seconds ---" % (time.time() - start_time))


def Insertion_Data_Domaine():
   # Temps d'exécution du programme
   start_time = time.time()
   
   # Total à insérer
   print('Total DOMAINE à insérer :', Table_Domaines.shape[0])
      
   # Total avant insertion
   Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM CATEGORIE;", conn)
   print("Total Table avant insertion : ", Total_Table)

   Insertion_Donnees = ''' 
         INSERT INTO CATEGORIE
         (Domaine) 
         VALUES (%(Domaine)s)
         ON CONFLICT (domaine)
         DO NOTHING;
      '''
   functions_PgSql.inserer_df(conn, Insertion_Donnees, Table_Domaines)
   
   # Total après insertion
   Total_Table_2 = pd.read_sql_query("SELECT COUNT(*) FROM CATEGORIE;", conn)

   print ("Total : ", Total_Table_2 - Total_Table)
   print("Insertion CATEGORIE : ","--- %s seconds ---" % (time.time() - start_time))


def Insertion_Data_Inventaire():
   # Temps d'exécution du programme
   start_time = time.time()

   # Total à insérer  
   print('Total INVENTAIRE à insérer :', Table_Inventaire.shape[0])
   
   # Total avant insertion
   Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM INVENTAIRE;", conn)
   print("Total Table avant insertion : ", Total_Table)

   Insertion_Donnees = ''' 
         INSERT INTO INVENTAIRE
         (Crea_Maj, Date_Inv)
         VALUES (%(Crea_Maj)s, %(Date)s)
         ON CONFLICT (Crea_Maj, Date_Inv)
         DO NOTHING;
      '''
   functions_PgSql.inserer_df(conn, Insertion_Donnees, Table_Inventaire)

   # Total après insertion
   Total_Table_2 = pd.read_sql_query("SELECT COUNT(*) FROM INVENTAIRE;", conn)

   print ("Total : ", Total_Table_2 - Total_Table)
   print("Insertion INVENTAIRE : ","--- %s seconds ---" % (time.time() - start_time))


def Insertion_Data_Oeuvre():
   # Temps d'exécution du programme
   start_time = time.time()

   # Total à insérer
   print('Total OEUVRE à insérer :', Table_Oeuvres.shape[0])

   # Total avant insertion
   Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM OEUVRE;", conn)
   print("Total Table avant insertion : ", Total_Table)

   # Connection à la base de données Musées_V3
   cursor = conn.cursor()

   # Recherche de l'Id Max
   query = cursor.execute("""SELECT Max(Id_Oeuvre)
                           FROM Oeuvre;
                        """)
   id_oeuvre = cursor.fetchone()

   # Rajouter 1 au dernier Id de la BDD
   for e in id_oeuvre:
      if e is None :
         e = 0
      else:
         e = e+1
   
   # Indexer l'Id à au df
   Table_Oeuvres.index +=e
   Table_Oeuvres['Id'] = Table_Oeuvres.index

   Insertion_Donnees = ''' 
         INSERT INTO OEUVRE
         (Id_Oeuvre, Reference, Titre_Oeuvre, Sujet_Represente, Denomination, Materiaux_Techniques, Periode_Creation,
         Millesime_de_creation, Epoque, Lieu_Creation_Utilisation, Date_Acquisition, Id_Museo)
         VALUES (%(Id)s, %(reference)s, %(Titre_Oeuvre)s, %(sujet_represente)s, %(denomination)s, %(materiaux_techniques)s,
         %(periode_de_creation)s, %(millesime_de_creation)s, %(epoque)s, %(lieu_de_creation_utilisation)s, 
         %(Date_Acquisition_1)s, %(identifiant_museofile)s);
      '''
   functions_PgSql.inserer_df(conn, Insertion_Donnees, Table_Oeuvres)

   # Total après insertion    
   Total_Table_2 = pd.read_sql_query("SELECT COUNT(*) FROM OEUVRE;", conn)

   print ("Total : ", Total_Table_2 - Total_Table)
   print("Insertion OEUVRE : ","--- %s seconds ---" % (time.time() - start_time))


#------------------------ INSERTION DONNEES TABLES MANY TO MANY -------------------------# 


def Insertion_Data_Oeuvre_Artiste():
   # Temps d'exécution du programme
   start_time = time.time()
   
   # Sélection des colonnes Auteur
   data_oeuvre = Table_Oeuvres.loc[:, Table_Oeuvres.columns.str.startswith(("Auteur-", "Id"))]
   Nb_Colonnes = len(data_oeuvre.columns)

   # Remplir les cellules vides par un _ pour la suppression des Nan
   # Création dictionnaire des auteurs dans la variable result
   clean_dict = filter(lambda k: not isnan(k), data_oeuvre)
   data_oeuvre = data_oeuvre.fillna('_')
   result = data_oeuvre.to_dict(orient="records")

   # Suppression des valeurs _ du dic result
   def delete_nan(result):
      for e in result:
         for i in range(1,Nb_Colonnes):
               if e[f"Auteur-{i}"]=='_':
                  del e[f"Auteur-{i}"]
      return result
   # Nouveau dic enregistré sous result2
   result2 = delete_nan(result)

   # Connection à la base de données Musées_Fr
   cursor = conn.cursor()
   # Recherche dans le dic result2
   for e in result2:
      # get acteur id des Auteur1 à 8
      for i in range(1,Nb_Colonnes):
         # Si le nom de l'auteur est dans la table artiste, renvoyer l'Id_artiste
         if f"Auteur-{i}"  in e:
               query = cursor.execute("""SELECT id_artiste 
                                       FROM artiste 
                                       WHERE nom_artiste = %s;
                                    """,(e[f"Auteur-{i}"],))
               id_auteur = cursor.fetchone()
               try:
                  # insert into table de oeuvre_artiste l'Id_artiste de la table artiste 
                  # et insérer l'Id oeuvre du dictionnaire result2
                  cursor.execute("""INSERT INTO oeuvre_artiste 
                                    VALUES (%s,%s)
                                 """,(e['Id'],id_auteur))
               except:
                  # Si double valeur existe déja dans la table, passer à la suivante
                  pass
                  
               conn.commit()
   
   # Total après insertion    
   Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM oeuvre_artiste;", conn)

   print ("Total_ARTISTE : ", Total_Table)
   print("Insertion OEUVRE ARTISTE : ","--- %s seconds ---" % (time.time() - start_time))


def Insertion_Data_Oeuvre_Domaine():
   # Temps d'exécution du programme
   start_time = time.time()

   data_domaine = Table_Oeuvres.loc[:, Table_Oeuvres.columns.str.startswith(("Domaine-", "Id"))]
   Nb_Colonnes = len(data_domaine.columns)

   # Remplir les cellules vides par un _ pour la suppression des Nan
   # création dictionnaire des domaines dans la variable result
   clean_dict = filter(lambda k: not isnan(k), data_domaine)
   data_domaine = data_domaine.fillna('_')
   result = data_domaine.to_dict(orient="records")

   # Suppression des valeurs _ du dic result
   def delete_nan(result):
      for e in result:
         for i in range(1,Nb_Colonnes):
               if e[f"Domaine-{i}"]=='_':
                  del e[f"Domaine-{i}"]
      return result
   # Nouveau dic enregistré sous result2
   result2 = delete_nan(result)

   # Connection à la base de données Musées_Fr
   cursor = conn.cursor()
   # Recherche dans le dic result2
   for e in result2:      
      # get domaine id des Domaine1 à 9
      for i in range(1,Nb_Colonnes):
         # Si le nom de du domaine est dans la table domaine, renvoyer l'Id_domaine
         if f"Domaine-{i}"  in e:
               query = cursor.execute("""SELECT id_categorie 
                                       FROM categorie 
                                       WHERE domaine = %s;
                                    """,(e[f"Domaine-{i}"],))
               id_domaine = cursor.fetchone()
               try:
                  # insert into table de oeuvre_categorie l'Id_categorie récupérée et insérer l'Id oeuvre du dictionnaire result2
                  cursor.execute("""INSERT INTO oeuvre_categorie 
                                 VALUES (%s,%s);""",(e['Id'],id_domaine))               
               except:
                  # Si double valeur existe déja dans la table, passer à la suivante
                  pass
               
               conn.commit()
   
   # Total après insertion    
   Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM OEUVRE_CATEGORIE;", conn)

   print ("Total : ", Total_Table)
   print("Insertion OEUVRE DOMAINE : ","--- %s seconds ---" % (time.time() - start_time))


def Insertion_Data_Crea():
   # Temps d'exécution du programme
   start_time = time.time()

   data_date = Table_Oeuvres[['Id', 'date_de_creation']]

   # Remplir les cellules vides par un _ pour la suppression des Nan
   # création dictionnaire des domaines dans la variable result
   clean_dict = filter(lambda k: not isnan(k), data_date)
   data_date = data_date.fillna('_')
   result = data_date.to_dict(orient="records")

   # Suppression des valeurs _ du dic result
   def delete_nan(result):
      for e in result:
         if e["date_de_creation"]=='_':
               del e["date_de_creation"]
      return result
   result2 = delete_nan(result)

   # Connection à la base de données Musées_Fr
   cursor = conn.cursor()
   # Recherche dans le dic result2
   for e in result2:
      if f"date_de_creation" in e:         
      # Si la date est dans la table date, renvoyer l'Id_inventaire
         query = cursor.execute("""SELECT id_inventaire 
                                    FROM inventaire 
                                    WHERE date_inv = %s AND crea_maj = 1;
                                 """,(e["date_de_creation"],))
         id_date = cursor.fetchone()         
         try:
               # insert into table de oeuvre_inventaire l'Id_inv récupérée et insérer l'Id oeuvre du
               # dictionnaire result3
               cursor.execute("""INSERT INTO oeuvre_inventaire 
                                       VALUES (%s,%s);
                              """,(e['Id'],id_date))
         except:
               # Si double valeur existe déja dans la table, passer à la suivante
               pass

         conn.commit()
   
   # Total après insertion    
   Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM OEUVRE_INVENTAIRE;", conn)

   print ("Total : ", Total_Table)
   print("Insertion DATE CREA : ","--- %s seconds ---" % (time.time() - start_time))


def Insertion_Data_Maj():
   # Temps d'exécution du programme
   start_time = time.time()

   data_maj = Table_Oeuvres[['Id', 'date_de_mise_a_jour']]

   # Remplir les cellules vides par un _ pour la suppression des Nan
   # création dictionnaire des domaines dans la variable result
   clean_dict = filter(lambda k: not isnan(k), data_maj)
   data_maj = data_maj.fillna('_')
   result = data_maj.to_dict(orient="records")
   
   # Suppression des valeurs _ du dic result
   def delete_nan(result):
         for e in result:
            if e["date_de_mise_a_jour"]=='_':
               del e["date_de_mise_a_jour"]
         return result
   result2 = delete_nan(result)

   # Connection à la base de données Musées_Fr
   cursor = conn.cursor()
   # Recherche dans le dic result3
   for e in result2:
      if f"date_de_mise_a_jour" in e:
      # Si la date est dans la table date, renvoyer l'Id_inventaire
         query = cursor.execute("""SELECT id_inventaire 
                                       FROM inventaire 
                                       WHERE date_inv = %s 
                                       AND crea_maj = 2;
                              """,(e["date_de_mise_a_jour"],))
         id_date = cursor.fetchone()         
         try:
               # insert into table de oeuvre_inventaire l'Id_inv récupérée et insérer l'Id oeuvre du
               # dictionnaire result4
               cursor.execute("""INSERT INTO oeuvre_inventaire 
                                    VALUES (%s,%s);
                              """,(e['Id'],id_date))
         except:
               # Si double valeur existe déja dans la table, passer à la suivante
               pass         
         
         conn.commit()
   
   # Total après insertion    
   Total_Table = pd.read_sql_query("SELECT COUNT(*) FROM OEUVRE_INVENTAIRE;", conn)

   print ("Total : ", Total_Table)
   print("Insertion DATE MAJ : ","--- %s seconds ---" % (time.time() - start_time))


# ------------------------------- END ------------------------------#


# Lorsque le fichier est appelé directement on exécute la fonction
if __name__ == "__main__":
   # Insertion des données, ordre d'insertion à respecter selon Fk
   Insertion_Data_Artiste()
   Insertion_Data_Domaine()
   Insertion_Data_Inventaire()
   Insertion_Data_Oeuvre()
   Insertion_Data_Oeuvre_Artiste()
   Insertion_Data_Oeuvre_Domaine()
   Insertion_Data_Crea()
   Insertion_Data_Maj()

