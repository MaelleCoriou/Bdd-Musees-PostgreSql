# postgreSQL-functions
import os
import psycopg2


def ouvrir_connection(nom_bdd, utilisateur, mot_passe, host='localhost', port=5432):
    try:
        conn = psycopg2.connect(dbname=nom_bdd, user=utilisateur, password=mot_passe, host=host, port=5432)
    except psycopg2.Error as e:
        print("Erreur lors de la connection à la base de données")
        print(e)
        return None
    # On force autocommit (non applicable ds SQLite3)
    conn.set_session(autocommit=True)
    return conn


def supprimer_table(conn, sql_suppression_table):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_suppression_table)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la suppression de la table")
        print(e)
        return
    cursor.close()
    print("La table a été supprimée avec succès")

    
def creer_table(conn, sql_creation_table):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_creation_table)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la création de la table")
        print(e)
        return
    cursor.close()
    print("La table a été créée avec succès")

    
def inserer_donnees(conn, sql_insertion_table, donnees):
    try:
        cursor = conn.cursor()
        for d in donnees:
            cursor.execute(sql_insertion_table, d)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de l'insertion des données")
        print(e)
        return
    cursor.close()
    print("Les données ont été insérées avec succès")

    
def lire_donnees(conn, sql_requete):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_requete)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la lecture des données")
        print(e)
        return None
    
    print("Les données ont été lues avec succès")
    data = []
    for row in cursor:
        data.append(row)

    cursor.close()
    
    return data

def modification_table(conn, modification_table):
    try:
        cursor = conn.cursor()
        cursor.execute(modification_table)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la création de la modification")
        print(e)
        return
    cursor.close()
    print("Modification effectuée avec succès")

def inserer_df(conn, sql_requete, donnees):
    try:
        cursor = conn.cursor()
        for index, row in donnees.iterrows():
            cursor.execute(sql_requete, row)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de l'insertion des données")
        print(e)
        return
    cursor.close()
    print("Les données ont été insérées avec succès")

def insert_csv(conn, sql_insert, filename):
    cur = conn.cursor()

    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        next(reader)
        try:
            
            for row in reader:
                cur.execute(sql_insert, row)
            conn.commit()
        
        except psycopg2.Error as e:
            print("Erreur lors de l'insertion des données")
            print(e)
            return
        cursor.close()
        print("Les données ont été insérées avec succès")


# Fonction permettant d'exécuter un requête SQL sur une BDD définie par sa connexion conn
def executer_requete(requete_sql, conn):
    try:
        cursor = conn.cursor()
        cursor.execute(requete_sql)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de l'execution de la requête")
        print(e)
        return
    cursor.close()