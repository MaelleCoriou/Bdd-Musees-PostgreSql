# Projet-chef-d-oeuvre
 
Projet en vue du passage de la Certification Développeur Data.


## Contexte

L’agence de voyage “Go Explore” souhaite développer son offre de séjours touristiques en proposant des circuits sur mesure sur le thème de l’art. 
Pour se faire, le directeur de l’agence souhaite avoir accès aux données des musées de France afin de développer cette nouvelle offre.


## Périmètre du projet

1. Développement d’une base de données relationnelle sous Postgres Sql :
<br/>    → Création d’une base socle, import fichiers CSV Joconde
<br/>    → Update BDD automatisée via requêtes de l’API Joconde

2. Analyse BDD sous requêtes SQL et valorisation du contenu selon les besoins

3. Datavizualisation des résultats sous Dash


## Sources de données

Open Data :

Base Joconde - Inventaire des musées de France
<br/>Upload format Csv, Json ou Api

Collections des musées de France : extrait de la base Joconde
<br/>[Base_Joconde](https://data.culture.gouv.fr/explore/dataset/base-joconde-extrait/information/)

Base Muséofile - Musées de France 
<br/>Upload fichiers formats Csv, Json ou via Api format json.
<br/>Musées de France : base Muséofile — Ministère de la Culture
<br/>[Base_Museo](https://data.culture.gouv.fr/explore/dataset/musees-de-france-base-museofile/information/)

Jeu de données sous licence ouverte : Licence Ouverte Version 2.0 (etalab.gouv.fr)
<br/>[Licence.pdf](https://www.etalab.gouv.fr/wp-content/uploads/2017/04/ETALAB-Licence-Ouverte-v2.0.pdf)


## Arborescence des dossiers du projet

    ├── Documentation    
    │   ├── CORIOU Maelle - Dossier Certification Data Nantes 2021.pdf     ---> Rapport du projet
    │   ├── Cahier_des_charges_Go_Explore.pdf
    │   ├── E-A_SCHEMA.png
    │   ├── ER_SCHEMA_Bdd.png
    │   ├── MLD.png
    │   ├── Planning_Projet_Go_Explore.png
    │   ├── Projet Go Explore _ “Sur la route des Artistes”.pdf            ---> Présentation slides pour l'oral
    │   ├── Schema_ETL_AP.png
    │   └── Schema_Fonctionnel.png
    │                     
    ├── Licences
    │   ├── ETALAB-Licence-Ouverte-v2.0.pdf
    │   └── Open Data-Ministère de la Culture.pdf
    │   
    ├── Scripts
    │   ├── Backup_Bdd
    │   │   ├── Backup_Files
    │   │   │   └── backup-20210518170000-113742.zip
    │   │   │
    │   │   ├── app.py
    │   │   └── backup.py
    │   │   
    │   ├── 01-01-Nettoyage-Base-Museo.py
    │   ├── 01-02-Nettoyage-Base-Joconde.py
    │   ├── 02-01-Crea_Tables_Data_Musee.py
    │   ├── 02-02-Api_Maj_Domaine.py
    │   ├── 02-03-Insertion_Data.py
    │   ├── 03-01-Repertoire-Metadata.py
    │   ├── 04-01-Requetes_SQL_Bdd.ipynb
    │   └── 05-02-Donnees-Geo-Google-API.py
    │   
    ├── Data
    │   ├── Csv_Bdd
    │   │   └── ....
    │   │   │
    │   └── Sources
    │       └── ...
    │   
    ├── Viz
    │   └── Html
    │   │   └── ...
    │   │   
    │   └── Assets
    │   │   └── ...
    │   │
    │   ├── Viz.ipynb
    │   ├── Viz.py                                                         ---> Application Dash  
    │   └── functions_PgSql.py    
    │                                          
    ├── README.md
    │
    └── requirements.txt


### Pré-requis

Pour la réalisation du projet, utilisation des langages et Librairies suivants :

   - Python 3.8.6 (+ requirements.txt)
        
   - Logiciels : 
       - Dbeaver 21.0.0
       - PostGreSQL 13
       - VS Code 1.56
       - MongoDB 4.4.2

   - Librairies :
       - Dash 1.20
       - Flask 1.1.2
       - folium 0.12.1
       - Pandas 1.2.3
       - Psycopg2 2.8.6
       - SQLAlchemy 1.3.20
       - JupyterLab 3.0.12#### Librairies :


### Installation

Exécuter les scripts Pyhton dans l'ordre de la liste des fichiers.


## Versions

**Dernière version :** 3.0


## Auteur
[![forthebadge](http://forthebadge.com/images/badges/built-with-love.svg)](http://forthebadge.com)

* **Maëlle Coriou** _alias_ [@MaelleCoriou](https://github.com/MaelleCoriou)

## License

Jeu de données sous licence ouverte : Licence Ouverte Version 2.0 (etalab.gouv.fr)
<br/>[Licence](https://www.etalab.gouv.fr/wp-content/uploads/2017/04/ETALAB-Licence-Ouverte-v2.0.pdf)

Données base Joconde originales téléchargées sur :
<br/>https://data.culture.gouv.fr/explore/dataset/base-joconde-extrait/export/, 
<br/>mise à jour du 26 février 2021.

Données base Muséofile originales téléchargées sur :
<br/>https://data.culture.gouv.fr/explore/dataset/musees-de-france-base-museofile/export/, 
<br/>mise à jour du 25 juin 2019.

