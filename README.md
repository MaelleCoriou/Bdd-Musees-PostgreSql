# Projet-chef-d-oeuvre
 
Projet en vue du passage de la Certification Développeur Data.


## Contexte

L’agence de voyage “Go Explore” souhaite développer son offre de séjours touristiques en proposant des circuits sur mesure sur le thème de l’art. 
Pour se faire, le directeur de l’agence souhaite avoir accès aux données des musées de France afin de développer cette nouvelle offre.


## Périmètre du projet

→ Développement d’une base de données relationnelle sous Postgres Sql :
   - Création d’une base socle, import fichiers CSV Joconde
   - Update BDD automatisée via requêtes de l’API Joconde

→ Analyse BDD sous requêtes SQL et valorisation du contenu selon les besoins

→ Datavizualisation des résultats sous Dash


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

|__ Documentation                           
|__ Licences
|__ Version_4
|   |__ Backup_Bdd
|   |   |__ Backup_Files
|   |__ Data
|   |   |__ Csv_Bdd
|   |   |__ Maj
|   |__ Sources
|   |__ Viz
|   |   |__ Html
|   |__ 01-01-Nettoyage-Base-Museo.py
|   |__ 01-02-Nettoyage-Base-Joconde.py
|   |__ 02-01-Crea_Tables_Data_Musee.py
|   |__ 02-02-Api_Maj_Domaine.py
|   |__ 02-03-Insertion_Data.py
|   |__ 03-01-Repertoire-Metadata.py
|   |__ 04-01-Requetes_SQL_Bdd.ipynb
|__ README.md
|__ requirements.txt


### Pré-requis

#### Langages : 
python==3.8.6
<br/>PostgreSql==4.28

#### Librairies :
<br/>dash==1.20.0
<br/>Flask==1.1.2
<br/>folium==0.12.1
<br/>jupyterlab==2.2.9
<br/>matplotlib==3.3.2
<br/>numpy==1.19.3
<br/>pandas==1.1.3
<br/>plotly==4.14.3
<br/>psycopg2==2.8.6
<br/>pymongo==3.11.2
<br/>requests==2.24.0
<br/>schedule==1.0.0
<br/>SQLAlchemy==1.3.20

#### Outils :
<br/>Dbeaver 21.0.0
<br/>Git Bash 
<br/>Jupyter NoteBook
<br/>MongoDb Compass
<br/>Pg Admin 4.28 pour PostgreSql
<br/>Visual Studio Code


### Installation

Exécuter les scripts Pyhton dans l'ordre de la liste des fichiers.


## Versions

**Dernière version :** 4.0


## Auteurs
[![forthebadge](http://forthebadge.com/images/badges/built-with-love.svg)](http://forthebadge.com)

* **Maëlle Coriou** _alias_ [@MaelleCoriou](https://github.com/MaelleCoriou)

## License

Jeu de données sous licence ouverte : Licence Ouverte Version 2.0 (etalab.gouv.fr)
<br/>[Licence](https://www.etalab.gouv.fr/wp-content/uploads/2017/04/ETALAB-Licence-Ouverte-v2.0.pdf)

Données base Joconde originales téléchargées sur 
<br/>https://data.culture.gouv.fr/explore/dataset/base-joconde-extrait/export/, 
<br/>mise à jour du 26 février 2021.

Données base Muséofile originales téléchargées sur 
<br/>https://data.culture.gouv.fr/explore/dataset/musees-de-france-base-museofile/export/, 
<br/>mise à jour du 25 juin 2019.

