#!/usr/bin/env python
import pymongo
import subprocess
import json
import os
import glob
from pymongo import MongoClient

def Metadata():
    ### transforme liste en dictionnaire
    path = "C:/Users/utilisateur/SIMPLON/DEV-DATA/projet-chef-d-oeuvre/Version_3/Data"

    shpfiles = []
    for dirpath, subdirs, files in os.walk(path):
        subdirs[:] = [d for d in subdirs]
        for x in files:
            shpfiles.append(os.path.join(dirpath, x))
    print(shpfiles)

    for l in shpfiles:
        input_file = l.replace("\\", "/")
        exe = "C:/Program Files (x86)/ExifTool/exiftool.exe"
        process = subprocess.Popen([exe, input_file], stdout=subprocess.PIPE, stderr= subprocess.STDOUT, universal_newlines=True)
        print(input_file) #si ça bloque tu sais quel fichier n'est pas lu correctement
        metadata = {}

        for output in process.stdout:
            line = (output.strip().split(":",1))
            metadata[line[0].strip()] = line[1].strip()

        #permet la création du client qui va se connecter à MongoDB
        client = MongoClient()

        #Préparation à la création de la base de données mabdd.
        db = client.meta_bdd_V3

        collection = db.meta_repertoire_V3

        result = collection.insert_one(metadata)

    #permet de vérifier la liste des collections créées :
    db.list_collection_names()

    
if __name__ == "__main__":
    Metadata()
