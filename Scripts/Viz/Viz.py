#!/usr/bin/env python

import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from dash.dependencies import Input, Output
import functions_PgSql
import psycopg2
import datetime
import folium
import os


# Spécifier le nom de la base de données créée au préalable dans PgAdmin
ma_base_donnees = "Musees_V3"
utilisateur = "postgres"

# Méthode os.environ.get pour utiliser le mot de passe 
# enregistré au préalable dans une variable d'environnement
mot_passe = os.environ.get('pg_psw')

# Fonction conn appelle la fonction ouvrir_connexion du fichier annexe Python
conn = functions_PgSql.ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)

cursor = conn.cursor()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


# Graphique 1 : 
# Nombre d'oeuvres et musées par villes, filtre sur >2000 oeuvres exposées
oeuvre_villes_musees = pd.read_sql_query('''SELECT m.ville, COUNT(o.id_oeuvre) AS Total_Oeuvres,
                        ROUND(ROUND(COUNT(o.id_oeuvre),2)*100/(SELECT ROUND(COUNT(o.id_oeuvre),2) 
                                                               FROM oeuvre o),2) AS Part_Oeuvres,
                        COUNT(DISTINCT o.Id_Museo) AS Nb_Musees
                        FROM oeuvre o,
                        musee m
                        WHERE o.Id_Museo = m.Id_Museo
                        GROUP BY ville
                        HAVING COUNT(o.id_oeuvre) > 2000
                        ORDER BY ville;''',conn)

trace1 = go.Bar(x=oeuvre_villes_musees.ville, y=oeuvre_villes_musees.nb_musees, name='Nombre_musées')
trace2 = go.Bar(x=oeuvre_villes_musees.ville, y=oeuvre_villes_musees.total_oeuvres, name='Nombre_oeuvres')


# Graphique 1 : 
# Nombre d'oeuvres et musées par villes, filtre sur <= 1000 oeuvres exposées
oeuvre_villes_musees2 = pd.read_sql_query('''SELECT m.ville, COUNT(o.id_oeuvre) AS Total_Oeuvres,
                        ROUND(ROUND(COUNT(o.id_oeuvre),2)*100/(SELECT ROUND(COUNT(o.id_oeuvre),2) 
                                                               FROM oeuvre o),2) AS Part_Oeuvres,
                        COUNT(DISTINCT o.Id_Museo) AS Nb_Musees
                        FROM oeuvre o,
                        musee m
                        WHERE o.Id_Museo = m.Id_Museo
                        GROUP BY ville
                        HAVING COUNT(o.id_oeuvre) < 2001
                        ORDER BY ville;''',conn)

trace1b = go.Bar(x=oeuvre_villes_musees2.ville, y=oeuvre_villes_musees2.nb_musees, name='Nombre_musées')
trace2b = go.Bar(x=oeuvre_villes_musees2.ville, y=oeuvre_villes_musees2.total_oeuvres, name='Nombre_oeuvres')

# Graphique 2 : 
# Nombre d'oeuvres et musées par villes
Top_15_villes = pd.read_sql_query('''
                        SELECT m.ville, 
                               COUNT(o.id_oeuvre) AS Total_Oeuvres,
                               COUNT(DISTINCT o.Id_Museo) AS Nb_Musees
                        FROM oeuvre o, musee m
                        WHERE o.Id_Museo = m.Id_Museo
                        GROUP BY ville
                        ORDER BY Total_Oeuvres DESC
                        LIMIT 15;''',conn)
fig = px.bar(Top_15_villes, x='ville', y='total_oeuvres',
             hover_data=['nb_musees', 'total_oeuvres'], color='nb_musees',
             labels={'Nb':'Nb Oeuvres'}, height=400, width=1000)

# Graphique 3 :
# Nombre d'oeuvres et musées par régions
oeuvre_region = pd.read_sql_query('''SELECT r.nom_region,
                                COUNT(DISTINCT o.id_museo) AS Nb_Musees,
                                COUNT( DISTINCT o.id_oeuvre) AS Total_Oeuvres, 
                                ROUND(ROUND(COUNT(o.id_oeuvre),2)*100/(SELECT ROUND(COUNT(o.id_oeuvre),2) FROM oeuvre o),2) AS Part_Oeuvres\
                                FROM oeuvre o, musee m, departement d, region r 
                                WHERE o.Id_Museo = m.Id_Museo 
                                AND m.code_dpt = d.id_dpt AND d.id_region = r.id_region 
                                GROUP BY nom_region ORDER BY total_oeuvres DESC;''',conn)

trace3 = go.Bar(x=oeuvre_region.nom_region, y=oeuvre_region.total_oeuvres, name='Nombre_oeuvres')
trace4 = go.Bar(x=oeuvre_region.nom_region, y=oeuvre_region.nb_musees, name='Nombre_Musees')

# Graphique 3 :
# Top 15 artistes les plus expoxés, exclus anonymes et inconnus
df3 = pd.read_sql_query('''SELECT DISTINCT nom_artiste,
                        COUNT(DISTINCT o.id_oeuvre) AS Nb_Oeuvre,
                        COUNT(DISTINCT o.id_museo) AS Nb_Musees
                        FROM oeuvre o
                        JOIN oeuvre_artiste oa
                            ON o.id_oeuvre = oa.id_oeuvre
                        JOIN artiste a
                            ON oa.id_artiste = a.id_artiste
                        WHERE nom_artiste <> 'ANONYME'
                        AND nom_artiste <> 'INCONNU'

                        GROUP BY nom_artiste
                        ORDER BY nb_oeuvre DESC
                        LIMIT 15;''',conn)

trace5 = go.Bar(x=df3.nom_artiste, y=df3.nb_musees, name='Nombre_musées')
trace6 = go.Bar(x=df3.nom_artiste, y=df3.nb_oeuvre, name='Nombre_oeuvres')

# Graphique 4 :
# Nombre d'acquisitions par années
date_acquisition = pd.read_sql_query('''SELECT o.date_acquisition,
                        COUNT(o.id_oeuvre) AS Total_Oeuvres,
                        ROUND(ROUND(COUNT(o.id_oeuvre),2)*100/(SELECT ROUND(COUNT(o.id_oeuvre),2) FROM oeuvre o),2) || ' %' AS Part_Oeuvres,
                        COUNT(DISTINCT o.Id_Museo) AS Nb_Musees                        
                        FROM oeuvre o,
                            musee m
                        WHERE o.Id_Museo = m.Id_Museo
                        AND date_acquisition != 'NaN'
 
                        GROUP BY date_acquisition
                        HAVING COUNT(o.id_oeuvre) > 10
                        ORDER BY date_acquisition DESC;''',conn)


date_acquisition.date_acquisition = date_acquisition.date_acquisition.replace(regex=True, to_replace= (r'[^0-9/-]', r'[^0-9.](-)*', r'[0-9]{2}-', r'[0-9]{1}-', r'-[0-9]{2}-', r'-[0-9]{1}-', r'[0-9]{1}-'), value=(r'-', r'-', r'-', r'-', r'-', r'-', r'-')).str.strip('-')
date_acquisition = date_acquisition.sort_values(by='date_acquisition')
mask = date_acquisition.date_acquisition.isin(['29', '30', '31', '22', '16', '19', '26', '27', '28', '15', '14', '12', '11', '10', '9', '17', '18', '23', '25', '09', '07', '06', '05', '04', '03', '02', '01', '21', '08', '1', '20'])
date_acquisition = date_acquisition[~mask]


trace7 = px.area(x=date_acquisition.date_acquisition, y=date_acquisition.total_oeuvres)
trace7.update_layout(
     title_font_size = 40, 
     width = 1200, height = 600)
trace7.update_xaxes(
     title_text = 'Années',
     title_font=dict(size=15, family='Verdana', color='black'), 
     tickfont=dict(family='Calibri', color='darkred', size=15))
trace7.update_yaxes(
     title_text = "Nombre Oeuvres", 
     title_font=dict(size=15,family='Verdana',color='black'), 
     tickfont=dict(family='Calibri', color='darkred', size=15))


## ------------------------------- APP DASH --------------------------------##


app.layout = html.Div(children=[
    html.H1('Oeuvres et musées par villes', 
            style={"fontSize": "44px", "text-align":"center"}),
    html.Div('Villes exposant plus de 2000 oeuvres',
             style={"fontSize": "22px"}),
    dcc.Graph(id='Villes',
            figure={'data' : [trace1, trace2],
                    'layout': {"title":'Villes Oeuvres et Musees', 
                    "barmode":'stack',
                    'height': 600,
                    }}
            ),
    html.Br(),
    html.Br(),

    html.H1('Oeuvres et musées par villes', 
                style={"fontSize": "44px", "text-align":"center"}),
    html.Div('Villes exposant moins de 2000 oeuvres',
                style={"fontSize": "22px"}),
    dcc.Graph(id='Villes 2',
            figure={'data' : [trace1b, trace2b],
                    'layout': {"title":'Villes Oeuvres et Musees', 
                    "barmode":'stack',
                    'height': 650,
                    }}
            ),
    html.Br(),
    html.Br(),

    html.H2('Oeuvres, musées par villes', 
            style={"fontSize": "44px", "text-align":"center"}),
    html.Div('Top 15 Villes', 
                style={"fontSize": "22px"}),
    dcc.Graph(id='Top 15 Villes',
            figure=fig
            ),
    html.Br(),
    html.Br(),

    html.H2('Nombre oeuvres, nombre musées par régions', style={"fontSize": "44px","text-align":"center"}),
    html.Div(''),
    dcc.Graph(id='Régions',
            figure={'data': [trace3, trace4],
                    'layout':
                    go.Layout(title='Régions', 
                    barmode='stack',
                    height= 450)
        }),
    html.Br(),
    html.Br(),

    html.H2('Top 15 Peintres', style={"fontSize": "44px", "text-align":"center"}),
    html.Div(''),
    dcc.Graph(id='Artistes oeuvres',
            figure={'data': [trace5, trace6],
                    'layout':
                    go.Layout(title='Oeuvres', 
                    barmode='stack',
                    height=450)
        }),
    html.Br(),
    html.Br(),

    html.H2('Nombre d\'oeuvres acquises par an', style={"text-align":"center"}),
    html.Div(''),
    dcc.Graph(id='Nb acquisition',
            figure=trace7
        ),
    html.Br(),
    html.Br(),

    html.H2('Scope sur les acquisitions de 2011', style={"fontSize": "30px", "text-align":"center"}),
    html.Div(html.Img(src=app.get_asset_url('acquisition.png')),
    
               ),       
    html.Br(),
    html.Br(),

    html.H2('Où peut-on admirer les oeuvres de Calder ?', 
            style={"fontSize": "44px", "text-align":"center"}),
    html.Div(''),   
    html.Iframe(id='map',
                srcDoc=open('C:/Users/maell/DEV_DATA/Musees_App/Scripts/Viz/Html/Calder.html','r').read(),
                            width='800', height='600', style={'textAlign': 'center'}),
    html.Br(),
    html.Br(),   
    
    html.H2('Sur la route du 21 ème siècle', 
           style={"fontSize": "44px", "text-align":"center"}),
    html.Div(''),   
    html.Iframe(id='map2',
                srcDoc=open('C:/Users/maell/DEV_DATA/Musees_App/Scripts/Viz/Html/21eme.html','r').read(),
                            width='800',height='600', style={'align':'center'}),
    html.Br(),
    html.Br(),


    html.H2('Passionné de l\'art Romain...', 
           style={"fontSize": "44px", "text-align":"center"}),
    html.Div('Voici les lieux :', style={"fontSize": "22px"}),   
    html.Iframe(id='map3',
                srcDoc=open('C:/Users/maell/DEV_DATA/Musees_App/Scripts/Viz/Html/ROMAIN.html','r').read(),
                            width='800',height='600', style={'align':'center'}),
    html.Br(),
    html.Br(),
])

# App Dash : Dash is running on http://127.0.0.1:8050/

if __name__ == '__main__':
    app.run_server(debug=True)
