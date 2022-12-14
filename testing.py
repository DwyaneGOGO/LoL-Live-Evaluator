#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics
"""
This module is the spark streaming analysis process.


Usage:
    If used with dataproc:
        gcloud dataproc jobs submit pyspark --cluster <Cluster Name> twitterHTTPClient.py

    Create a dataset in BigQurey first using
        bq mk bigdata_sparkStreaming

    Remeber to replace the bucket with your own bucket name


Todo:
    1. hashtagCount: calculate accumulated hashtags count
    2. wordCount: calculate word count every 60 seconds
        the word you should track is listed below.
    3. save the result to google BigQuery

"""

from google.cloud import storage
from io import StringIO
import pandas as pd
import json
from flask import Flask, render_template, request
import requests
import pandas as pd
import numpy as np
from riotwatcher._apis.league_of_legends import MatchApiV5
from riotwatcher import LolWatcher, ApiError
import numpy as np
import csv

def subtract_list(list1,list2):
    array1 = np.array(list1)
    array2 = np.array(list2)
    subtracted_array = np.subtract(array1, array2)
    subtracted = list(subtracted_array)
    return subtracted

def get_match_json(matchid):
    api_key='RGAPI-bc2f7582-a02b-44cf-b4ff-5bffbb9fdc92'
    region='AMERICAS'
    
    url_pull_match = "https://{}.api.riotgames.com/lol/match/v5/matches/{}/timeline?api_key={}".format(region, matchid, api_key)
    match_data_all = requests.get(url_pull_match).json()
    #try:
     #   length_match = match_data_all['info']['frames'][15]
    return match_data_all
   # except IndexError:
    #    return ['Match is too short. Skipping.']

def get_player_stats(match_data, player,time): #  player has to be an int (1-10)
    # Get player information 

    player_query = match_data['info']['frames'][time]['participantFrames'][str(player)]
    #player_team = player_query['teamId'] - It is not possibly with the endpoint to get the teamId
    player_total_gold = player_query['totalGold']
    return player_total_gold 

def cal_gold_dif(golddiff,data,time):
    team1gold=0
    team2gold=0
    for i in [1,2,3,4,5]:
        team1gold+=get_player_stats(data, i,time)
    for j in [6,7,8,9,10]:
        team2gold+=get_player_stats(data, i,time)
    golddiff.append(team1gold-team2gold)
    return golddiff

def new_pad_list(mylist,time):
    #pad un-updated list with the previous value
    if len(mylist) <(time+1):
        mylist.append(mylist[-1])
    return mylist

def append_1min_stat(team1drag,team2drag,team1baron,team2baron,team1herald,team2herald,team1turrent,team2turrent,team1inhib,team2inhib,team1kill,team2kill,df_1min,time):
   #count kc for kill count, tc for turrent count , ic for inhibitor count
    team1kc=0
    team2kc=0
    team1tc=0
    team2tc=0
    team1ic=0
    team2ic=0
    winner=0
    for i in range(df_1min.shape[0]):
        #champion kill count
        if df_1min['type'].iloc[i]=='CHAMPION_KILL':
            if df_1min['killerId'].iloc[i] in [1,2,3,4,5]:
                team1kc+=1
            else:
                team2kc+=1
        #dragon kill,baron kill, herald kill
        if df_1min['type'].iloc[i]=='ELITE_MONSTER_KILL':
            if df_1min['monsterType'].iloc[i]=='DRAGON':
                if df_1min['killerTeamId'].iloc[i]== 100.0:
                    team1drag.append(team1drag[-1]+1)  
                
                if df_1min['killerTeamId'].iloc[i]==200.0:
                    team2drag.append(team2drag[-1]+1)
            if df_1min['monsterType'].iloc[i]=='BARON_NASHOR':
                if df_1min['killerTeamId'].iloc[i]== 100.0:
                    team1baron.append(team1baron[-1]+1)  
                
                if df_1min['killerTeamId'].iloc[i]==200.0:
                    team2baron.append(team2baron[-1]+1)
            if df_1min['monsterType'].iloc[i]=='RIFTHERALD':
                if df_1min['killerTeamId'].iloc[i]== 100.0:
                    team1herald.append(team1herald[-1]+1)  
                
                if df_1min['killerTeamId'].iloc[i]==200.0:
                    team2herald.append(team2herald[-1]+1) 
        #building kill:turrent and inhibitors
        if df_1min['type'].iloc[i]=='BUILDING_KILL':
            if df_1min['buildingType'].iloc[i]=='TOWER_BUILDING':
                if df_1min['killerId'].iloc[i] in [1,2,3,4,5]:
                    team1tc+=1
                else:
                    team2tc+=1
            if df_1min['buildingType'].iloc[i]=='INHIBITOR_BUILDING':
                if df_1min['killerId'].iloc[i] in [1,2,3,4,5]:
                    team1ic+=1
                else:
                    team2ic+=1
        if df_1min['type'].iloc[i]=='GAME_END':
            if df_1min['winningTeam'].iloc[i]==100.0:
                winner=0
            else:
                winner=1
    #append new champion kill, turrent kill, inhibitor kill 
    team1kill.append(team1kill[-1]+team1kc)
    team2kill.append(team2kill[-1]+team2kc)
    team1turrent.append(team1turrent[-1]+team1tc)
    team2turrent.append(team2turrent[-1]+team2tc)
    team1inhib.append(team1inhib[-1]+team1ic)
    team2inhib.append(team2inhib[-1]+team2ic)
    team1drag=new_pad_list(team1drag,time)
    team2drag=new_pad_list(team2drag,time)
    team1baron=new_pad_list(team1baron,time)
    team2baron=new_pad_list(team2baron,time)
    team1herald=new_pad_list(team1herald,time)
    team2herald=new_pad_list(team2herald,time)
    
    return winner,team1drag,team2drag,team1baron,team2baron,team1herald,team2herald,team1turrent,team2turrent,team1inhib,team2inhib,team1kill,team2kill

def get_1matchid(matchid):
    outputdf=pd.DataFrame(columns=['golddiff','dragondiff','barondiff','heralddiff','towerdiff','inhibitordiff','killdiff'])
    count=0
    
     
    json1=get_match_json("NA1_"+str(matchid))
    
    
   
        
    json2=json1['info']['frames']
       
    gamelength=len(json2)
    team1drag=[0]
    team2drag=[0]
    team1baron=[0]
    team2baron=[0]
    team1herald=[0]
    team2herald=[0]
    team1turrent=[0]
    team2turrent=[0]
    team1inhib=[0]
    team2inhib=[0]
    team1kill=[0]
    team2kill=[0]
    golddiff=[0]
       
    for i in range(gamelength):
        event_query = json1['info']['frames'][i]['events']
    
        df=pd.DataFrame.from_dict(event_query)
        winner,team1drag,team2drag,team1baron,team2baron,team1herald,team2herald,team1turrent,team2turrent,team1inhib,team2inhib,team1kill,team2kill=append_1min_stat(
        team1drag,team2drag,team1baron,team2baron,team1herald,team2herald,team1turrent,team2turrent,team1inhib,team2inhib,team1kill,team2kill,df,(i+1))
        golddiff=cal_gold_dif(golddiff,json1,i)
    df2=pd.DataFrame([[golddiff,subtract_list(team1drag,team2drag),subtract_list(team1baron,team2baron),subtract_list(team1herald,team2herald),
                         subtract_list(team1turrent,team2turrent),subtract_list(team1inhib,team2inhib),subtract_list(team1kill,team2kill),winner ]],columns=['golddiff','dragondiff','barondiff','heralddiff','towerdiff','inhibitordiff','killdiff','winner'])
    outputdf=pd.concat([outputdf,df2],ignore_index=True)
    count+=1
    print("finish:",count)
        
    return outputdf

def reformat(data_list):
    red = []
    blue = []
    for i in range(len(data_list[0][0])):
        red.append(50)
        blue.append(50)
    rows = [red, blue]
    head = []
    for i in range(len(red)):
      head.append(i+1)
    
    with open('sample-output.csv', 'w') as f:
        # using csv.writer method from CSV package
        write = csv.writer(f)
        write.writerow(head)
        write.writerows(rows)
        write.writerow(data_list[0][1])
        write.writerow(data_list[0][0])
        write.writerow(data_list[0][2])
        write.writerow(data_list[0][3])
        write.writerow(data_list[0][4])
        write.writerow(data_list[0][5])
        write.writerow(data_list[0][6])
    new = pd.read_csv('sample-output.csv')
    new = new.iloc[: , 1:]
    return new

# global variables
bucket = "dataproc-staging-us-central1-419343931639-hthrtj25"

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gs://{}/GFG.csv".format(bucket)

client = storage.Client(project="chrome-insight-363115")

bucket = client.get_bucket(bucket)

blob = bucket.get_blob("sample-output.csv")

bt = blob.download_as_string()

s = str(bt, "utf-8")
s = StringIO(s)

df = pd.read_csv(s)
data = df.values.tolist() #list of outputs
#print(data)

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index-.html')
#with open('graph.js', 'w') as out_file:
#  out_file.write('var data = %s;' % df)
#print(df)

@app.route('/index_matchid/')
def matchid():
    name = data
    return render_template('index_matchid.html',name=name)

@app.route('/postmethod', methods = ['POST'])
def get_post_javascript_data():
    match_id = request.form['javascript_data']
    print(match_id)
    wholedf=get_1matchid(match_id)
    print(wholedf.values.tolist())
    wholedf = reformat(wholedf.values.tolist())
    print(wholedf)
    bucket.blob('sample-output.csv').upload_from_string(wholedf.to_csv(), 'text/csv')
    return match_id

app.run(debug=True)