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
    1. Need to get the deep learning model to 
       give outputs for predictions for the live_api

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
from sklearn.preprocessing import StandardScaler
import math
import torch
from torch import nn
from torch.utils.data import Dataset, DataLoader, random_split
from torch.autograd import Variable
from tqdm import tqdm
import warnings
from ast import literal_eval
import time
warnings.filterwarnings('ignore')
from flask import jsonify

def subtract_list(list1,list2):
    array1 = np.array(list1)
    array2 = np.array(list2)
    subtracted_array = np.subtract(array1, array2)
    subtracted = list(subtracted_array)
    return subtracted

def get_match_json(matchid):
    api_key='RGAPI-bef6259f-dd47-41ae-8929-87ec6b4c36ad'
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

def get_predictions(data_list, model):
    MAX_TIME_STEP = len(data_list[0][0])
    print("MAX_TIME_STEP",MAX_TIME_STEP)
    labels = []
    red = []
    blue = []
    scalers = {}
    for i in range(len(data_list[0])-1):
        scalers[i] = StandardScaler()
        for row in data_list[0][i]:
            scalers[i].partial_fit(np.asanyarray(row).reshape(-1, 1))
    
    for i in range(len(data_list[0])-1):
        data_list[0][i] = scalers[i].transform(np.asanyarray(data_list[0][i]).reshape(-1, 1)).reshape(-1)
    
    for i in range(MAX_TIME_STEP):
      max = i+1
      x = np.asarray([[ [data_list[0][i][timestep] for i in range(len(data_list[0])-1)] for timestep in range(max) ]], dtype=np.float32)
    
      model.eval()
      with torch.no_grad():
          x = torch.from_numpy(x)
          predict = model(x)
          winner = ['red', 'blue'][predict.argmax(1)]
          prob_red = math.exp(predict[0][0].item()) / (math.exp(predict[0][0].item()) + math.exp(predict[0][1].item()))
          prob_blue = math.exp(predict[0][1].item()) / (math.exp(predict[0][0].item()) + math.exp(predict[0][1].item()))
          labels.append(max)
          red.append(prob_red*100)
          blue.append(prob_blue*100)
          #print(f"model predicted winner: { winner }")
          #print(f"red wins: {prob_red * 100 :.1f}% | blue wins: {prob_blue * 100:.1f}%")
    print('red:',red)
    print('blue:',blue)
    return red, blue

def reformat(data_list):
    red, blue = get_predictions(data_list, model)
    #for i in range(len(data_list[0][0])):
    #    red.append(50)
    #    blue.append(50)
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

def reformat_bubble(data_list):
    head = ['val', 'id' ,'groupid' ,'size']
    #We might need to concatenate this for multiple values/time stamps into one csv.
    with open('sample-output-modded.csv', 'w') as f:
      # using csv.writer method from CSV package
        write = csv.writer(f)
        write.writerow(head)
        print('GUYS:',len(data_list[0][0]))
        for val in range(len(data_list[0][1])):
            print('HERE: ',data_list[0][1])
            if data_list[0][1][val]!=0:
              if data_list[0][1][val] < 0:
                dragon_num = - data_list[0][1][val]
                dragon_group_id = 1
              if data_list[0][1][val] > 0:
                dragon_num = data_list[0][1][val]
                dragon_group_id = 2
              id = 1
              size = 2000
              for i in range(dragon_num):
                print('Entered here')
                write.writerow([val, id, dragon_group_id, size])
            
            if data_list[0][2][val]!=0:
              if data_list[0][2][val] < 0:
                baron_num = - data_list[0][2][val]
                baron_group_id = 1
              if data_list[0][2][val] > 0:
                baron_num = data_list[0][2][val]
                baron_group_id = 2
              id = 2
              size = 2000
              for i in range(baron_num):
                print('Entered here')
                write.writerow([val, id, baron_group_id, size])
            
            if data_list[0][3][val]!=0:
              if data_list[0][3][val] < 0:
                herald_num = - data_list[0][3][val]
                herald_group_id = 1
              if data_list[0][3][val] > 0:
                herald_num = data_list[0][3][val]
                herald_group_id = 2
              id = 3
              size = 2000
              for i in range(herald_num):
                print('Entered here')
                write.writerow([val, id, herald_group_id, size])
          
            if data_list[0][4][val]!=0:
              if data_list[0][4][val] < 0:
                tower_num = - data_list[0][4][val]
                tower_group_id = 1
              if data_list[0][4][val] > 0:
                tower_num = data_list[0][4][val]
                tower_group_id = 2
              id = 4
              size = 2000
              for i in range(tower_num):
                print('Entered here')
                write.writerow([val, id, tower_group_id, size])
            
            if data_list[0][5][val]!=0:
              if data_list[0][5][val] < 0:
                inhibitor_num = - data_list[0][5][val]
                inhibitor_group_id = 1
              if data_list[0][5][val] > 0:
                inhibitor_num = data_list[0][5][val]
                inhibitor_group_id = 2
              id = 5
              size = 2000
              for i in range(inhibitor_num):
                print('Entered here')
                write.writerow([val, id, inhibitor_group_id, size])
    new = pd.read_csv('sample-output-modded.csv')
    #new = new.iloc[: , 1:]
    return new

class RNN(nn.Module):
    def __init__(self):
        super(RNN,self).__init__()
        self.hidden_size = 256
        
        self.rnn= nn.RNN(
            nonlinearity = 'relu',
            input_size = 7,
            hidden_size = self.hidden_size,
            num_layers = 1,
            batch_first = True
        )

        self.out = nn.Linear(self.hidden_size, 2)
    
    def forward(self,x):
        r_out, hn = self.rnn(x, torch.zeros(1, len(x), self.hidden_size))
        out = self.out(r_out[:, -1, :])
        return out

class RNN_ng(nn.Module):
    def __init__(self):
        super(RNN_ng,self).__init__()
        self.hidden_size = 256
        
        self.rnn= nn.RNN(
            nonlinearity = 'relu',
            input_size = 6,
            hidden_size = self.hidden_size,
            num_layers = 1,
            batch_first = True
        )

        self.out = nn.Linear(self.hidden_size, 2)
    
    def forward(self,x):
        r_out, hn = self.rnn(x, torch.zeros(1, len(x), self.hidden_size))
        out = self.out(r_out[:, -1, :])
        return out

model = RNN()
model.load_state_dict(torch.load(r"C:\Users\ayman\Dropbox\My PC (LAPTOP-19GOKHVG)\Downloads\model_all_feat.pt"))
# global variables
bucket = "dataproc-staging-us-central1-419343931639-hthrtj25"

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gs://{}/GFG.csv".format(bucket)

client = storage.Client(project="chrome-insight-363115")

bucket = client.get_bucket(bucket)

blob1 = bucket.get_blob("sample-output.csv")
blob2 = bucket.get_blob("sample-output-modded.csv")

bt1 = blob1.download_as_string()
bt2 = blob2.download_as_string()

s1 = str(bt1, "utf-8")
s1 = StringIO(s1)

s2 = str(bt2, "utf-8")
s2 = StringIO(s2)

df = pd.read_csv(s1)
data = df.values.tolist() #list of outputs

df2 = pd.read_csv(s2)
data2 = df2.values.tolist() #list of outputs
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
    blob1 = bucket.get_blob("sample-output.csv")
    blob2 = bucket.get_blob("sample-output-modded.csv")

    bt1 = blob1.download_as_string()
    bt2 = blob2.download_as_string()

    s1 = str(bt1, "utf-8")
    s1 = StringIO(s1)

    s2 = str(bt2, "utf-8")
    s2 = StringIO(s2)

    df = pd.read_csv(s1)
    data = df.values.tolist() #list of outputs

    df2 = pd.read_csv(s2).iloc[: , 1:]
    data2 = df2.values.tolist() #list of outputs
    
    name = data
    bubble = data2
    
    lst = [name, bubble]
    return render_template('index_matchid.html',lst=lst)

@app.route('/index_live/')
def matchlive():
    blob1 = bucket.get_blob("leaguedata/text.csv")

    bt1 = blob1.download_as_string()

    s1 = str(bt1, "utf-8")
    s1 = StringIO(s1)

    df = pd.read_csv(s1)
    data = df.values.tolist() #list of outputs
    data_ref = data
    
    print(data)
    mod_data = []
    for x in data[0]:
        if len(mod_data)==len(data[0])-1:
            continue
        print('X:',x)
        x = literal_eval(x)
        mod_data.append(x)
    data = []
    data.append(mod_data.copy())
    print("mod_data: ",mod_data)
    bubble = reformat_bubble(data)
    print('bubs:',bubble)
    if bubble.empty:
        head = ['val', 'id' ,'groupid' ,'size']
        with open('sample-output-modded.csv', 'w') as f:
            write = csv.writer(f)
            write.writerow(head)
            write.writerow([30, 1, 2, 2000])
        bubble = pd.read_csv('sample-output-modded.csv')
    bubble = bubble.values.tolist()
    print('bubby:',bubble)
    
    model = RNN_ng()
    model.load_state_dict(torch.load(r"C:\Users\ayman\Dropbox\My PC (LAPTOP-19GOKHVG)\Downloads\model_no_gold.pt"))
    print('DATA: ',data)
    red, blue = get_predictions(data,model)
    print('Preds:')
    print(red)
    print(blue)
    pred = []
    pred.append(red)
    pred.append(blue)
    
    name = mod_data
    lst = [name, bubble, pred]
    return render_template('index_live.html',lst=lst)

@app.route('/test', methods=['GET'])
def testfn():
    blob1 = bucket.get_blob("leaguedata/text.csv")

    bt1 = blob1.download_as_string()

    s1 = str(bt1, "utf-8")
    s1 = StringIO(s1)

    df = pd.read_csv(s1)
    data = df.values.tolist() #list of outputs
    data_ref = data
    
    print(data)
    mod_data = []
    for x in data[0]:
        if len(mod_data)==len(data[0])-1:
            continue
        print('X:',x)
        x = literal_eval(x)
        mod_data.append(x)
    data = []
    data.append(mod_data.copy())
    print("mod_data: ",mod_data)
    bubble = reformat_bubble(data)
    print('bubs:',bubble)
    if bubble.empty:
        head = ['val', 'id' ,'groupid' ,'size']
        with open('sample-output-modded.csv', 'w') as f:
            write = csv.writer(f)
            write.writerow(head)
            write.writerow([30, 1, 2, 2000])
        bubble = pd.read_csv('sample-output-modded.csv')
    bubble = bubble.values.tolist()
    print('bubby:',bubble)
    
    model = RNN_ng()
    model.load_state_dict(torch.load(r"C:\Users\ayman\Dropbox\My PC (LAPTOP-19GOKHVG)\Downloads\model_no_gold.pt"))
    print('DATA: ',data)
    red, blue = get_predictions(data,model)
    print('Preds:')
    print(red)
    print(blue)
    pred = []
    pred.append(red)
    pred.append(blue)
    
    name = mod_data
    lst = [name, bubble, pred]
    print(type(name))
    print(type(bubble))
    # GET request
    if request.method == 'GET':
        message = {'data': lst}
        return jsonify(message)  # serialize and use JSON headers

@app.route('/postmethod', methods = ['POST'])
def get_post_javascript_data():
    match_id = request.form['javascript_data']
    print(match_id)
    wholedf=get_1matchid(match_id)
    print(wholedf.values.tolist())
    wholedf1 = reformat(wholedf.values.tolist())
    bubbledf = reformat_bubble(wholedf.values.tolist())
    print(wholedf1)
    print('Bubble')
    print(bubbledf)
    bucket.blob('sample-output.csv').upload_from_string(wholedf1.to_csv(), 'text/csv')
    bucket.blob('sample-output-modded.csv').upload_from_string(bubbledf.to_csv(), 'text/csv')
    model = RNN()
    model.load_state_dict(torch.load(r"C:\Users\ayman\Dropbox\My PC (LAPTOP-19GOKHVG)\Downloads\model_all_feat.pt"))
    return match_id
    
app.run(debug=True)