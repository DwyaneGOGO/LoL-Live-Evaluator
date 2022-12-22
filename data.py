"""
This code includes all our data preprocessing and fetching data from API 
functions. Also get's predictions from DL model and adds to dataframe
"""
import pandas as pd
import requests
import numpy as np
import csv
from sklearn.preprocessing import StandardScaler
import math
import torch
import warnings
warnings.filterwarnings('ignore')

RIOT_API_KEY = 'RGAPI-56c8bcdb-2691-4f64-b485-676870e8a883' #Riot API key
RIOT_REGION = 'AMERICAS'


def get_match_json(matchid):
    #Function to get raw data from API based on match id
    api_key=RIOT_API_KEY
    region=RIOT_REGION
    
    url_pull_match = "https://{}.api.riotgames.com/lol/match/v5/matches/{}/timeline?api_key={}".format(region, matchid, api_key)
    match_data_all = requests.get(url_pull_match).json() #Get Match Data from API
    return match_data_all

def subtract_list(list1,list2):
    #Simple function to subtract two numpy arrays
    array1 = np.array(list1)
    array2 = np.array(list2)
    subtracted_array = np.subtract(array1, array2)
    subtracted = list(subtracted_array)
    return subtracted

def get_player_stats(match_data, player,time): #  player has to be an int (1-10)
    # Get player information 

    player_query = match_data['info']['frames'][time]['participantFrames'][str(player)]
    player_total_gold = player_query['totalGold']
    return player_total_gold 

def cal_gold_dif(golddiff,data,time):
    #Calculate golddiff by adding each players gold through stats data and then taking the difference
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
    #Function that takes match_id and returns data in specfic dataframe format
    outputdf=pd.DataFrame(columns=['golddiff','dragondiff','barondiff','heralddiff','towerdiff','inhibitordiff','killdiff']) #Creating initial empty pandas dataframe
    
    json1=get_match_json("NA1_"+str(matchid))#Get match data in json format from match ID
         
    json2=json1['info']['frames'] #To find number of frames/game length
       
    gamelength=len(json2)
    
    #Define initial arrays to append to our final output dataframe
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
        event_query = json1['info']['frames'][i]['events'] #Get event query from our json match data
        
        df=pd.DataFrame.from_dict(event_query)
        winner,team1drag,team2drag,team1baron,team2baron,team1herald,team2herald,team1turrent,team2turrent,team1inhib,team2inhib,team1kill,team2kill=append_1min_stat(
        team1drag,team2drag,team1baron,team2baron,team1herald,team2herald,team1turrent,team2turrent,team1inhib,team2inhib,team1kill,team2kill,df,(i+1))
        golddiff=cal_gold_dif(golddiff,json1,i) #golddiff needs to be separate since we collect them from player stats
    
    #Get complete dataframe with all differences in specific order as DL model to append to output
    df2=pd.DataFrame([[golddiff,subtract_list(team1drag,team2drag),subtract_list(team1baron,team2baron),subtract_list(team1herald,team2herald),
                         subtract_list(team1turrent,team2turrent),subtract_list(team1inhib,team2inhib),subtract_list(team1kill,team2kill),winner ]],columns=['golddiff','dragondiff','barondiff','heralddiff','towerdiff','inhibitordiff','killdiff','winner'])
    
    #Concatanate for final output result
    outputdf=pd.concat([outputdf,df2],ignore_index=True)
        
    return outputdf

def reformat(data_list, model):
    #Function to reformat data taken from API to appopriate format that html code expects for displaying data 
    red, blue = get_predictions(data_list, model)
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
    #Function to reformat data taken from API to appropriate format that html code expects for displaying bubble chart
    head = ['val', 'id' ,'groupid' ,'size']
    with open('sample-output-modded.csv', 'w') as f:
        write = csv.writer(f)
        write.writerow(head)
        for val in range(len(data_list[0][1])):
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
                write.writerow([val, id, inhibitor_group_id, size])
    new = pd.read_csv('sample-output-modded.csv')
    return new

def get_predictions(data_list, model):
    #Function that runs our pretrained RNN model on data to get odds for a match
    MAX_TIME_STEP = len(data_list[0][0]) #Max time step our model needs to run
    
    red = []
    blue = []
    scalers = {}
    
    #Scale the data similar to the data that the model has been trained on to improve convergence
    for i in range(len(data_list[0])-1):
        #Define scalers for our data
        scalers[i] = StandardScaler()
        for row in data_list[0][i]:
            scalers[i].partial_fit(np.asanyarray(row).reshape(-1, 1))
    
    for i in range(len(data_list[0])-1):
        #Scale our data
        data_list[0][i] = scalers[i].transform(np.asanyarray(data_list[0][i]).reshape(-1, 1)).reshape(-1)
    
    #Now we can finally get our model's output from the data
    for i in range(MAX_TIME_STEP):
      max = i+1 #Input feature predictions for each step until max, which increments till it reached max_time_step
      x = np.asarray([[ [data_list[0][i][timestep] for i in range(len(data_list[0])-1)] for timestep in range(max) ]], dtype=np.float32)
     
      model.eval() #Place model in eval mode for testing
      with torch.no_grad():
          #Convert input array to torch tensor
          x = torch.from_numpy(x)
          
          #Predict odds from model
          predict = model(x)
          winner = ['red', 'blue'][predict.argmax(1)]
          prob_red = math.exp(predict[0][0].item()) / (math.exp(predict[0][0].item()) + math.exp(predict[0][1].item()))
          prob_blue = math.exp(predict[0][1].item()) / (math.exp(predict[0][0].item()) + math.exp(predict[0][1].item()))
          
          #Append the odds for each team
          red.append(prob_red*100)
          blue.append(prob_blue*100)
    return red, blue