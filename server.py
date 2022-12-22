"""
This is the main code for running our application.
It defines and runs our flask application.

"""

from google.cloud import storage
from io import StringIO
import pandas as pd
from flask import Flask, render_template, request
import csv
import torch
import warnings
from ast import literal_eval
warnings.filterwarnings('ignore')
from flask import jsonify

from models import RNN, RNN_ng
from data import get_1matchid, reformat, reformat_bubble, get_predictions

# global variables
bucket = "dataproc-staging-us-central1-419343931639-hthrtj25"
PROJECT_ID = "chrome-insight-363115"

#Path for pretrained live DL model
live_model_path = r"C:\Users\ayman\Dropbox\My PC (LAPTOP-19GOKHVG)\Downloads\model_no_gold.pt"
matchid_model_path = r"C:\Users\ayman\Dropbox\My PC (LAPTOP-19GOKHVG)\Downloads\model_all_feat.pt"
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gs://{}/GFG.csv".format(bucket)

#Define deep learning model that we use for match_id and load pretrained weights from state dict
model = RNN()
model.load_state_dict(torch.load(matchid_model_path))

#Define our client for google cloud storage with our project name
client = storage.Client(project=PROJECT_ID)

bucket = client.get_bucket(bucket)

#Get reformatted data for html page and bubble chart respectively. Requires you to upload csv files with the same names
#in your google cloud storage bucket beforehand

blob1 = bucket.get_blob("sample-output.csv")
blob2 = bucket.get_blob("sample-output-modded.csv")

#Convert blob data to pandas dataframe for html data and bubble data respectively
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

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index-.html') #Use jinja to serve html template as response

@app.route('/index_matchid/')
def matchid():
    #Get reformatted data for html page and bubble chart respectively similar to steps done globally. Just reading new data for new match id.
    blob1 = bucket.get_blob("sample-output.csv")
    blob2 = bucket.get_blob("sample-output-modded.csv")
    
    #Convert new match_id data to pandas dataframe format
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
    
    #Send the data to index_matchid.html using jinja
    return render_template('index_matchid.html',lst=lst)

@app.route('/index_live/')
def matchlive():
    #Get reformatted data for live data streamed to bucket. Then reformat this data to pandas dataframe
    blob1 = bucket.get_blob("leaguedata/text.csv")

    bt1 = blob1.download_as_string()

    s1 = str(bt1, "utf-8")
    s1 = StringIO(s1)

    df = pd.read_csv(s1)
    data = df.values.tolist() #list of outputs
    
    
    #Modify the data to similar format as required for html file and bubble chart
    mod_data = []
    for x in data[0]:
        if len(mod_data)==len(data[0])-1:
            continue
        x = literal_eval(x)
        mod_data.append(x)
    data = []
    #Appending copy to make sure no changes done to actual mod_data when we later convert data to torch tensor for our DL model predictions
    data.append(mod_data.copy()) 
    
    bubble = reformat_bubble(data)
    
    #If nothing has happened yet regarding the dragons, barons, heralds, inhibitors etc, append dummy values that can't be used by html file
    if bubble.empty:
        head = ['val', 'id' ,'groupid' ,'size']
        with open('sample-output-modded.csv', 'w') as f:
            write = csv.writer(f)
            write.writerow(head)
            write.writerow([30, 1, 2, 2000])
        bubble = pd.read_csv('sample-output-modded.csv')
    bubble = bubble.values.tolist()
    
    #Define model for live data which does not make use of golddiff feature, since live API does not have this feature
    model = RNN_ng()
    model.load_state_dict(torch.load(live_model_path))
    
    #Get live predictions from data
    red, blue = get_predictions(data,model)
    
    pred = []
    pred.append(red)
    pred.append(blue)
    
    name = mod_data
    lst = [name, bubble, pred]
    
    #Send data to index_live.html using jinja
    return render_template('index_live.html',lst=lst)

@app.route('/test', methods=['GET'])
def testfn():
    #GET method calls this function everytime live button is pressed in index_live.html to read data from bucket again and reformat data appropriately
    
    #Get reformatted data for live data streamed to bucket. Then reformat this data to pandas dataframe
    blob1 = bucket.get_blob("leaguedata/text.csv")

    bt1 = blob1.download_as_string()

    s1 = str(bt1, "utf-8")
    s1 = StringIO(s1)

    df = pd.read_csv(s1)
    data = df.values.tolist() #list of outputs
    
    #Modify the data to similar format as required for html file and bubble chart
    mod_data = []
    for x in data[0]:
        if len(mod_data)==len(data[0])-1:
            continue
        x = literal_eval(x)
        mod_data.append(x)
    data = []
    #Appending copy to make sure no changes done to actual mod_data when we later convert data to torch tensor for our DL model predictions
    data.append(mod_data.copy())
    
    bubble = reformat_bubble(data)
    
    #If nothing has happened yet regarding the dragons, barons, heralds, inhibitors etc, append dummy values that can't be used by html file
    if bubble.empty:
        head = ['val', 'id' ,'groupid' ,'size']
        with open('sample-output-modded.csv', 'w') as f:
            write = csv.writer(f)
            write.writerow(head)
            write.writerow([30, 1, 2, 2000])
        bubble = pd.read_csv('sample-output-modded.csv')
    bubble = bubble.values.tolist()
    
    #Define model for live data which does not make use of golddiff feature, since live API does not have this feature
    model = RNN_ng()
    model.load_state_dict(torch.load(live_model_path))
    
    red, blue = get_predictions(data,model)
    pred = []
    pred.append(red)
    pred.append(blue)
    
    name = mod_data
    lst = [name, bubble, pred]
    
    # GET request
    if request.method == 'GET':
        message = {'data': lst}
        return jsonify(message)  # serialize and use JSON headers to send data

@app.route('/postmethod', methods = ['POST'])
def get_post_javascript_data():
    
    #Read match_id from data entered into index-.html file using POST method
    match_id = request.form['javascript_data']
    
    #Get data from API using match_id
    wholedf=get_1matchid(match_id)
    
    
    #Define RNN model
    model = RNN()
    model.load_state_dict(torch.load(matchid_model_path))
    
    #Reformat data to appropriate format that html file expects for accessing data and displaying bubble chart respectively
    wholedf1 = reformat(wholedf.values.tolist(), model)
    bubbledf = reformat_bubble(wholedf.values.tolist())
    
    #Upload data to GCP bucket
    bucket.blob('sample-output.csv').upload_from_string(wholedf1.to_csv(), 'text/csv')
    bucket.blob('sample-output-modded.csv').upload_from_string(bubbledf.to_csv(), 'text/csv')
    return match_id

if __name__ == "__main__":
    #Running our application with Flask as server
    app.run(debug=True)
