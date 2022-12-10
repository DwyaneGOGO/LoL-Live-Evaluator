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
from flask import Flask, render_template

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

app = Flask(__name__)

@app.route('/')
def index():
    name = data
    return render_template('index-.html', name=name)
#with open('graph.js', 'w') as out_file:
#  out_file.write('var data = %s;' % df)
print(df)

app.run(debug=True)