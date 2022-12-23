# EECSE6893: Big Data Analytics
## League-of-Legends Evaluator 

Over the years, there has been a lot of development done in the field of competitive e-sports viewing, with popular multiplayer online games like League of Legends reaching peak viewers of over 4 Million. Despite this popularity however, there has been a surprisingly lack of projects done in the field of game evaluation for these types of games, unlike games like chess or Go. In this project, we will be focussing on “League of legends”, and will attempt to evaluate the game using standard ML/DL techniques.


Our implementation consists of a web-user-interface to allow players to either evaluate their League of Legends game live by leveraging a pretrained RNN deep learning model, and also allowing them to analyze previously completed game by entering the appropriate Riot match-id. Our web-interface consists of webpages written in html, javascript and d3, as well as Flask and Jinja for the back-end of the application. On the other hand, our deep learning model is trained using Pytorch and pandas.<br>

Below is a simplified system overview of our project for reference:![System-Overview](https://raw.githubusercontent.com/DwyaneGOGO/LoL-Live-Evaluator/main/templates/Pipeline-web.PNG)

We also have a sample video of our application being used in a live match here(it shows the data being uplaoded to our gcp bucket, as well as the analysis for the first 30 mins of the game): https://youtu.be/Uubxadngviw

In order to setup the environment to run the code, you will need to do the following:
1) Make sure that all required libraries, such as Google cloud storage, Flask, Pytorch, pandas, numpy and riotwatcher.
2) Run "gcloud init" on the terminal and intialize your google cloud account with the terminal in order to have permissions to store data onto your GCP bucket
3) Have temperory files saved into your google cloud bucket called 'sample-output.csv','sample-output-modded.csv' and "leaguedata/text.csv". You can use our dummy files(saved in sample-data) and upload them to the bucket, or make your own and save them. These will be overwritten by our code when the project runs regardless, so the content of the files do not really matter.
4) Clone the repository, make sure you enter the right variables for bucket-names, project ids and pretrained model path in server.py, data.py and models.py, and run "python server.py".
5) Head to "http://127.0.0.1:5000/" to see the flask application running. You can now enter match_ids or use the website as intended. Note that if you want to analyze live gameplay, you will need to run the "live_stream.ipynb" on the system that is running the League of legends game. This will allow the code to stream the data at every minute and upload it to the gcp bucket for our website to analyze. 

Note: We have also included the link to our concatanated dataset here for training: https://drive.google.com/drive/folders/1S4PiVpN3Raxffhfufvz5PHP5HnFXJr0N?usp=sharing
