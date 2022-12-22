# EECSE6893: Big Data Analytics
## League-of-Legends Evaluator 

Over the years, there has been a lot of development done in the field of competitive e-sports viewing, with popular multiplayer online games like League of Legends reaching peak viewers of over 4 Million. Despite this popularity however, there has been a surprisingly lack of projects done in the field of game evaluation for these types of games, unlike games like chess or Go. In this project, we will be focussing on “League of legends”, and will attempt to evaluate the game using standard ML/DL techniques.


Our implementation consists of a web-user-interface to allow players to either evaluate their League of Legends game live by leveraging a pretrained RNN deep learning model, and also allowing them to analyze previously completed game by entering the appropriate Riot match-id. Our web-interface consists of webpages written in html, javascript and d3, as well as Flask and Jinja for the back-end of the application. On the other hand, our deep learning model is trained using Pytorch and pandas.<br>

Below is a system overview of our project once again for reference:![System-Overview](https://raw.githubusercontent.com/DwyaneGOGO/LoL-Live-Evaluator/main/templates/system_overview.PNG)
