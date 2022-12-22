# EECSE6893: Big Data Analytics
## League-of-Legends Evaluator 

In order to try and tackle the issue of digitization of an On-the-Board chess game using a low-power, computationally inexpensive device, we will be attempting to run our chess board to FEN model on the Jetson Nano for this project. We will also look into methods to optimize the performance of the model and integrate it with a chess bot API in order to predict the next moves and analyze the game in real time.

Our implementation can be divided into the following tasks:
-	Reproduce results of LiveChess2FEN paper and compare with existing models.
-	Collect and annotate a new FEN dataset. Use same model setup to test performance in this new dataset
-	Annotate a new chess piece classification dataset from the FEN dataset using pretrained geometric detect board script. Finetune a Resnet18 model for chess piece classification and test.
-	Test optimization using JIT.trace on model and observe results.
-	Generate ONNX model of file and try to test performance.
-	Integrate with Chess API and observe output SVN

Our code makes use of the code from https://github.com/davidmallasen/LiveChess2FEN as starter code, including all folders and setups indicated in the repo. We have modified the test_lc2fen.py file to be our main.py file, and have also made changed to code throughout to include our new functions in the pipeline, such as using preloaded images instead of image_paths in argumenets, skipping over the infer_chess_pieces function in test_predict_board if using pytorch, and so on. <br>

We also make use of functions from previous lab assignments in order to collect our dataset, plot our functions, and so on. We have also referred to teh Nvidia TrT documentation in order to convert our torch file to ONNX, as well as the https://python-chess.readthedocs.io/en/latest/ documentation in order to use our chess engine and display images. <br>
Below is a system overview of our project once again for reference:![System-Overview](https://raw.githubusercontent.com/DwyaneGOGO/LoL-Live-Evaluator/main/templates/system_overview.PNG)
