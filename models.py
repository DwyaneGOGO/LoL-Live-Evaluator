#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Columbia EECS E6893 Big Data Analytics
"""
This is the main code for running our application.
It runs our flask application.

"""

from google.cloud import storage
from io import StringIO
import pandas as pd
import json
from flask import Flask, render_template, request
import requests
import pandas as pd
import torch
from torch import nn

class RNN(nn.Module):
    #Define RNN model that we make use of for our predictions from match_id
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
    ''''
    Define the RNN model that we make use of for our live predictions. Since our live API dosen't give us golddiff like
    it does for match_id, we make predictions without this feature, thus making the total number of features '6' instead.
    Note that this did not drastically affect our accuracy during testing, and should thus still work fine.
    '''
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