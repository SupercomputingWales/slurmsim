#!/usr/bin/env python3
'''
The following assumes that the text to parse was produced with the command

sacctmgr list QOS --parsable2

'''

import pandas as pd
from sys import stdout,argv

data = pd.read_csv(argv[1],sep='|')

QOSES=argv[2] # comma-separated list of QOSes
   
stdout.write(f"Cluster - 'sunbird':QOS='{QOSES}'\n")
stdout.write(f"Parent - 'root'\n")

for t in data.itertuples():
    if pd.na(t.User): # then it's an account line



