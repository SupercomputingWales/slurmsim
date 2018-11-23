#!/usr/bin/env python3
'''
The following assumes that the text to parse was produced with the command

sacctmgr list QOS --parsable2

'''

import pandas as pd
from sys import stdout,argv

data = pd.read_csv(argv[1],sep='|')
   
for t in data.itertuples():
    stdout.write(f"yes | $SACCTMGR add QOS {t.Name}\n")
    stdout.write(f"yes | $SACCTMGR modify QOS {t.Name} set ")
    for k in set(data.keys())-{'Name','GraceTime','MaxSubmitPU'}:
        if pd.notna(getattr(t,k)):
            stdout.write(f'{k}="{getattr(t,k)}" ')
    # in some cases it does not work. 
    # GraceTime requires a numeric value in seconds instead of a HH:MM:SS
    # string.
    if pd.notna(getattr(t,'GraceTime')):
        grace_time = getattr(t,'GraceTime')
        h,m,s = grace_time.split(":")
        grace_time = 3600*int(h)+60*int(m)+int(s)
        stdout.write(f'GraceTime="{grace_time}" ')
    # The string 'MaxSubmitPU' is not accepted sacctmgr modify ... set 
    # The string 'MaxSubmitJobsPU' is accepted instead
    if pd.notna(getattr(t,'MaxSubmitPU')):
        value = getattr(t,'MaxSubmitPU')
        stdout.write(f'MaxSubmitJobsPU="{value}" ')
    stdout.write('\n')
