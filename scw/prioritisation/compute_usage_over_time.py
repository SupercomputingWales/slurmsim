#!/usr/bin/env python3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse as ap
import textwrap

#max_cpus=(40*(136+26+13+26+3+1+1)) # HAWK
max_cpus=5040 # SUNBIRD


        
description = 'Plot usage of a SLURM cluster over time,starting from the \n\
output of the sacct command showing NCPUS,Start and End times, with the \n\
--parsable2 option.'

parser = ap.ArgumentParser(description=description)

parser.add_argument("files_and_labels",nargs='+',help=textwrap.dedent('''\
        A list of files and labels in the format
           fileA labelA fileB labelB ...
           '''))
parser.add_argument("--start",help="Left limit of the plot.",required=True)
parser.add_argument("--end",help="Right limit of the plot.",required=True)
parser.add_argument("--max_cpus",help="Number of cpus in the cluster.",type=int,default=max_cpus)

args = parser.parse_args()

files_and_labels = args.files_and_labels
date_start = args.start
date_end = args.end   
max_cpus = args.max_cpus

plt.figure(figsize=(8,7.0))
for i,arg in enumerate(files_and_labels[0::2]):
    data = pd.read_csv(arg,sep='|')

    data.Start = pd.to_datetime(data.Start)
    data.End   = pd.to_datetime(data.End  )
    
    starts = data.loc[:,['Start','NCPUS']]
    ends = data.loc[:,['End','NCPUS']]

    usage = (data.End-data.Start)/np.timedelta64(1,'s') * data.NCPUS
    print('Total core seconds: {}'.format(usage.sum()))

    ends.NCPUS = - ends.NCPUS
    
    ends.columns = ['Time','NCPUS']
    starts.columns = ['Time','NCPUS']
    events  = pd.concat([starts,ends])
    events = events.sort_values(by='Time')
    cores_used = np.cumsum(events.NCPUS)
    events['Used'] = cores_used
    events.loc[events.Used < 0,'Used'] = 0
    events.loc[events.Used > max_cpus,'Used'] = max_cpus
    used_avg = events.loc[:,['Time','Used']].groupby('Time').mean()
    
    plt.step(used_avg.index,used_avg/max_cpus*100,label=files_and_labels[1+i*2])


plt.xlim([pd.to_datetime(date_start),pd.to_datetime(date_end)])
plt.ylabel('Utilisation (%)')
plt.setp(plt.xticks()[1] ,rotation = 45, ha = 'right')
plt.legend()
plt.show()
