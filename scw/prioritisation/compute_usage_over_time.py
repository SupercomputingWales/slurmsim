#!/usr/bin/env python3
import pandas as pd
import numpy as np
from sys import argv
import matplotlib.pyplot as plt

#max_cpus=(40*(136+26+13+26+3+1+1)) # HAWK
max_cpus=5040 # SUNBIRD

plt.figure(figsize=(8,7.0))
for i,arg in enumerate(argv[1::2]):
    data_original = pd.read_csv(arg,sep='|')
    cancelled_jobs = data_original.State.str.contains('CANCELLED')
    data_without_cancelled = data_original.loc[~cancelled_jobs,:]

    for data,label in (data_original,''),(data_without_cancelled,'_nocancel'):
        
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
        
        plt.step(events.Time,events.Used,label=argv[2+i*2]+label)




plt.ylabel('Number of CPUs in use')
plt.setp(plt.xticks()[1] ,rotation = 45, ha = 'right')
plt.legend()
plt.show()
