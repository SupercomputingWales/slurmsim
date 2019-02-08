#!/usr/bin/env python3
import pandas as pd
import numpy as np
from sys import argv
import matplotlib.pyplot as plt

data = pd.read_csv(argv[1],sep='|')

data.Start = pd.to_datetime(data.Start)
data.End   = pd.to_datetime(data.End  )

starts = data.loc[:,['Start','NCPUS']]
ends = data.loc[:,['End','NCPUS']]
ends.NCPUS = - ends.NCPUS
starts = starts.set_index('Start')
ends = ends.set_index('End')
events  = pd.concat([starts,ends])
events = events.sort_index()
cores_used = np.cumsum(events.NCPUS)
events['Used'] = cores_used

plt.plot(events.index,events.Used)
plt.show()
