
import numpy as np
import pandas as pd
from sys import argv

from matplotlib import pyplot as plt
from matplotlib import dates as mdates

simulated = pd.read_csv('jobcomp.log',sep='|')
live = pd.read_csv('sacct_output.csv',sep='|')

n_simulated = len(simulated)
n_live = len(live)

join = live.join(simulated.set_index('JobIDRaw'),
        on='JobIDRaw',lsuffix='_live',rsuffix='_sim',how='right')

join['Start_live'] = pd.to_datetime(join['Start_live'])
join['Start_sim'] = pd.to_datetime(join['Start_sim'])
join['Submit_live'] = pd.to_datetime(join['Submit_live'])


join['Delta_Start'] = join['Start_sim']-join['Start_live']
join['Delta_Start'] = join['Delta_Start'].astype('timedelta64[s]')

join['Wait_live'] = join['Start_live'] - join['Submit_live']
join['Wait_live'] = join['Wait_live'].astype('timedelta64[s]')

fig, axs = plt.subplots(nrows=1,ncols=3,figsize=(15.4,7.0))
fig.suptitle(argv[1]+ " ({}[sim]/{}[live])".format(n_simulated,n_live))

axs[0].set_yscale('log',nonposy='clip')
axs[0].set_ylabel('Count')
axs[0].set_xlabel('Delta Start (sim-live) [s]')
axs[0].ticklabel_format(style='sci',scilimits=(-3,4),axis='x')
axs[0].hist(join['Delta_Start'],bins='scott')

axs[1].set_yscale('linear')
axs[1].set_ylabel('Delta Start (sim-live) [s]')
axs[1].set_xlabel('Wait time (live)')
axs[1].ticklabel_format(style='sci',scilimits=(-3,4),axis='both')
axs[1].grid()
axs[1].plot(join['Wait_live'],join['Delta_Start'],linestyle='None',marker='+')

axs[2].set_yscale('linear')
axs[2].set_ylabel('Delta Start (sim-live) [s]')
axs[2].set_xlabel('Start Date (live)')
axs[2].ticklabel_format(style='sci',scilimits=(-3,4),axis='both')
axs[2].grid()
axs[2].plot(join['Start_live'],join['Delta_Start'],linestyle='None',marker='+')

plt.setp(plt.xticks()[1] ,rotation = 45, ha = 'right')




plt.savefig('deltas.png')
