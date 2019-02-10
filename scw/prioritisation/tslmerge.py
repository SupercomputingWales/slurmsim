import pandas as pd
import numpy as np
trace = pd.read_csv('test_trace.csv')
trace = trace.set_index('sim_job_id')

simulated = pd.read_csv('jobcomp.log',sep='|')
simulated = simulated.set_index('JobIDRaw')

live = pd.read_csv('sacct_output.csv',sep='|')
live = live.set_index('JobIDRaw')

for column in 'End','Start':
    for df in simulated,live:
        df[column] = pd.to_datetime(df[column])



trace_sim = trace.join(simulated)
trace_sim_live = trace_sim.join(live,rsuffix='L')
columns_to_drop = [ column+'L' for column in simulated.columns if column in live.columns and all(simulated[column][simulated.index] == live[column][simulated.index])]
columns_to_drop += ['sim_tasks']

tsl = trace_sim_live.drop(columns_to_drop,axis = 'columns')

tsl['DurationL'] = tsl.EndL - tsl.StartL
tsl['Duration'] = tsl.End - tsl.Start

tsl['CoreSecL'] = tsl.DurationL / np.timedelta64(1,'s') * tsl.NCPUSL
tsl['CoreSec']  = tsl.Duration  / np.timedelta64(1,'s') * tsl.NCPUS

tsl['DurationDiffRatio'] = (tsl.DurationL - tsl.Duration)/(tsl.DurationL + tsl.Duration)
