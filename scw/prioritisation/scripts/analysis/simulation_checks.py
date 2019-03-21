#!/usr/bin/env python3
'''
This script runs a number of checks on the results of the simulation.
There are 4 files which are needed by the scipt:
    1. jobcompo.log - containing the output of the simulation, in a format 
       similar to sacct_output.csv.
    2. sacct_output.csv - the corresponding live data.
    3. test_trace.csv - the .csv file the binary version of which is fed to the
       simulator
    4. cancelled_jobs.csv - A list of cancelled jobs obtained from slurmctld.log,
       in the format 'CancelTime,JobIDRaw'. Obtaining this requires grepping
       through the slurmctld.log file, which can be hard.
'''

import pandas as pd
import numpy as np
from functools import reduce
trace = pd.read_csv('test_trace.csv')
trace = trace.set_index('sim_job_id')

check_cancellation_times = False

files = {
    'Sim': ('jobcomp.log', 'JobIDRaw', '|'),
    'Live': ('sacct_output.csv', 'JobIDRaw', '|'),
    'Trace': ('test_trace.csv', 'sim_job_id', ','),
}
if check_cancellation_times: 
    files['Canc'] = ('cancelled_jobs.csv', 'JobIDRaw', ',')


dfs = [ (name,pd.read_csv(filename,sep=separator).set_index(idcol)) \
        for (name,(filename,idcol,separator)) in files.items() ]

dfs_dict = dict(dfs)

# JOB NUMBER CHECKS
len_sim = len(dfs_dict['Sim'])
result = len_sim == len(dfs_dict['Live'])
print(f"STATEMENT: The number of jobs in the simulator ({len_sim}) is ")
print(f"STATEMENT: the same as in the live system:")
print(f"STATEMENT: STATUS : {result}")
len_trace = len(dfs_dict['Trace'])
result = len_trace == len(dfs_dict['Live'])
print(f"STATEMENT: The number of jobs in the trace ({len_trace}) is ")
print(f"STATEMENT: the same as in the live system:")
print(f"STATEMENT: STATUS : {result}")

# JOIN ALL
datetime_columns = {
    'End', 'Start', 'Submit', 'sim_submit', 'Eligible', 'CancelTime'
}


for name, df in dfs:
    for column in set(df.columns).intersection(datetime_columns):
        df[column] = pd.to_datetime(df[column])


def single_join(left, right):
    nameL, dfL = left
    nameR, dfR = right
    return '', dfL.join(dfR, rsuffix=nameR, lsuffix=nameL)


joined = reduce(single_join, dfs)[1]

# JOB DURATION CHECKS
durationLive = joined.EndLive - joined.StartLive
# chcking that job durations are the same for live and trace
result = all(durationLive / np.timedelta64(1, 's') == joined.sim_duration)
print(f"STATEMENT: job durations are the same for live and trace:")
print(f"STATEMENT: STATUS : {result}")

# checking differences in duration between simulation and reality
durationSim = joined.EndSim - joined.StartSim
diff = (durationSim - durationLive)
maxDiff = diff.max()
minDiff = diff.min()
print(f"INFO     : Differences in duration, Sim - Live.")
print(f"INFO     : max:{maxDiff}")
print(f"INFO     : min:{minDiff}")

joined['durationLive'] = durationLive
joined['durationSim'] = durationSim

# checking that job durations are different only for cancelled jobs.
threshold = np.timedelta64(5, 's')
job_is_cancelled_live = joined.StateLive.str.contains('CANCELLED')
duration_diff_above_threshold = abs(durationSim - durationLive) > threshold
condition = job_is_cancelled_live | (~duration_diff_above_threshold)
result = all(condition)

print(
    f"STATEMENT: Job durations (live and sim) differ more than {str(threshold)}"
)
print(f"STATEMENT: only for cancelled jobs:")
print(f"STATEMENT: STATUS : {result}")

# CORE-SECONDS CHECK
core_seconds_live = (
    joined.durationLive / np.timedelta64(1, 's') * joined.NCPUSLive).sum()
core_seconds_sim = (
    joined.durationSim / np.timedelta64(1, 's') * joined.NCPUSSim).sum()
print(f"INFO     : Total core seconds on the live system: {core_seconds_live}")
print(
    f"INFO     : Total core seconds on the simulated system: {core_seconds_sim}"
)
core_seconds_sim2 = (
    joined.durationSim / np.timedelta64(1, 's') * joined.NCPUSLive).sum()
print(
    f"INFO     : Total core seconds on the simulated system (*): {core_seconds_sim2}"
)

if check_cancellation_times:
    # CANCEL TIME CHECKS
    condition = (abs(joined.CancelTime - joined.EndLive) < threshold) | pd.isna(
        joined.CancelTime)
    result = all(condition)
    print(
        f"STATEMENT: For the jobs cancelled in the sim, ends differ only by {str(threshold)} max"
    )
    print(
        f"STATEMENT: between the cancellation request to the simulator and the live data:"
    )
    print(f"STATEMENT: STATUS : {result}")
    
    threshold = np.timedelta64(5, 'm')
    job_simulation_ends_before_live = ((joined.EndSim - joined.EndLive) <
                                       threshold)
    condition = job_simulation_ends_before_live | (~job_is_cancelled_live)
    result = all(condition)
    print(
        f"STATEMENT: For the jobs cancelled in the sim, they end before their live"
    )
    print(
        f"STATEMENT: counterpart or max after {str(threshold)} the live counterpart:"
    )
    print(f"STATEMENT: STATUS : {result}")
    
    threshold = np.timedelta64(5, 'm')
    short_duration_threshold = np.timedelta64(2, 's')
    job_sim_lasts_very_little = joined.durationSim < short_duration_threshold
    condition = job_sim_lasts_very_little | ~job_is_cancelled_live
    result = all(condition)
    print(
        f"STATEMENT: Jobs that were cancelled in the live system last less than ")
    print(f"STATEMENT: {str(short_duration_threshold)} in the simulator:")
    print(f"STATEMENT: STATUS : {result}")
    
    # Check that the number of core seconds missing from the simulation is
    # explained by the cancelled jobs
    core_seconds_live_cancelled = (joined.durationLive / np.timedelta64(1, 's') *
                                   joined.NCPUSLive)[job_is_cancelled_live].sum()
    core_seconds_sim_cancelled = (joined.durationSim / np.timedelta64(1, 's') *
                                  joined.NCPUSSim)[job_is_cancelled_live].sum()
    print(
        f"INFO     : Total core seconds on the live system in cancelled jobs: {core_seconds_live_cancelled}"
    )
    print(
        f"INFO     : Total core seconds on the simulated system in cancelled jobs: {core_seconds_sim_cancelled}"
    )
    core_seconds_sim_cancelled2 = (joined.durationSim / np.timedelta64(1, 's') *
                                   joined.NCPUSLive)[job_is_cancelled_live].sum()
    print(
        f"INFO     : Total core seconds on the simulated system in cancelled jobs (*): {core_seconds_sim_cancelled2}"
    )

missing = core_seconds_live - core_seconds_sim2 - core_seconds_live_cancelled + core_seconds_sim_cancelled2
margin = joined.NCPUSLive.sum()
result = missing < margin

print(f"STATEMENT: The number of core seconds missing from the simulation is ")
print(f"STATEMENT: explained by the cancelled jobs:")
print(f"STATEMENT: STATUS : {result}")

if check_cancellation_times:
    # Check that cancellation time in the simulator is logged in at the right time.
    threshold = np.timedelta64(1, 's')
    condition = (abs(joined.CancelTime - joined.EndLive) < threshold) | pd.isna(
        joined.CancelTime)
    result = all(condition)
    print(
        f"STATEMENT: Cancellation time reported in the simulator logs coincide with"
    )
    print(
        f"STATEMENT: the live cancellation times within {str(threshold)} (when reported):"
    )
    print(f"STATEMENT: STATUS : {result}")
    
    cancellations_logged = (~pd.isna(joined.CancelTime)).sum()
    cancellations_live = (job_is_cancelled_live).sum()
    job_is_cancelled_sim = joined.StateSim.str.contains('CANCELLED')
    cancellations_sim = (job_is_cancelled_sim).sum()
    print(
        f"INFO     : Number of cancellation requests logged    :{cancellations_logged}"
    )
    print(
        f"INFO     : Number of cancellation requests live      :{cancellations_live}"
    )
    print(
        f"INFO     : Number of cancellation in simulator output:{cancellations_sim}"
    )
    
    print(f"(*): Computed using NCPUSLive instead of NCPUSSim. ")
    
    print(f"(*): Computed using NCPUSLive instead of NCPUSSim. ")
