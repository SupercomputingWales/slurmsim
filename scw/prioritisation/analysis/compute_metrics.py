#!/usr/bin/env python3
'''
Implementation of the metrics described by Ed Bennett in the email sent to
the Queue Prioritization Subcommittee on 21.01.2019.

This script reads the output of a simulation (jobcomp.log) and computes the
metrics required. The format of the output must be

'JobID'
'JobIDRaw'
'Cluster'
'Partition'
'Account'
'Group'
'GID'
'User'
'UID'
'Submit'
'Eligible'
'Start'
'End'
'Elapsed'
'ExitCode'
'State'
'NNodes'
'NCPUS'
'ReqCPUS'
'ReqMem'
'Timelimit'
'NodeList'
'QOS'
'ScheduledBy'
'JobName'

The metrics are:

• Average queue time per job for all users
• Upper and lower quartile queue time per job for all users
• Maximum queue time for any job for all users
• Total core hours for all users
• Average queue time per job for prioritised users
• Upper and lower quartile queue time per job for prioritised users
• Maximum queue time for any job for prioritised users
• Total core hours for prioritized users
• Average queue time per job for non-prioritised users
• Upper and lower quartile queue time per job for non-prioritised users
• Maximum queue time for any job for non-prioritised users
• Total core hours for non-prioritized users
• Average queue time per job for “long tail” users
• Upper and lower quartile queue time per job for “long tail" users
• Maximum queue time for any job for “long tail” users
• Total core hours for "long tail" users
'''
import pandas as pd
import argparse as ap
import numpy as np

# Parsing arguments
# Transforming column formats for convenience
def fix_dataframe(simulated_data):
    column_names_to_datetime = {'Submit', 'Eligible', 'Start', 'End'}
    
    for colname in column_names_to_datetime.intersection(set(simulated_data.columns)):
        simulated_data[colname] = pd.to_datetime(simulated_data[colname])
    
    # Pandas cannot properly parse this column, but it is equal to End-Start,
    # so we substitute it.
    simulated_data['Elapsed'] = simulated_data.End - simulated_data.Start

# COMPUTING METRICS

# Functions for metrics
functions_for_cart_product = []
funnames_short_to_long = dict()


def avgwait(df,starttime,endtime):
    return (df.Start - df.Submit).mean()

fname = "Avgqt"
fname_long = "Average " + 'queue time per job for '
funnames_short_to_long[fname] = fname_long
functions_for_cart_product.append((avgwait, fname))


def maxwait(df,starttime,endtime):
    return (df.Start - df.Submit).max()


fname = "Maxqt"
fname_log = "Maximum " + 'queue time per job for '
funnames_short_to_long[fname] = fname_long
functions_for_cart_product.append((maxwait, fname))

def lower_quartile(df,starttime,endtime):
    arr = (df.Start - df.Submit).astype('timedelta64[s]')
    q25 = np.percentile(arr, q=25).astype('timedelta64[s]')
    return pd.to_timedelta(q25,'s')

fname = "q25qt"
fname_long = "(seconds) Lower quartile " + 'queue time per job for '
funnames_short_to_long[fname] = fname_long
functions_for_cart_product.append((lower_quartile, fname))

def upper_quartile(df,starttime,endtime):
    arr = (df.Start - df.Submit)
    q75 = np.percentile(arr, q=75).astype('timedelta64[s]')
    return pd.to_timedelta(q75,'s')


fname = "q75qt"
fname_long = "(seconds) Upper quartile " + 'queue time per job for '
funnames_short_to_long[fname] = fname_long
functions_for_cart_product.append((upper_quartile, fname))

def corehours(df,starttime,endtime):
    s = df.Elapsed.astype('timedelta64[h]')*df.NCPUS
    crop = df.loc[:,['Start','End']].copy()
    crop.loc[crop.Start < starttime,'Start'] = starttime
    crop.loc[crop.End   > endtime,'End'] = endtime
    cropped_elapsed = crop.End - crop.Start
    s = cropped_elapsed.astype('timedelta64[h]')*df.NCPUS
    return s.sum()

fname = "TCH"
fname_long = "Total Core Hours "
funnames_short_to_long[fname] = fname_long
functions_for_cart_product.append((corehours, fname))

# THIS MUST BE ADDED BY HAND
def tot_core_hours_available(date_start,date_end,totncpus):
    return (date_end - date_start).total_seconds()/3600 * totncpus


# user sets
ds_short_to_long = {'pr':'prioritised',
                   'no-pr':'non prioritised',
                   'lt':'"long tail"'}

def prepare_datasets(simulated_data,date_start,date_end,prioritized_accounts):
    datasets = []
    
    #cut_condition = (simulated_data.Start > date_start ) & (simulated_data.End < date_end)
    cut_condition = (simulated_data.End   > date_start ) & (simulated_data.Start < date_end)
    
    print(f"[DEBUG] Min date: {simulated_data.Submit.min()}")
    print(f"[DEBUG] Max date: {simulated_data.End.max()}")
    print(f"[DEBUG] Analysis Date interval: {date_start}-{date_end}")
    simulated_data = simulated_data.loc[cut_condition,:].copy()
    
    print(f"[DEBUG] Nuber of job in dataframe between selected dates: {len(simulated_data)}")
    
    datasets.append((simulated_data, 'all'))
    if prioritized_accounts is not None:
        # Prioritized and non-prioritized users
   
        simulated_data_prioritized = simulated_data.loc[
                [acc in prioritized_accounts for acc in simulated_data.Account], :]
    
        #print(simulated_data_prioritized)
        if len(simulated_data_prioritized) != 0:
            datasets.append((simulated_data_prioritized, 'pr'))
        else:
            print("WARNING: No jobs found for prioritized accounts.")
    
        simulated_data_non_prioritized = simulated_data.loc[[
            acc not in prioritized_accounts for acc in simulated_data['Account']
        ], :]
        #print(simulated_data_non_prioritized)
        if len(simulated_data_non_prioritized) != 0:
            datasets.append((simulated_data_non_prioritized, 'no-pr'))
        else:
            print("WARNING: No jobs found for non-prioritized accounts.")
    
    # Long-tail users
    # Long tail users are defined as the least active users who consume in total
    # less than 5% of the total resources.
    
    simulated_data['CoreSeconds'] = simulated_data.NCPUS * simulated_data.Elapsed
    simulated_data.CoreSeconds = simulated_data.CoreSeconds.astype('timedelta64[s]')
    
    users_core_seconds_job = simulated_data.loc[:,['User','Account','CoreSeconds']]\
    
    users_core_seconds = users_core_seconds_job.groupby(['User', 'Account']).sum()
    
    users_core_seconds = users_core_seconds.sort_values(by='CoreSeconds')
    
    TotalCoreSeconds = users_core_seconds.CoreSeconds.sum()
    
    TailDefinition = 0.05
    
    i = 0
    while users_core_seconds.head(
            i + 1).CoreSeconds.sum() < TotalCoreSeconds * TailDefinition:
        i += 1
    
    long_tail_users = set(
        users_core_seconds.head(i).index.get_level_values('User'))
    long_tail_accounts = set(
        users_core_seconds.head(i).index.get_level_values('Account'))
    
    simulated_data_long_tail = simulated_data.loc[
        [user in long_tail_users for user in simulated_data['User']], :]
    
    datasets.append((simulated_data_long_tail, 'lt'))
    return datasets

#print("Total core*seconds: {:e}".format(TotalCoreSeconds))
#print("Tail users total time: {:e}".format(users_core_seconds.head(i)['CoreSeconds'].sum()))
#print("Long-tail users:")
#print(long_tail_users)

def keyname(func, dataset):
    f, fname = func
    d, dname = dataset
    return fname + dname 

def keyname_long(func, dataset):
    f, fname = func
    d, dname = dataset
    fname = funnames_short_to_long[fname]
    dname = ds_short_to_long[dname]
    return ' '.join([fname,dname,'user'])


short_to_long = dict()

for func in functions_for_cart_product:
   for dataset in [(None,'all'),(None,'pr'),(None,'no-pr'),(None,'lt')]:
       short_name = keyname(func, dataset)
       long_name  = keyname_long(func, dataset)
       short_to_long[short_name] = long_name

short_to_long['TotCHAvail']  = "Total core hours available"
 
# Calculating metrics

def calc_metrics(simulated_data, date_start, date_end,prioritized_accounts,totncpus):
    metrics = dict()
    
    datasets = prepare_datasets(simulated_data, date_start,date_end, prioritized_accounts)
    
    for func in functions_for_cart_product:
        for dataset in datasets:
            f, fname = func
            d, dname = dataset
            key = keyname(func, dataset)
    
            value = f(d,date_start,date_end)
            metrics[key] = value

    metrics['TotCHAvail'] = tot_core_hours_available(date_start,date_end,totncpus)
    print(f"[DEBUG] Total core hours available: {metrics['TotCHAvail']}")
    
    return metrics


def get_palist(filename):
    if filename is not None:
        with open(filename, 'r') as f:
            prioritized_accounts = [l.strip() for l in set(f.read().split()) if l != '']
    else:
        prioritized_accounts = None
    
    return prioritized_accounts


if __name__ == "__main__":
    parser = ap.ArgumentParser(
        description="Compute metrics given a csv file output\
    from the slurm simulator.")
    
    parser.add_argument(
        "--simresults",
        type=str,
        help="The .csv file with the simulation results (e.g. 'jobcomp.log')",
        required=True)
    parser.add_argument(
        "--pa",
        type=str,
        help="A file containing a list of prioritized accounts.")
    parser.add_argument(
        "--start",
        type=str,
        help="Start date for the computation of stats (Format DDMMYY).",required=True)
    parser.add_argument(
        "--end",
        type=str,
        help="End date for the computation of stats (Format DDMMYY).",required=True)

    parser.add_argument(
        "--totncpus",
        type=int,
        help="Number of CPUS in the cluster (to compute total core hours availability).",
        required=False,
        default = 126*40) # default : sunbird
    
    args = parser.parse_args()
    
    date_start = pd.to_datetime(args.start,format="%d%m%y")
    date_end = pd.to_datetime(args.end,format="%d%m%y")
    
    simulated_data = pd.read_csv(args.simresults, sep='|')
    print(f"[DEBUG] Nuber of job in {args.simresults}: {len(simulated_data)}")

    fix_dataframe(simulated_data)

    prioritized_accounts = get_palist(args.pa)

    metrics = calc_metrics(simulated_data,date_start,date_end,prioritized_accounts,args.totncpus)

    maxl = max([len(k) for k in metrics.keys()])

    for k,v in metrics.items():
         print(f"{k:<{maxl}} : {v}")

 

