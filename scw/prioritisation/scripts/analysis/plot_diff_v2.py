#!/usr/bin/env python3
import numpy as np
import pandas as pd
import argparse as ap
from sys import argv

from matplotlib import pyplot as plt
from matplotlib import dates as mdates

parser = ap.ArgumentParser(
    description="Plot metrics from a pair of result files, or a result file"
    "and a sacct output file.")

parser.add_argument(
    "first",
    type=str,
    help="A .csv file with the simulation results (e.g. 'jobcomp.log')")

parser.add_argument(
    "second",
    type=str,
    help="Another .csv file with the simulation results (e.g. 'sacct_output.csv')")

parser.add_argument(
    "--start",
    type=str,
    help="Start date for the computation of stats.",required=True)
parser.add_argument(
    "--end",
    type=str,
    help="End date for the computation of stas.",required=True)
parser.add_argument(
    "--prioritized_accounts",
    type=str,
    help="A file containing \
a list of prioritized accounts.")

args = parser.parse_args()

if args.prioritized_accounts is not None:
    with open(args.prioritized_accounts, 'r') as f:
        prioritized_accounts = set(f.read().split()) - {''}

first = pd.read_csv(args.first,sep='|')
second = pd.read_csv(args.second,sep='|')
date_start = pd.to_datetime(args.start)
date_end = pd.to_datetime(args.end)

print(date_start)
print(date_end)

join = first.join(second.set_index('JobIDRaw'),
        on='JobIDRaw',lsuffix='_first',rsuffix='_second',how='right')

# conversions to datetime
join.loc[:,'Start_first'] = pd.to_datetime(join['Start_first'])
join.loc[:,'Start_second'] = pd.to_datetime(join['Start_second'])
join.loc[:,'End_first'] = pd.to_datetime(join['End_first'])
join.loc[:,'End_second'] = pd.to_datetime(join['End_second'])
join.loc[:,'Submit_first'] = pd.to_datetime(join['Submit_first'])
# deltas
join.loc[:,'Delta_Start'] = join['Start_second']-join['Start_first']
join.loc[:,'Wait_first'] = join['Start_first'] - join['Submit_first']
# conversion to numbers
join.loc[:,'Delta_Start'] = join['Delta_Start'].astype('timedelta64[s]')
join.loc[:,'Wait_first'] = join['Wait_first'].astype('timedelta64[s]')

cut_condition = (join.Start_first > date_start ) & (join.End_first < date_end )

join = join.loc[cut_condition,:]



plt.figure(1,figsize=(8.0,7.0))
plt.ylabel('Count')
plt.xlabel('Difference in queue wait time [h]')
plt.xlim([-3,3])
plt.ticklabel_format(style='sci',scilimits=(-3,4),axis='x')
if args.prioritized_accounts is not None :
    plt.hist(join['Delta_Start']/3600,bins=200,range=(-10,10),label='Prioritized')
    condition = np.array([ acc not in prioritized_accounts for acc in join.Account_first ])
    plt.hist(join['Delta_Start'][condition]/3600,bins=200,range=(-10,10),label = 'Non Prioritized')
else : 
    plt.hist(join['Delta_Start']/3600,bins=200,range=(-10,10))

plt.legend()
plt.savefig('deltas_histogram.png')

theshold = 0
print("Anticipated jobs: {}".format((join.Delta_Start < theshold ).sum()))

plt.figure(2,figsize=(8.0,7.0))
plt.ylabel('Difference in queue wait time [h]')
plt.ylim([-3,3])
plt.xlabel('Start Date ')
plt.ticklabel_format(style='sci',scilimits=(-3,4),axis='both')
plt.grid()
if args.prioritized_accounts is not None :
    condition = np.array([ acc not in prioritized_accounts for acc in join.Account_first ])
    plt.plot(join['Start_first'][condition],join['Delta_Start'][condition]/3600,linestyle='None',marker='+',label= 'Non Prioritized')
    plt.plot(join['Start_first'][~condition],join['Delta_Start'][~condition]/3600,linestyle='None',marker='+',label= 'Prioritized')
else:
    plt.plot(join['Start_first'],join['Delta_Start']/3600,linestyle='None',marker='+')
plt.setp(plt.xticks()[1] ,rotation = 45, ha = 'right')
plt.legend()

plt.savefig('deltas_time.png')
