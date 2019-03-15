#!/usr/bin/env python3
'''
Improved version which also allows to change the QOS priority in slurm.conf.
Requires the new QOS priority value as parameter and also a slur.conf.template file
(in config)
'''

import configparser
import pandas as pd
import numpy as np
import argparse as ap
import re

# INPUT

parser = ap.ArgumentParser(
        description = 'Prepare a number of files for the slurm simulator run')

parser.add_argument("config",type=str,help="A configuration file containing \
input/output file names and parameters.")
parser.add_argument("--start",type=str,help="Start date for the simulation \
(in DDMMYY format)",required=True)
parser.add_argument("--end",type=str,help="End date for the simulation \
(in DDMMYY format)",required=True)
parser.add_argument("--newqosprio",type=int,help="New value for PriorityWeightQOS\
 in slurm.conf",required=True)


args = parser.parse_args()
config_file = args.config

print(f"Reading {config_file}")
config = configparser.ConfigParser()
config.read(config_file)

# input from config file
input_config = config['INPUT']
prioritized_accounts_file = input_config['PrioritizedAccountFile']
sacct_output_wide = input_config['SacctOutputWide']
qos_file_start = input_config['QOSFile']
sacctmgr_dump_file = input_config['SacctmgrDump']
slurm_conf_template = input_config['SlurmConfTemplate']

# input from command line
date_start = args.start
date_start = pd.to_datetime(date_start,format='%d%m%y')
date_end   = args.end  
date_end   = pd.to_datetime(date_end  ,format='%d%m%y')

newqospriorityweight = args.newqosprio


# output from config file
output_config = config['OUTPUT']
output_sacct_cut = output_config['SacctOutputCut']
output_QOS = output_config['ModifiedQOSFileOutput']
output_sacctmgr_dump_file = output_config['SacctmgrDumpOutput']
slurm_conf_output = output_config['SlurmConfOutput']

cdict = config['CVALUES']

# PROCESSING JOB DATA
print(f"Reading {sacct_output_wide}")
job_data = pd.read_csv(sacct_output_wide, sep='|')

#removing the lines where the start or the end times are not known

job_data_cleaned = job_data.loc[np.logical_and(
    [x != 'Unknown'
     for x in job_data['Start']], [x != 'Unknown'
                                   for x in job_data['End']]), :].copy()

# converting the columns containing datetimes to datetimes
# we hope that the pandas method is clever enough for this.
column_names_to_datetime = {'Submit', 'Start', 'End'}

for colname in column_names_to_datetime.intersection(
        set(job_data_cleaned.columns)):
    job_data_cleaned[colname] = pd.to_datetime(job_data_cleaned[colname])

job_data_cleaned[
    'duration'] = job_data_cleaned['End'] - job_data_cleaned['Start']
job_data_cleaned[
    'CoreHours'] = job_data_cleaned['duration'] * job_data_cleaned['NCPUS'] / np.timedelta64(1,'h')

# computing corehours spent by account and partition
account_corehours_partitions = job_data_cleaned.loc[
        job_data_cleaned['End'] < date_start, 
        [ 'Account', 'CoreHours', 'Partition' ]
       ].groupby(['Account', 'Partition']).sum()

cdict = dict(cdict)
partitions = account_corehours_partitions.index.levels[1]
for partition in partitions:
    if partition not in cdict:
        cdict[partition] = 0.02 # DEFAULT VALUE
    else:
        cdict[partition] = float(cdict[partition])

# PREPARING NEW QOS LEVELS
print(f"Reading {prioritized_accounts_file}")
prioritized_accounts = pd.read_csv(prioritized_accounts_file,
        skip_blank_lines=True, comment='#')
# we set the index to be equal to the account name for convenience.
prioritized_accounts = prioritized_accounts.set_index('Account')

# Condition A_t - sum_n c_n t_n > 0
# Multplying priority it by 1000 because: 
# - only the integer part will be accepted
# - it will be normalised to the highest
prioritized_accounts['PriorityBoost'] = False
acpch = account_corehours_partitions['CoreHours']
for account in prioritized_accounts.index:
    c_terms = [ cdict[partition]*acpch[account,partition] for partition in cdict if (account,partition) in acpch ]
    prioritized_accounts.loc[account,'PriorityBoost'] = \
    int(1000*np.log10(1+prioritized_accounts['Attribution'][account])) if\
    len(c_terms) > 0 and sum(c_terms) > 0.0 else 0

print(f"Reading {qos_file_start}")
QOS_data = pd.read_csv(qos_file_start, sep='|')
# we set the index to be equal to the QOS name for convenience.
QOS_data = QOS_data.set_index('Name')

# Creating new QOSes as copies of the 'compute_default' one
for account in prioritized_accounts.index:
    QOS_data.loc[account, :] = QOS_data.loc['compute_default', :]

# setting up new QOSes
for account in prioritized_accounts.index:
    QOS_data.loc[account,'Priority'] = int(1000*np.log10(
        1.0 + prioritized_accounts['Attribution'][account]))

# Saving the modified version of QOS_data
print(f"Writing {output_QOS}")
QOS_data.to_csv(output_QOS, sep='|')


# dropping created columns and creating a new object 
job_data_output = job_data_cleaned.loc[
    (job_data_cleaned['End'] > date_start)&(job_data_cleaned['Start'] < date_end ),: ].drop(['duration','CoreHours'],axis = 1 ).set_index('JobIDRaw')
    
# changing QOS in sacct_output.csv
for account in prioritized_accounts.index:
    condition = job_data_output['Account'] == account
    job_data_output.loc[condition,'QOS'] = account

# converting back to datetimes to string
for colname in column_names_to_datetime.intersection(
       set(job_data_cleaned.columns)):
    job_data_output[colname] = job_data_output[colname].dt.\
         strftime("%Y-%m-%dT%H:%M:%S")


print(f"Writing {output_sacct_cut}")
job_data_output.to_csv(output_sacct_cut,sep='|')


# Adjusting the sacctmgr dump
print(f"Reading {sacctmgr_dump_file}")
with open(sacctmgr_dump_file,'r') as f:
    cluster_dump_lines = f.readlines()

account_definition_line_matches = [ re.match("Account - '(scw[0-9]{4})'",
    line) for line in cluster_dump_lines]

accounts_to_change = set(prioritized_accounts.index)
 
lines_to_change = [ (match.group(1) in accounts_to_change,match.group(1)) if match is not None else (False,None) for match in account_definition_line_matches ] 

new_cluster_dump_lines = [
    old_line[:-1]+f":QOS='+{account}'\n" if change is True else old_line 
    for old_line,(change,account) in zip(cluster_dump_lines,lines_to_change) ] 

print(f"Writing {output_sacctmgr_dump_file}")
with open(output_sacctmgr_dump_file,'w') as f:
    f.write("".join(new_cluster_dump_lines))

# Adjusting slurm.conf
print(f"Reading {slurm_conf_template}")
with open(slurm_conf_template,'r') as f:
    slurm_conf_lines = f.readlines()

new_slurm_conf_lines = [ l.replace('PRIORITYWEIGHTQOSPLACEHOLDER',str(newqospriorityweight))
    for l in slurm_conf_lines ]

print(f"Writing {slurm_conf_output}")
with open(slurm_conf_output,'w') as f:
    f.write("".join(new_slurm_conf_lines))




