#!/usr/bin/env python3
import configparser
from sys import argv
import pandas as pd
import numpy as np
import re

# INPUT
print(f"Reading {argv[1]}")
config = configparser.ConfigParser()
config.read(argv[1])
#config.read('config')

input_config = config['INPUT']
prioritized_accounts_file = input_config['PrioritizedAccountFile']
sacct_output_wide = input_config['SacctOutputWide']
qos_file_start = input_config['QOSFile']
date_start = input_config['DateStart']
sacctmgr_dump_file = input_config['SacctmgrDump']

output_config = config['OUTPUT']
output_sacct_cut = output_config['SacctOutputCut']
output_QOS = output_config['ModifiedQOSFileOutput']
output_sacctmgr_dump_file = output_config['SacctmgrDumpOutput']

cdict = config['CVALUES']

print(f"Reading {prioritized_accounts_file}")
prioritized_accounts = pd.read_csv(prioritized_accounts_file,
        skip_blank_lines=True, comment='#')
# we set the index to be equal to the account name for convenience.
prioritized_accounts = prioritized_accounts.set_index('Account')


print(f"Reading {sacct_output_wide}")
job_data = pd.read_csv(sacct_output_wide, sep='|')

print(date_start)
date_start = pd.to_datetime(date_start)


print(f"Reading {qos_file_start}")
QOS_data = pd.read_csv(qos_file_start, sep='|')
# we set the index to be equal to the QOS name for convenience.
QOS_data = QOS_data.set_index('Name')

# PROCESSING
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

# 
account_corehours_partitions = job_data_cleaned.loc[
        job_data_cleaned['End'] < date_start, 
        [ 'Account', 'CoreHours', 'Partition' ]
       ].groupby(['Account', 'Partition']).sum()


cdict = dict(cdict)
partitions = account_corehours_partitions.index.levels[1]
for partition in partitions:
    if partition not in cdict:
        cdict[partition] = 0.02
    else:
        cdict[partition] = float(cdict[partition])


# Condition A_t - sum_n c_n t_n > 0
prioritized_accounts['PriorityBoost'] = False
acpch = account_corehours_partitions['CoreHours']
for account in prioritized_accounts.index:
    c_terms = [ cdict[partition]*acpch[account,partition] for partition in cdict if (account,partition) in acpch ]
    prioritized_accounts.loc[account,'PriorityBoost'] = \
    np.log10(1+prioritized_accounts['Attribution'][account]) if\
    len(c_terms) > 0 and sum(c_terms) > 0.0 else 0

# Creating new QOSes as copies of the 'compute_default' one
for account in prioritized_accounts.index:
    QOS_data.loc[account, :] = QOS_data.loc['compute_default', :]

# setting up new QOSes
for account in prioritized_accounts.index:
    QOS_data.loc[account,'Priority'] = np.log10(
        1.0 + prioritized_accounts['Attribution'][account])

# Saving the modified version of QOS_data
print(f"Writing {output_QOS}")
QOS_data.to_csv(output_QOS, sep='|')


# dropping created columns and creating a new object 
job_data_output = job_data_cleaned.loc[
    job_data_cleaned['End'] > date_start,: ].drop(['duration','CoreHours'],axis = 1 ).set_index('JobIDRaw')
    
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








