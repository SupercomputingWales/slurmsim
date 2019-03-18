#!/usr/bin/env python3.6
import compute_metrics as cm
import pandas as pd
import argparse as ap
import numpy as np
import re
import os
from io import StringIO


def clear_text(filename):
    def clear_line(line):
        ''' Removes comments from line. Output lines with endline character appended.'''
        line_start = line.strip('\n').split('#')[0]
        return (line_start + '\n') 
        
    with open(filename,'r') as f:
        lines = f.readlines()
        cleared_lines = [ clear_line(line) for line in lines ]
        text = '\n'.join([ ','.join(clear_line(line).split()) for line in cleared_lines if len(line) != 1 ])
        while '\n\n' in text:
            text = text.replace('\n\n','\n')
        
    return text

def read_cycle_info_file(filename):
    settings_table = pd.read_csv(StringIO(clear_text(filename)),dtype=str)
    settings_table.columns = ['SimulationStart','SimulationEnd',
        'PriorityWeightQOS','AnalysisStart','AnalysisEnd']
   
    return settings_table

def dirname(SimulationStart,SimulationEnd,PriorityWeightQOS):
    return f"{SimulationStart}_{SimulationEnd}_{PriorityWeightQOS}"
    
simres_basename = 'jobcomp.log'
original_res_basename = 'sacct_output.csv'


def analyse_all_from_table(settings_table, prioritized_accounts):
 
    metrics = dict()
    for SimulationStart, SimulationEnd, PriorityWeightQOS, AnalysisStart, AnalysisEnd in zip(
            settings_table.SimulationStart,
            settings_table.SimulationEnd,
            settings_table.PriorityWeightQOS,
            settings_table.AnalysisStart,
            settings_table.AnalysisEnd):
        
         simres_name = os.path.join(dirname(SimulationStart, SimulationEnd, PriorityWeightQOS),
		simres_basename)

         simulated_data = pd.read_csv(simres_name,sep='|')
         print(f"[DEBUG] Nuber of job in {simres_name}: {len(simulated_data)}")
    
         cm.fix_dataframe(simulated_data)

         SimulationStart = pd.to_datetime(SimulationStart,format = '%d%m%y')
         SimulationEnd   = pd.to_datetime(SimulationEnd  ,format = '%d%m%y')
         PriorityWeightQOS = int(PriorityWeightQOS)
         AnalysisStart   = pd.to_datetime(AnalysisStart  ,format = '%d%m%y')
         AnalysisEnd     = pd.to_datetime(AnalysisEnd    ,format = '%d%m%y')
 
         metrics[(SimulationStart, SimulationEnd, PriorityWeightQOS)] = \
             cm.calc_metrics(simulated_data,AnalysisStart,AnalysisEnd,prioritized_accounts)

    return metrics

def make_metrics_df(metrics):

    met_df = pd.DataFrame(columns= ['RunStart','RunEnd','QOSPriority'] + 
         list(list(metrics.values())[0].keys())).set_index(['RunStart','RunEnd','QOSPriority'])

    for k,v in metrics.items():
        for k2,v2 in v.items():
            met_df.loc[k,k2] = v2

    return met_df



if __name__ == "__main__":
    parser = ap.ArgumentParser(description="Compute metrics on a bunch of files.")

    parser.add_argument("cycle_data",type=str,
         help="The table file with all the settings for runs and analyses.")
    parser.add_argument("--pa",type=str,
        help="A file containing a list of prioritized accounts.")

    args = parser.parse_args()
   
    settings_table = read_cycle_info_file(args.cycle_data)

    prioritized_accounts = cm.get_palist(args.pa)
   
    metrics = cmb.analyse_all_from_table(settings_table,prioritized_accounts)

    met_df = cmb.make_metrics_df(metrics)
   




