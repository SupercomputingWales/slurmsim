#!/usr/bin/env python3.6
import pandas as pd
from io import StringIO

def clear_text(filename):
    def clear_line(line):
        ''' Removes comments from line. Output lines with endline character appended.'''
        line_start = line.strip('\n').split('#')[0]
        return (line_start + '\n') 
        
    with open(filename,'r') as f:
        lines = f.readlines()
        cleared_lines = [ clear_line(line) for line in lines ]
        good_lines = [ ','.join(clear_line(line).split()) for line in cleared_lines if len(line) != 1 ]
        print(f"[DEBUG] No of good lines: {len(good_lines)}")
        header = ','.join(['SimulationStart','SimulationEnd',
        'PriorityWeightQOS','AnalysisStart','AnalysisEnd'])
        text = '\n'.join([header]+good_lines)
        while '\n\n' in text:
            text = text.replace('\n\n','\n')
        
    return text

def read_cycle_info_file(filename):
    settings_table = pd.read_csv(StringIO(clear_text(filename)),dtype=str)
  
    return settings_table

def dirname(SimulationStart,SimulationEnd,PriorityWeightQOS):
    return f"{SimulationStart}_{SimulationEnd}_{PriorityWeightQOS}"
    
simres_basename = 'jobcomp.log'
original_res_basename = 'sacct_output.csv'


