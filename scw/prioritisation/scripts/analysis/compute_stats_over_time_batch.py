#!/usr/bin/env python3.6
import pandas as pd
import batch_lib as bl
import compute_stats_over_time as csot
import compute_metrics as cm
import argparse as ap
import os 
from matplotlib import pyplot as plt 

if __name__ == "__main__":
    description = 'Repeats the function of "compute_stats_over_time.py" over\n\
            many files, reading a "cycle_data" file that contains all the \n\
            settings for the runs and analysis.'

    parser = ap.ArgumentParser(description=description)
    
    parser.add_argument("cycle_data",type=str,
         help="The table file with all the settings for runs and analyses.")
    parser.add_argument(
        "--max_cpus",
        help="Number of cpus in the cluster.",
        type=int,
        default=csot.max_cpus)
    parser.add_argument(
        "--pa", type=str, help="A file containing a list of prioritized accounts.")

    parser.add_argument("--savefig",action = 'store_true',
            help="Save figures in .eps format instead of plotting.")
    
    parser.add_argument(
        "--data_prefix",
        type=str,
        help="Path prefix to where the directories containing the data are.",
        required=True)
    
    args = parser.parse_args()
    
    settings_table = bl.read_cycle_info_file(args.cycle_data)
    max_cpus = args.max_cpus
    prioritized_accounts = cm.get_palist(args.pa)
    savefig = args.savefig
    data_prefix = args.data_prefix

    already_done = set()

    for SimulationStart, SimulationEnd, PriorityWeightQOS, AnalysisStart, AnalysisEnd in zip(
            settings_table.SimulationStart,
            settings_table.SimulationEnd,
            settings_table.PriorityWeightQOS,
            settings_table.AnalysisStart,
            settings_table.AnalysisEnd):
 
        original_res_name = os.path.join(data_prefix,
                bl.dirname(SimulationStart, SimulationEnd, PriorityWeightQOS),
		bl.original_res_basename)

        AnalysisStart   = pd.to_datetime(AnalysisStart  ,format = '%d%m%y')
        AnalysisEnd     = pd.to_datetime(AnalysisEnd    ,format = '%d%m%y')

        if (AnalysisStart,AnalysisEnd) not in already_done:
            print(f"Reading {original_res_name}")
            data_all = pd.read_csv(original_res_name, sep='|')
            data_all = cm.fix_dataframe(data_all)
            data_all = cm.crop_dataset(data_all,AnalysisStart,AnalysisEnd)
    
            fig,axes = plt.subplots(3,1,sharex=True,figsize = (6.4,11.4))
            fig.suptitle(csot.figname(AnalysisStart,AnalysisEnd))
            csot.plots_dataframe(data_all,prioritized_accounts,'',axes)
            csot.finalise_plots(AnalysisStart,AnalysisEnd,axes,savefig)
            already_done.add((AnalysisStart,AnalysisEnd))


