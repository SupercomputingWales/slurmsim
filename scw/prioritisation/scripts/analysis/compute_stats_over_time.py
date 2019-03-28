#!/usr/bin/env python3.6
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse as ap
import compute_metrics as cm
import textwrap


def step_trend(starts, ends, limits=None):
    assert starts.columns[0] == ends.columns[0]
    assert starts.columns[1] == ends.columns[1]

    index_quantity = starts.columns[0]
    quantity_to_plot = starts.columns[1]
    ends[quantity_to_plot] = -ends[quantity_to_plot]
    events = pd.concat([starts, ends])
    events = events.sort_values(by=index_quantity)
    events = events.groupby(index_quantity).sum()
    cumulative_sum = np.cumsum(events)
    cumulative_sum_name = 'S' + quantity_to_plot
    events[cumulative_sum_name] = cumulative_sum
    if limits is not None:
        low, high = limits
        if low is not None:
            events.loc[events[cumulative_sum_name] < low,
                       cumulative_sum_name] = low
        if high is not None:
            events.loc[events[cumulative_sum_name] > high,
                       cumulative_sum_name] = high

    return events[cumulative_sum_name]


def plots_dataframe(data_all,prioritized_accounts,pre_label,axes):
    
    datasets = [data_all]
    dataset_labels = ['All']

   
    if prioritized_accounts is not None:
        data_prioritized = data_all.loc[
            [acc in prioritized_accounts for acc in data_all.Account], :].copy()
        datasets.append(data_prioritized)
        dataset_labels.append('Prioritised')
    
    (ax1,ax2,ax3) = axes
    for data, post_label in zip(datasets, dataset_labels):
        # Plotting cluster usage
        starts = data.loc[:, ['Start', 'NCPUS']]
        ends = data.loc[:, ['End', 'NCPUS']]
    
        usage = (data.End - data.Start) / np.timedelta64(1, 's') * data.NCPUS
        label = ' '.join([pre_label,post_label]).strip()
        print(f'[{label}]: Total core seconds: {usage.sum()}')
    
        ends.columns = ['Time', 'NCPUS']
        starts.columns = ['Time', 'NCPUS']
        used = step_trend(starts, ends, (0, max_cpus))
    
        p = ax1.step(used.index, used / max_cpus * 100, label=label, where = 'post')[0]
    
        # Plotting queue status - requested core hours
    
        data['CoreSeconds'] = data.Timelimit.astype(
            'timedelta64[s]') * data.NCPUS
        data.Submit = pd.to_datetime(data.Submit)
    
        starts = data.loc[:, ['Submit',
                              'CoreSeconds']]  # job STARTS being in the queue
        ends = data.loc[:, ['Start',
                            'CoreSeconds']]  # job ENDS being in the queue
    
        starts.columns = ['Time', 'CoreSeconds']
        ends.columns = ['Time', 'CoreSeconds']
        CSIQ = step_trend(starts, ends, (None, None))
    
        ax2.step(CSIQ.index, CSIQ, label=label, color=p.get_color(), where = 'post')
    
        # Plotting queue status - number of jobs
    
        starts = data.loc[:, ['Submit']]  # job STARTS being in the queue
        ends = data.loc[:, ['Start']]  # job ENDS being in the queue
    
        starts.columns = ['Time']
        ends.columns = ['Time']
        starts['DeltaNJobs'] = np.ones(len(starts.Time))
        ends['DeltaNJobs'] = np.ones(len(ends.Time))
        NJobs = step_trend(starts, ends, (None, None))
    
        ax3.step(NJobs.index, NJobs, label=label, color=p.get_color(), where = 'post')

def figname(date_start,date_end):
    return "-".join([date_start.strftime("%d.%m.%y"),
        date_end.strftime("%d.%m.%y")])

def finalise_plots(date_start,date_end,axes,savefig):
    for i, label in enumerate(['Utilisation (%)','Queeue - Core hours','Queue - Job Number']):
        axes[i].set_xlim([ date_start, date_end ])
        axes[i].set_ylabel(label)
        axes[i].legend()
    plt.setp(plt.xticks()[1], rotation=45, ha='right')
    if savefig:
        filename = figname(date_start,date_end)
        filename = filename.replace('.','_')+'.eps'
        print(f"Writing {filename}")
        plt.savefig(filename)
    else:
        plt.show()

max_cpus = (40 * (136 + 26 + 13 + 26 + 3 + 1 + 1))  # HAWK
max_cpus = 5040  # SUNBIRD


if __name__ == "__main__":
    description = 'Plots usage and queue status of a SLURM cluster over time,\n\
            starting from the output of the sacct command showing NCPUS,Start\n\
            and End times, with the --parsable2 option.'
    
    parser = ap.ArgumentParser(description=description)
    
    parser.add_argument(
        "files_and_labels",
        nargs='+',
        help=textwrap.dedent('''\
            A list of files and labels in the format
               fileA labelA fileB labelB ...
               '''))
    parser.add_argument(
        "--start", help="Left limit of the plot (format DDMMYY).", required=True)
    parser.add_argument(
        "--end", help="Right limit of the plot (format DDMMYY).", required=True)
    parser.add_argument(
        "--max_cpus",
        help="Number of cpus in the cluster.",
        type=int,
        default=max_cpus)
    parser.add_argument(
        "--pa", type=str, help="A file containing a list of prioritized accounts.")
    
    args = parser.parse_args()
    
    files_and_labels = args.files_and_labels
    date_start = pd.to_datetime(args.start,format = '%d%m%y')
    date_end = pd.to_datetime(args.end,format = '%d%m%y')
    max_cpus = args.max_cpus
    prioritized_accounts = cm.get_palist(args.pa)

    fig,axes = plt.subplots(3,1,sharex=True,figsize = (6.4,14.4))


    for i, arg in enumerate(files_and_labels[0::2]):
        data_all = pd.read_csv(arg, sep='|')

        data_all = cm.fix_dataframe(data_all)  
        data_all = cm.crop_dataset(data_all,date_start,date_end)
        plots_dataframe(data_all,prioritized_accounts,files_and_labels[2*i+1],axes)

    finalise_plots(date_start,date_end,axes,False)
   

