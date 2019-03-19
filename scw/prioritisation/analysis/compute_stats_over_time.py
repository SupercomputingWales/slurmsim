#!/usr/bin/env python3.6
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import argparse as ap
import textwrap


def fix_timelimit(data):
    '''
    Fixes timelimit in a dataframe, in place.
    '''
    max_timelimit_string = "3d00:00:00"
    data.loc[data.Timelimit.str.match('Partition_Limit'
                                      ), 'Timelimit'] = max_timelimit_string
    data.Timelimit = data.Timelimit.str.replace('-', 'd')
    data.Timelimit = pd.to_timedelta(data.Timelimit)
    data.loc[data.Timelimit > pd.to_timedelta(max_timelimit_string),
             'Timelimit'] = pd.to_timedelta(max_timelimit_string)


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



max_cpus = (40 * (136 + 26 + 13 + 26 + 3 + 1 + 1))  # HAWK
max_cpus = 5040  # SUNBIRD

description = 'Plot usage of a SLURM cluster over time,starting from the \n\
output of the sacct command showing NCPUS,Start and End times, with the \n\
--parsable2 option.'

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
date_start = args.start
date_end = args.end
max_cpus = args.max_cpus

plt.figure(figsize=(8, 7.0))
for i, arg in enumerate(files_and_labels[0::2]):
    data_all = pd.read_csv(arg, sep='|')

    fix_timelimit(data_all)

    data_all.Start = pd.to_datetime(data_all.Start)
    data_all.End = pd.to_datetime(data_all.End)

    datasets = [data_all]
    dataset_labels = ['All']

    if args.pa is not None:
        with open(args.pa, 'r') as f:
            prioritized_accounts = [
                l.strip() for l in set(f.read().split()) if l != ''
            ]
        data_prioritized = data_all.loc[
            [acc in prioritized_accounts for acc in data_all.Account], :].copy()
        datasets.append(data_prioritized)
        dataset_labels.append('Prioritised')

    for data, post_label in zip(datasets, dataset_labels):
        # Plotting cluster usage
        starts = data.loc[:, ['Start', 'NCPUS']]
        ends = data.loc[:, ['End', 'NCPUS']]

        usage = (data.End - data.Start) / np.timedelta64(1, 's') * data.NCPUS
        label = files_and_labels[1 + i * 2] + ' ' + post_label
        print(f'[{label}]: Total core seconds: {usage.sum()}')

        ends.columns = ['Time', 'NCPUS']
        starts.columns = ['Time', 'NCPUS']
        used = step_trend(starts, ends, (0, max_cpus))

        plt.figure(1)
        p = plt.step(used.index, used / max_cpus * 100, label=label, where = 'post')[0]

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

        plt.figure(2)
        plt.step(CSIQ.index, CSIQ, label=label, color=p.get_color(), where = 'post')

        # Plotting queue status - number of jobs

        starts = data.loc[:, ['Submit']]  # job STARTS being in the queue
        ends = data.loc[:, ['Start']]  # job ENDS being in the queue

        starts.columns = ['Time']
        ends.columns = ['Time']
        starts['DeltaNJobs'] = np.ones(len(starts.Time))
        ends['DeltaNJobs'] = np.ones(len(ends.Time))
        NJobs = step_trend(starts, ends, (None, None))

        plt.figure(3)
        plt.step(NJobs.index, NJobs, label=label, color=p.get_color(), where = 'post')

for i, label in [(1, 'Utilisation (%)'), (2, 'Queeue - Core hours'),
                 (3, 'Queue - Job Number')]:
    plt.figure(i)
    plt.xlim([
        pd.to_datetime(date_start, format='%d%m%y'),
        pd.to_datetime(date_end, format='%d%m%y')
    ])
    plt.ylabel(label)
    plt.setp(plt.xticks()[1], rotation=45, ha='right')
    plt.legend()
plt.show()
