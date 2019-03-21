#!/usr/bin/env python3.6
import pandas as pd
import numpy as np
from sys import argv
from matplotlib import pyplot as plt
import compute_metrics_batch as cmb
import argparse as ap
from itertools import cycle



parser = ap.ArgumentParser(description="A script to plot the data obtained \n\
        from the analysis of a number of runs of the slurm simulator")

parser.add_argument('pickled_data',type=str,
        help="A pickle dump of the pandas dataframe containing all the metrics for all the runs")

parser.add_argument("--savefig",action = 'store_true',
        help="Save figures in .eps format instead of plotting.")

parser.add_argument("--nnodes",type=int,
        help="The number of nodes in the cluster - see corresponding parameters \n\
        in the other scripts.",default=126) # sunbird

args = parser.parse_args()

pickled_data_filename = args.pickled_data
savefig = args.savefig
nnodes = args.nnodes

print(f"Reading {pickled_data_filename}")
data = pd.read_pickle(pickled_data_filename)

#qos_varying_columns = ['Avgqtall', 'Avgqtpr', 'Avgqtno-pr', 'Avgqtlt', 
#        'Maxqtall', 'Maxqtpr', 'Maxqtno-pr', 'Maxqtlt']
qos_varying_columns = [ col for col in data.columns if 'Avg' in col or 'Max' in col ]

#quartiles_columns = [ 'q25qtall', 'q25qtpr', 'q25qtno-pr', 'q25qtlt', 
#        'q75qtall', 'q75qtpr', 'q75qtno-pr', 'q75qtlt']
quartiles_columns = [ col for col in data.columns if 'q25' in col or 'q75' in col ]


metrics_df_index_names = cmb.metrics_df_index_names.copy()
metrics_df_index_names.remove('QOSPriority')

qos_varying_columns_rel = [col+'Rel' for col in qos_varying_columns]

rel_to_abs = dict(zip(qos_varying_columns_rel,qos_varying_columns))

for col in qos_varying_columns:
    for idx in data.index :
        start,end,qos = idx
        denom = data.loc[(start,end,0),col]
        num = data.loc[idx,col]
        data.loc[idx,col+'Rel'] = num/denom

data['Usage'] = data.TCHall / data.TotCHAvail
data['PrVsAll'] = data.TCHpr / data.TCHall

data = data.reset_index()

usage_avg = data.loc[:,metrics_df_index_names + ['Usage']].groupby(
        metrics_df_index_names).apply(lambda df: np.mean(df.Usage))   # DIOCANE
prvsall_avg = data.loc[:,metrics_df_index_names+['PrVsAll']].groupby(
        metrics_df_index_names).apply(lambda df: np.mean(df.PrVsAll)) # DIOCANE

linestyles = ['-', '--', '-.', ':']
markerstyles = ['.','o','v','^','<','>']

def red_level(usage_level,prvsall_level):
    return 0.9*(usage_level - usage_avg.min())/(usage_avg.max()-usage_avg.min())

def green_level(usage_level,prvsall_level):
    return 0.1

def blue_level(usage_level,prvsall_level):
    return -0.9*(usage_level - usage_avg.max())/(usage_avg.max()-usage_avg.min())

colorfuns = [red_level,green_level,blue_level]

def linestyle(usage_level,prvsall_level):
    idx =  (usage_level - usage_avg.min())/(usage_avg.max()-usage_avg.min())
    idx = int(idx * len(linestyles))
    if idx == len(linestyles):
        return linestyles[-1]
    else:
        return linestyles[idx]

def markerstyle(usage_level,prvsall_level):
    idx =  (prvsall_level - prvsall_avg.min())/(prvsall_avg.max()-prvsall_avg.min())
    idx = int(idx * len(markerstyles))
    if idx == len(markerstyles):
        return markerstyles[-1]
    else:
        return markerstyles[idx]


def pldf(df,metrics_df_index_names,ob):
    dfr = df.reset_index()
    name0 = metrics_df_index_names[0]
    name1 = metrics_df_index_names[1]

    label = dfr[name0][0].strftime("%d/%m/%y") + '-' + dfr[name1][0].strftime("%d/%m/%y")
    usage_level = np.mean(df.Usage)
    prvsall_level = np.mean(df.PrVsAll)
    color = [ cf(usage_level,prvsall_level) for cf in colorfuns ]
    ls = linestyle(usage_level,prvsall_level)
    ms = markerstyle(usage_level,prvsall_level)

    plt.plot(df.QOSPriority, df[ob] / np.timedelta64(1, 'h'), label=label,
            color = color, linestyle = ls, marker = ms)
      
for ob in qos_varying_columns+quartiles_columns:
    plt.figure()
    plt.yscale('log')
    data.groupby(metrics_df_index_names).apply(lambda df: pldf(df,metrics_df_index_names,ob))

    plt.title("Effects of PriorityWeightQOS")
    plt.xlabel('PriorityWeightQOS')
    plt.ylabel(cmb.cm.short_to_long[ob] + ' [h]')
    plt.legend()
    if savefig:
        filename = ob+'.eps'
        print(f"Writing {filename}")
        plt.savefig(filename)
        print(ob,cmb.cm.short_to_long[ob])
    else:
        plt.show()

relative_maxima = data.loc[:,metrics_df_index_names+ 
        qos_varying_columns_rel ].groupby(
                metrics_df_index_names).max(axis='index')
relative_minima = data.loc[:,metrics_df_index_names+
        qos_varying_columns_rel ].groupby(
                metrics_df_index_names).min(axis='index')

relative_maxima['Usage'] = usage_avg*nnodes
relative_minima['Usage'] = usage_avg*nnodes

relative_maxima = relative_maxima.sort_values(by='Usage')
relative_minima = relative_minima.sort_values(by='Usage')

# plot relative maxima/minima as a function of usage
for col in qos_varying_columns_rel:
    plt.figure()
    plt.yscale('linear')
    plt.fill_between(relative_maxima.Usage,
            relative_minima[col],
            relative_maxima[col])

    plt.title("Max/Min Effects of PriorityWeightQOS as a function of cluster usage")
    plt.xlabel(f'Mean Usage Estimate ({nnodes}=full cluster)')
    plt.ylabel(cmb.cm.short_to_long[rel_to_abs[col]] + ' (relative)' )
    if savefig:
        filename = 'minmax_'+col+'_rel.eps'
        print(f"Writing {filename}")
        plt.savefig(filename)
    else:
        plt.show()


plt.figure()
plt.title("Simulator Runs - configuration space sampling")

def pldf(df,metrics_df_index_names):
    dfr = df.reset_index()
    name0 = metrics_df_index_names[0]
    name1 = metrics_df_index_names[1]

    label = dfr[name0][0].strftime("%d/%m/%y") + '-' + dfr[name1][0].strftime("%d/%m/%y")
    usage_level = np.mean(df.Usage)
    prvsall_level = np.mean(df.PrVsAll)
    color = [ cf(usage_level,prvsall_level) for cf in colorfuns ]
    ls = linestyle(usage_level,prvsall_level)
    ms = markerstyle(usage_level,prvsall_level)
    plt.plot(usage_level,prvsall_level,marker = ms,color=color,label=label)


data.groupby(metrics_df_index_names).apply(lambda df: pldf(df,metrics_df_index_names))

plt.xlabel(f'Mean Usage Estimate ({nnodes}=full cluster)')
plt.ylabel('Usage by prioritised users/usage by all users')
plt.legend()

if savefig:
    filename = 'usageVsPrioOverAll.eps'
    print(f"Writing {filename}")
    plt.savefig(filename)
else:
    plt.show()



