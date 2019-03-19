#!/usr/bin/env python3.6
import pandas as pd
import numpy as np
from sys import argv
from matplotlib import pyplot as plt
import compute_metrics_batch as cmb

nnodes=126
data = pd.read_pickle(argv[1])


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
for ob in qos_varying_columns+quartiles_columns:
    plt.yscale('log')
    data.groupby(metrics_df_index_names).apply(lambda df: plt.plot(
        df.QOSPriority,
        df[ob] / np.timedelta64(1, 'h'),
        label=df.reset_index()[metrics_df_index_names[0]][0].strftime("%d/%m/%y") + 
              '-' + 
              df.reset_index()[metrics_df_index_names[1]][0].strftime("%d/%m/%y")))

    plt.title("Effects of PriorityWeightQOS")
    plt.xlabel('PriorityWeightQOS')
    plt.ylabel(cmb.cm.short_to_long[ob] + ' [h]')
    plt.legend()
    plt.show()

relative_maxima = data.loc[:,metrics_df_index_names+ 
        qos_varying_columns_rel ].groupby(
                metrics_df_index_names).max(axis='index')
relative_minima = data.loc[:,metrics_df_index_names+
        qos_varying_columns_rel ].groupby(
                metrics_df_index_names).min(axis='index')
usage_avg = data.loc[:,metrics_df_index_names + ['Usage']].groupby(
        metrics_df_index_names).apply(lambda df: np.mean(df.Usage))   # DIOCANE
prvsall_avg = data.loc[:,metrics_df_index_names+['PrVsAll']].groupby(
        metrics_df_index_names).apply(lambda df: np.mean(df.PrVsAll)) # DIOCANE


relative_maxima['Usage'] = usage_avg*nnodes
relative_minima['Usage'] = usage_avg*nnodes

relative_maxima = relative_maxima.sort_values(by='Usage')
relative_minima = relative_minima.sort_values(by='Usage')

# plot relative maxima/minima as a function of usage
for col in qos_varying_columns_rel:
    plt.yscale('linear')
    plt.fill_between(relative_maxima.Usage,
            relative_minima[col],
            relative_maxima[col])

    plt.title("Max/Min Effects of PriorityWeightQOS as a function of cluster usage")
    plt.xlabel(f'Mean Usage Estimate ({nnodes}=full cluster)')
    plt.ylabel(cmb.cm.short_to_long[rel_to_abs[col]] + ' (relative)' )
    plt.show()


plt.title("Simulator Runs - configuration space sampling")
plt.plot(usage_avg*nnodes,prvsall_avg,marker = '+',linestyle = 'None')
plt.xlabel(f'Mean Usage Estimate ({nnodes}=full cluster)')
plt.ylabel('Usage by prioritised users/usage by all users')
plt.show()
