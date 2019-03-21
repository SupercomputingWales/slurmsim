all:  ./01_01_19-11_01_19.eps ./01_11_18-11_11_18.eps ./01_12_18-11_12_18.eps ./11_01_19-21_01_19.eps ./11_11_18-21_11_18.eps\
 ./11_12_18-21_12_18.eps ./19_01_19-29_01_19.eps ./21_10_18-01_11_18.eps ./21_11_18-01_12_18.eps ./21_12_18-01_01_19.eps ./Avgqtall.eps ./Avgqtlt.eps\
 ./Avgqtno-pr.eps ./Avgqtpr.eps ./Maxqtall.eps ./Maxqtlt.eps ./Maxqtno-pr.eps ./Maxqtpr.eps ./all_metrics.csv ./all_metrics.pickle\
 ./minmax_AvgqtallRel_rel.eps ./minmax_AvgqtltRel_rel.eps ./minmax_Avgqtno-prRel_rel.eps ./minmax_AvgqtprRel_rel.eps ./minmax_MaxqtallRel_rel.eps\
 ./minmax_MaxqtltRel_rel.eps ./minmax_Maxqtno-prRel_rel.eps ./minmax_MaxqtprRel_rel.eps ./q25qtall.eps ./q25qtlt.eps ./q25qtno-pr.eps ./q25qtpr.eps\
 ./q75qtall.eps ./q75qtlt.eps ./q75qtno-pr.eps ./q75qtpr.eps ./usageVsPrioOverAll.eps

./01_01_19-11_01_19.eps ./01_11_18-11_11_18.eps ./01_12_18-11_12_18.eps ./11_01_19-21_01_19.eps ./11_11_18-21_11_18.eps \
./11_12_18-21_12_18.eps ./19_01_19-29_01_19.eps ./21_10_18-01_11_18.eps ./21_11_18-01_12_18.eps ./21_12_18-01_01_19.eps  : \
./../data/slurm_sim_data5/011018_011118_0/sacct_output.csv ./../data/slurm_sim_data5/011118_011218_0/sacct_output.csv ./../data/slurm_sim_data5/011218_010119_0/sacct_output.csv \
./../data/slurm_sim_data5/111018_111118_0/sacct_output.csv ./../data/slurm_sim_data5/111118_111218_0/sacct_output.csv ./../data/slurm_sim_data5/111218_110119_0/sacct_output.csv \
./../data/slurm_sim_data5/211018_211118_0/sacct_output.csv ./../data/slurm_sim_data5/211118_211218_0/sacct_output.csv ./../data/slurm_sim_data5/211218_210119_0/sacct_output.csv \
./../data/slurm_sim_data5/291218_290119_0/sacct_output.csv 
	cd  . &&  log ../scripts/analysis/compute_stats_over_time_batch.py cycle_data_analysis.set --savefig --pa ../data/slurm_sim_data5/package/prioritised_accounts.txt --data_prefix ../data/slurm_sim_data5

./all_metrics.csv ./all_metrics.pickle  : ./../data/slurm_sim_data5/011018_011118_0/jobcomp.log \
./../data/slurm_sim_data5/011018_011118_100/jobcomp.log ./../data/slurm_sim_data5/011018_011118_1000/jobcomp.log ./../data/slurm_sim_data5/011018_011118_300/jobcomp.log \
./../data/slurm_sim_data5/011018_011118_3000/jobcomp.log ./../data/slurm_sim_data5/011118_011218_0/jobcomp.log ./../data/slurm_sim_data5/011118_011218_100/jobcomp.log \
./../data/slurm_sim_data5/011118_011218_1000/jobcomp.log ./../data/slurm_sim_data5/011118_011218_300/jobcomp.log ./../data/slurm_sim_data5/011118_011218_3000/jobcomp.log \
./../data/slurm_sim_data5/011218_010119_0/jobcomp.log ./../data/slurm_sim_data5/011218_010119_100/jobcomp.log ./../data/slurm_sim_data5/011218_010119_1000/jobcomp.log \
./../data/slurm_sim_data5/011218_010119_300/jobcomp.log ./../data/slurm_sim_data5/011218_010119_3000/jobcomp.log ./../data/slurm_sim_data5/111018_111118_0/jobcomp.log \
./../data/slurm_sim_data5/111018_111118_100/jobcomp.log ./../data/slurm_sim_data5/111018_111118_1000/jobcomp.log ./../data/slurm_sim_data5/111018_111118_300/jobcomp.log \
./../data/slurm_sim_data5/111018_111118_3000/jobcomp.log ./../data/slurm_sim_data5/111118_111218_0/jobcomp.log ./../data/slurm_sim_data5/111118_111218_100/jobcomp.log \
./../data/slurm_sim_data5/111118_111218_1000/jobcomp.log ./../data/slurm_sim_data5/111118_111218_300/jobcomp.log ./../data/slurm_sim_data5/111118_111218_3000/jobcomp.log \
./../data/slurm_sim_data5/111218_110119_0/jobcomp.log ./../data/slurm_sim_data5/111218_110119_100/jobcomp.log ./../data/slurm_sim_data5/111218_110119_1000/jobcomp.log \
./../data/slurm_sim_data5/111218_110119_300/jobcomp.log ./../data/slurm_sim_data5/111218_110119_3000/jobcomp.log ./../data/slurm_sim_data5/211018_211118_0/jobcomp.log \
./../data/slurm_sim_data5/211018_211118_100/jobcomp.log ./../data/slurm_sim_data5/211018_211118_1000/jobcomp.log ./../data/slurm_sim_data5/211018_211118_300/jobcomp.log \
./../data/slurm_sim_data5/211018_211118_3000/jobcomp.log ./../data/slurm_sim_data5/211118_211218_0/jobcomp.log ./../data/slurm_sim_data5/211118_211218_100/jobcomp.log \
./../data/slurm_sim_data5/211118_211218_1000/jobcomp.log ./../data/slurm_sim_data5/211118_211218_300/jobcomp.log ./../data/slurm_sim_data5/211118_211218_3000/jobcomp.log \
./../data/slurm_sim_data5/211218_210119_0/jobcomp.log ./../data/slurm_sim_data5/211218_210119_100/jobcomp.log ./../data/slurm_sim_data5/211218_210119_1000/jobcomp.log \
./../data/slurm_sim_data5/211218_210119_300/jobcomp.log ./../data/slurm_sim_data5/211218_210119_3000/jobcomp.log ./../data/slurm_sim_data5/291218_290119_0/jobcomp.log \
./../data/slurm_sim_data5/291218_290119_100/jobcomp.log ./../data/slurm_sim_data5/291218_290119_1000/jobcomp.log ./../data/slurm_sim_data5/291218_290119_300/jobcomp.log \
./../data/slurm_sim_data5/291218_290119_3000/jobcomp.log ./cycle_data_analysis.set 
	cd  . &&  log ../scripts/analysis/compute_metrics_batch.py cycle_data_analysis.set --data_prefix ../data/slurm_sim_data5/ --pa ../data/slurm_sim_data5/package/prioritised_accounts.txt

./Avgqtall.eps ./Avgqtlt.eps ./Avgqtno-pr.eps ./Avgqtpr.eps ./Maxqtall.eps ./Maxqtlt.eps ./Maxqtno-pr.eps ./Maxqtpr.eps \
./minmax_AvgqtallRel_rel.eps ./minmax_AvgqtltRel_rel.eps ./minmax_Avgqtno-prRel_rel.eps ./minmax_AvgqtprRel_rel.eps ./minmax_MaxqtallRel_rel.eps \
./minmax_MaxqtltRel_rel.eps ./minmax_Maxqtno-prRel_rel.eps ./minmax_MaxqtprRel_rel.eps ./q25qtall.eps ./q25qtlt.eps ./q25qtno-pr.eps ./q25qtpr.eps \
./q75qtall.eps ./q75qtlt.eps ./q75qtno-pr.eps ./q75qtpr.eps ./usageVsPrioOverAll.eps  : ./all_metrics.pickle 
	cd  . &&  log ../scripts/analysis/plot_campaign_data.py --savefig all_metrics.pickle

