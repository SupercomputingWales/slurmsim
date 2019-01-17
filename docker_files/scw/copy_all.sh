#!/bin/bash
ORIGIN_DIR=/home/slurm/slurm_sim_ws/slurm_sim_tools/docker_files/scw
TARGET_DIR=/install_files

for file in \
check_results.R \
generate_job_trace.sh \
initial_test.sh \
populate_slurmdb.sh \
run_sim.sh \
sacctmgr_output_to_sacctmgr_commands_QOS.py \
scw_cluster_setup.py \
scw_sim_variables.py \
scw_ws_config.sh \
slurm_conf_setup.sh \
startup_file.sh \
run_all.sh \
variables.env 
do
  cp $ORIGIN_DIR/$file $TARGET_DIR
  chmod +x $TARGET_DIR/$file
done
