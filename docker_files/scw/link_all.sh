#!/bin/bash
ORIGIN_DIR=/home/slurm/slurm_sim_ws/slurm_sim_tools/docker_files/scw
TARGET_DIR=/install_files

ln -s $ORIGIN_DIR/package_install.R $TARGET_DIR/
ln -s $ORIGIN_DIR/startup_file.sh $TARGET_DIR/
ln -s $ORIGIN_DIR/initial_test.sh $TARGET_DIR/
ln -s $ORIGIN_DIR/scw_cluster_setup.py $TARGET_DIR/
ln -s $ORIGIN_DIR/scw_ws_config.sh $TARGET_DIR/
ln -s $ORIGIN_DIR/populate_slurmdb.sh $TARGET_DIR/
ln -s $ORIGIN_DIR/generate_job_trace.sh $TARGET_DIR/
ln -s $ORIGIN_DIR/run_sim.sh $TARGET_DIR/
ln -s $ORIGIN_DIR/check_results.R $TARGET_DIR/
ln -s $ORIGIN_DIR/download_slurm_conf_from_sunbird.sh $TARGET_DIR/
ln -s $ORIGIN_DIR/sacctmgr_output_to_sacctmgr_commands_QOS.py $TARGET_DIR/
ln -s $ORIGIN_DIR/scw_sim_variables.py $TARGET_DIR/
ln -s $ORIGIN_DIR/variables.sh $TARGET_DIR/
