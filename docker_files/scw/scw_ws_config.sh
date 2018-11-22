#!/bin/bash

# This script sets up the workspace and slurm configuration for the micro cluster simulation

echo "Starting scw cluster sim ws and slurm configuration...."

# initiating workspace for micro-cluster simulation
cd /home/slurm/slurm_sim_ws
mkdir -p /home/slurm/slurm_sim_ws/sim/scw

# creating slurm configuration properly
cd /home/slurm/slurm_sim_ws
/home/slurm/slurm_sim_ws/slurm_sim_tools/src/cp_slurm_conf_dir.py -o -s /home/slurm/slurm_sim_ws/slurm_opt /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/etc /home/slurm/slurm_sim_ws/sim/scw/baseline

echo "Finished with workspace setup and configuration"









