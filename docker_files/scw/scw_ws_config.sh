#!/bin/bash

# This script sets up the workspace and slurm configuration for the scw cluster simulation

source /install_files/variables.sh
echo "Starting scw cluster sim ws and slurm configuration...."

# initiating workspace for scw-cluster simulation
cd /home/slurm/slurm_sim_ws
mkdir -p $SLURM_SIM/scw

# creating slurm configuration properly
cd /home/slurm/slurm_sim_ws
$SLURM_SIM_TOOLS/src/cp_slurm_conf_dir.py -o -s $SLURM_OPT $SLURM_SIM_TOOLS/scw/etc $SLURM_SIM/scw/baseline

echo "Finished with workspace setup and configuration"









