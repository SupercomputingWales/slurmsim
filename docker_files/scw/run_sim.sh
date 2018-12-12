#!/bin/bash

# This script ensures that the simulation is run from the correct directory
source /install_files/variables.env

# goes to baseline directory
cd $SLURM_SIM/scw/baseline


# runs the simulation
$SLURM_SIM_TOOLS/src/run_sim.py -e $SLURM_SIM/scw/baseline/etc -s $SLURM_OPT -d











