#!/bin/bash

# This script sets up and runs the generation of the job trace files using Rscript

# goes to the directory where we want to edit things in (working directory)
cd /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/

# begins the R script that generates a bunch of test jobs for the simulator
Rscript parse_slurm_output_and_write_trace.R































