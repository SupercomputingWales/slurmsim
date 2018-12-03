#!/bin/bash


# this script sets up the scw cluster simulation and runs it, checking if it works properly

echo "Setting up SCW Cluster simulation...."

# creates and uses mysql database needed for the simulation
mysql -e "CREATE DATABASE slurm_scwsim;"
mysql -e "USE slurm_scwsim;"

# calls the setup file for the scw Cluster simulation (executes as slurm)
su slurm -c /install_files/scw_cluster_setup.py

echo "Done with SCW Cluster Setup"

echo "Starting simulation...."

# runs the simulation as the slurm user so the simulator doesn't get upset
su slurm -c /install_files/run_sim.sh

echo "Simulation Finished."

echo "Starting R check file....."

# this file runs some code that checks if features were given correctly
Rscript /install_files/check_results.R

cd /home/slurm # goes to the home directory of slurm

su slurm # switches to slurm user at the end (starts bash)







