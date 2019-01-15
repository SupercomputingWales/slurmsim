#!/usr/bin/env Rscript

# This script gets the results from the simulation and runs some tests on them 
# It tests if the requested features were given to the jobs
# Features: cpu type, gpu, big mem
# How implemented - each feature corresponds to a different type of node

library(dplyr)
library(tidyverse)
library(lubridate)
library(stringr)



sacct_output_simulated <- "/home/slurm/slurm_sim_ws/sim/scw/baseline/results/jobcomp.log"
sacct_output_live<- "/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/sacct_output.csv"

# reads in log file of resulting data (what jobs were assigned, where, etc)
simulated <- read.csv2(sacct_output_simulated,sep = '|')
live <- read.csv2(sacct_output_live, sep='|')





# creating a joined data frame by job id so that can go through jobs easier
joined <- left_join(simulated, live, by = c("JobIDRaw" = "JobIDRaw"),
		    suffix = c(".sim",".live") )

start.sim  <- joined$Start.sim %>% ymd_hms
start.live  <- joined$Start.live %>% ymd_hms

diff <- start.sim - start.live
ids <- joined$JobIDRaw


towrite <- cbind(ids,start.live,diff)

colnames(towrite) <- c('JobIDRaw','Start(live)','Difference')

write.csv(towrite,"differences.dat",sep="\t")


