# load RSlurmSimTools
library(RSlurmSimTools)
library(tidyverse)
library(lubridate)

# change working directory to script location
#top_dir <- dirname(rstudioapi::getActiveDocumentContext()$path)
#setwd(top_dir) # DEBUG

#library(scales)
#library(lubridate)
#library(stringr)

#sacct_output <- read.csv2(file.path(top_dir,'sacct_output.csv'), sep = '|') # DEBUG
sacct_output <- read.csv2(file.path('.','sacct_output.csv'), sep = '|') # DEBUG


# Checking that the file read contains the following data

# The command used to retrieve the data was this:
# sacct --format JobIdRaw,Submit,Timelimit,End,Start,NTasks,NNodes,User,QOS,Partition,Account,ReqMem,ReqGRES,NCPUS,State --allocations --parsable2 --allusers

expected_colnames <- c( "JobIDRaw","Submit","Timelimit","End","Start",
                       "NTasks","NNodes","User","QOS","Partition","Account",
                       "ReqMem","ReqGRES","NCPUS","State")

((colnames(sacct_output) %>% sort) == (expected_colnames %>% sort)) %>% 
    all -> condition
if(!condition) stop("Column names in sacct_output.csv do not meet expectations")

# We need now to transform the data in sacc_output in this new data frame format:
# sim_job_id         : int  1001 1002 1003 1004 1005 1006 1007 1008 1009 1010 ...
# sim_submit         : POSIXct, format: "2017-01-01 00:02:15" "2017-01-01 00:04:08" ...
# sim_wclimit        : int  30 28 10 22 22 6 9 5 10 17 ...
# sim_duration       : int  1201 893 0 0 1258 247 271 33 91 719 ...
# sim_tasks          : int  6 12 1 24 1 1 12 24 12 2 ...
# sim_tasks_per_node : int  6 12 1 12 1 1 12 12 12 2 ...
# sim_username       : chr  "user5" "user5" "user3" "user3" ...
# sim_submit_ts      : int  1483228935 1483229048 1483229670 ...
# sim_qosname        : Factor w/ 1 level "normal": 1 1 1 1 1 1 1 1 1 1 ...
# sim_partition      : Factor w/ 1 level "normal": 1 1 1 1 1 1 1 1 1 1 ...
# sim_account        : chr  "account2" "account2" "account1" "account1" ...
# sim_req_mem        : int  NA NA NA NA NA NA NA NA NA NA ...
# sim_req_mem_per_cpu: int  0 0 0 0 0 0 0 0 0 0 ...
# sim_features       : Factor w/ 3 levels "CPU-N","","CPU-M": 1 2 2 1 2 ...
# sim_gres           : Factor w/ 3 levels "","gpu:1","gpu:2": 1 1 1 1 3 ...
# sim_shared         : int  1 1 1 1 1 1 1 1 1 1 ...
# sim_cpus_per_task  : int  1 1 1 1 1 1 1 1 1 1 ...
# sim_dependency     : Factor w/ 1 level "": 1 1 1 1 1 1 1 1 1 1 ...
# sim_cancelled_ts   : int  0 0 0 0 0 0 0 0 0 0 ...
# freq               : num  0.4 0.4 0.8 0.4 0.6 ...

# Relations between what we have and what we need to have:
#"sim_job_id"           = JobIdRaw                                                   
#"sim_submit"           = Submit                                                     
#"sim_wclimit"          = Timelimit                                                  
#"sim_duration"         = // End-Start                                               
#"sim_tasks"            = NTasks                                                     
#"sim_tasks_per_node"   = ?? // int(NTasks/NNodes) ?                                 
#"sim_username"         = User                                                       
#"sim_submit_ts"        = //seems the same as submit but as UNIX time minus 18000sec (it's R conversion from date to integer?)
#"sim_qosname"          = QOS                                                        
#"sim_partition"        = Partition                                                  
#"sim_account"          = Account                                                    
#"sim_req_mem"          = ReqMem                                                     
#"sim_req_mem_per_cpu"  = // can be set to zero, looking at the examples             
#"sim_features"         = ?? // constraints set with -C or --constraints in sbatch // no info in sacct?
#"sim_gres"             = ReqGRES                                                    
#"sim_shared"           = ?? // there is no info in sacct?                           
#"sim_cpus_per_task"    = ?? // likely NCPUS/NTasks                                  
#"sim_dependency"       = ?? // there is no info in sacct                            
#"sim_cancelled_ts"     = ?? // From State and the End field                         
##"sim_freq"            = No meaning for slurm, only a check for examples in tutorials
#


(sacct_output$End == 'Unknown') %>% length -> n_end_unknown
if(n_end_unknown != 0 ){
    warning("There are unfinished jobs in the list. Ignoring them.")
}
sacct_output <- sacct_output[ (sacct_output$End != 'Unknown'), ]


sim_job_id <-  sacct_output$JobIdRaw                   # 
sim_submit <-  sacct_output$Submit %>% ymd_hms         #   

get_min_duration <- function(x){
    if(grepl('-',x)){
        days_and_rest <- strsplit(x,'-')
        days <- days_and_rest[[1]][[1]]
        rest <- days_and_rest[[1]][[2]]
        as.integer(days)*1440+
            as.integer(ceiling(as.numeric(hms(rest))/as.numeric(dminutes(1))))
    }else{
        as.integer(ceiling(as.numeric(hms(x))/as.numeric(dminutes(1))))
    }
}
sim_wclimit <- sapply(as.character(sacct_output$Timelimit),
                      get_min_duration) %>% as.integer   #

sim_duration <- 
    (( sacct_output$End %>% ymd_hms) - ( sacct_output$Start %>% ymd_hms)) %>%
    as.integer(., units='secs')  #

sim_tasks <- as.integer(sacct_output$NTasks)

sim_tasks_per_node <- as.integer(sim_tasks/as.integer(sacct_output$NNodes))
sim_username <- as.character(sacct_output$User)
sim_submit_ts <- sacct_output$Submit %>% ymd_hms %>% as.integer(.,unit='secs')
sim_qosname <- sacct_output$QOS
sim_partition <- sacct_output$Partition
sim_account <- as.character(sacct_output$Account)

## WIP
## TODO


#
##write job trace for Slurm Simulator
#write_trace(file.path('.',"test.trace"),trace)   # DEBUG
#
##write job trace as csv for reture reference
#write.csv(trace,"test_trace.csv")
