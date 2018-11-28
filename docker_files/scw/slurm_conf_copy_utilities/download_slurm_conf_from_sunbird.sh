#!/bin/bash

echo "This script will download data from sunbird"
USER_HOST=$1


ask_and_do(){

    echo "$1"
    echo "Execute Command? (yes || y)"
    read answer
    if [[ "$answer" == "yes" || "$answer" == "y" ]]
    then
        echo "$1" | bash
    else
        echo "Nothing done"
    fi

}



#COPY_DIR_DESTINATION=/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/
COPY_DIR_DESTINATION=.
# getting historical job data from sacct
ask_and_do "ssh $USER_HOST 'sacct --format JobIdRaw,Submit,Timelimit,End,Start,NTasks,NNodes,User,QOS,Partition,Account,ReqMem,ReqGRES,NCPUS,State --allocations --parsable2 --allusers --starttime=092018 --endtime=102018' > $COPY_DIR_DESTINATION/sacct_output.csv "

# getting user-account relationships 
ask_and_do "ssh $USER_HOST 'sacctmgr list associations --parsable2' > $COPY_DIR_DESTINATION/associations.csv"

# getting QoS information
ask_and_do "ssh $USER_HOST 'sacctmgr list QOS --parsable2' > $COPY_DIR_DESTINATION/QOS_data.csv"

