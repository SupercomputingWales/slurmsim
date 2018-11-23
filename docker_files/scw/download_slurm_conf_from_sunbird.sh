#!/bin/bash

echo "This script will download data from sunbird"
USER_HOST=$1

# getting historical job data from sacct
echo "ssh $USER_HOST 'sacct --format JobIdRaw,Submit,Timelimit,End,Start,NTasks,NNodes,User,QOS,Partition,Account,ReqMem,ReqGRES,NCPUS,State --allocations --parsable2 --allusers --starttime=092018 --endtime=102018' > /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/sacct_output.csv "
echo "Execute command? Only 'yes' or 'y ' are valid affirmative answers."
read answer
if [[ "$answer" == "yes" || "$answer" == "y" ]]
then
    ssh $USER_HOST 'sacct --format JobIdRaw,Submit,Timelimit,End,Start,NTasks,NNodes,User,QOS,Partition,Account,ReqMem,ReqGRES,NCPUS,State --allocations --parsable2 --allusers --starttime=092018 --endtime=102018' > /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/sacct_output.csv 
else
    echo "Nothing done"
fi


# getting user-account relationships 
echo "ssh $USER_HOST 'sacctmgr list user --parsable2' > /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/users_accounts.csv"
echo "Execute command? Only 'yes' or 'y ' are valid affirmative answers."
read answer
if [[ "$answer" == "yes" || "$answer" == "y" ]]
then
    ssh $USER_HOST 'sacctmgr list user --parsable2' > /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/users_accounts.csv
else
    echo "Nothing done"
fi

# getting QoS information
echo "ssh $USER_HOST 'sacctmgr list QOS --parsable2' > /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/QOS_data.csv"
echo "Execute command? Only 'yes' or 'y ' are valid affirmative answers."
if [[ "$answer" == "yes" || "$answer" == "y" ]]
then
ssh $USER_HOST 'sacctmgr list QOS --parsable2' > /home/slurm/slurm_sim_ws/slurm_sim_tools/scw/QOS_data.csv 
else
    echo "Nothing done"
fi

