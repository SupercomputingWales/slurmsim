
BASE_IMAGE=sunbird4
SCRIPTS_DIR=/home/michele/slurm_sim_analysis/setup
LIST_OF_SIMULATIONS=starts_ends_qosps.txt

mdirname(){
  START=$1
  END=$2
  QOSP=$3
  echo $START\_$END\_$QOSP
}

base_container_name(){
  START=$1
  END=$2
  QOSP=$3
  echo v5_$(mdirname $START $END $QOSP)_base
}

container_name(){
  START=$1
  END=$2
  QOSP=$3
  echo v5_$(mdirname $START $END $QOSP)_v2
}

committed_image_name(){
  START=$1
  END=$2
  QOSP=$3
  echo v5_$(mdirname $START $END $QOSP)
}

sacct_output_filename(){
  START=$1
  END=$2
  echo sacct_output_$START\_$END.csv 
}

remove(){
  START=$1
  END=$2
  QOSP=$3

  rm -r ../$(mdirname $START $END $QOSP)
  docker rm $(base_container_name $START $END $QOSP)
  docker rm $(container_name $START $END $QOSP)
  docker rmi $(committed_image_name $START $END $QOSP)

}


 
cycle(){
FUN=$1
while read START END QOSP
do
    DIR=$(mdirname $START $END $QOSP)
    echo "Cycling on dir $DIR"
    mkdir -p $DIR
    cd $DIR
    $FUN $START $END $QOSP 
    cd -
done < <(sed 's/#.*$//' $LIST_OF_SIMULATIONS | awk '(NF==3){print $0}')


}
 
cycle_async(){
FUN=$1
WAIT=$2 # optional
while read START END QOSP
do
    DIR=$(mdirname $START $END $QOSP)
    echo "Cycling on dir $DIR"
    mkdir -p $DIR
    cd $DIR
    $FUN $START $END $QOSP &
    $WAIT
    cd -
done < <(sed 's/#.*$//' $LIST_OF_SIMULATIONS | awk '(NF==3){print $0}')


}

wait_for_ps_to_be_less_than_10(){

   echo 'Waiting a bit anyway...'
   sleep 10
   echo 'Checking...'
   while  [ "$(docker ps | wc -l )" -ge "10" ]
   do
      echo 'Too many containers running. Waiting 5s'
      sleep 5
   done
   echo 'Go!'
}

prepare_run_files(){
  START=$1
  END=$2
  QOSP=$3

  $SCRIPTS_DIR/prepare_run_files.v2.py ../package/config.v2 --start $START --end $END --newqosprio $QOSP

}

create_scw_tar(){
  START=$1
  END=$2
  QOSP=$3

  tar -cvf scw.tar sacct_output.csv sunbird.cfg QOS_data.csv slurm.conf

}


make_base_container(){
  START=$1
  END=$2
  QOSP=$3
  CONTAINER_NAME=$(base_container_name $START $END $QOSP)
  COMMITTED_IMAGE_NAME=$(committed_image_name $START $END $QOSP)

  docker create --name $CONTAINER_NAME $BASE_IMAGE
  docker cp scw.tar $CONTAINER_NAME:/home/slurm/slurm_sim_ws/slurm_sim_tools/scw
  docker commit $CONTAINER_NAME $COMMITTED_IMAGE_NAME
  docker rm $CONTAINER_NAME

}

run_container(){
  START=$1
  END=$2
  QOSP=$3
  COMMITTED_IMAGE_NAME=$(committed_image_name $START $END $QOSP)
  CONTAINER_NAME=$(container_name $START $END $QOSP)

  docker run --network=none --name $CONTAINER_NAME $COMMITTED_IMAGE_NAME /install_files/run_all.sh

}

get_data_out(){
  START=$1
  END=$2
  QOSP=$3
  CONTAINER_NAME=$(container_name $START $END $QOSP)

  docker cp $CONTAINER_NAME:/home/slurm/slurm_sim_ws/sim/scw/baseline/results/jobcomp.log ./
  docker cp $CONTAINER_NAME:/home/slurm/slurm_sim_ws/sim/scw/baseline/results/slurmctld.log ./
  docker cp $CONTAINER_NAME:/home/slurm/slurm_sim_ws/slurm_sim_tools/scw/test_trace.csv ./

}

check_job_number(){

  echo $(pwd |xargs basename ) $(wc -l jobcomp.log ) $(wc -l sacct_output.csv)

}

do_all_pre_run(){

  START=$1
  END=$2
  QOSP=$3
  
  prepare_run_files $START $END $QOSP
  create_scw_tar $START $END $QOSP
  make_base_container $START $END $QOSP

}

do_all(){

  START=$1
  END=$2
  QOSP=$3
  
  prepare_run_files $START $END $QOSP
  create_scw_tar $START $END $QOSP
  make_base_container $START $END $QOSP
  run_container $START $END $QOSP
  get_data_out $START $END $QOSP

}
 
