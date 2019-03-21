while read START END
do
    sacct --format JobIdRaw,Submit,Timelimit,End,Start,NTasks,NNodes,User,QOS,Partition,Account,ReqMem,ReqGRES,NCPUS,State --allocations --parsable2 --allusers --starttime=$START --endtime=$END > sacct_output_$START\_$END.csv 
done < starts_ends.txt
