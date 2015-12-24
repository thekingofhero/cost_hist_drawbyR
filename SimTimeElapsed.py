import os
import re
import subprocess 
import traceback 
import platform

def SimTimeElapsed(file,csv_fp_dic,total_csv_fp):
    
    
    if 'ss_max' not in file:
        query_name = os.path.basename(file).split('_')[0]
    else:
        query_name = 'ss_max'
        
    #linux
    if platform.system() == "Linux":
        fn_ptr = os.popen("grep -i 'elapsed' %s"%(file))
    #windows
    elif platform.system() == "Windows":
        fn_ptr =os.popen("find /i \"elapsed\" %s"%(file))
    with fn_ptr as fp:
        for line in fp:
            #hdfs scan node
            com = r"\[INFO\]\s+(\d+.\d+e\+\d+)\sns:.*ImpalaServer\[(\d+)\].*PlanFragmentExecutor\[(\d+)\].*\[Backend (\d+)\].*\[HdfsScanExecNode (\d+)\].*time elapsed: (\d+.\d+) sec.*throughput: (\d+.\d+) MB/sec"
            line_match = re.match(com,line)
            if line_match:
                timestamp,ImpalaServer,PlanFragmentExecutor,Backend,HdfsScanExecNode,time_elapsed,throughput = line_match.groups()
                print ("%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,Backend,HdfsScanExecNode,time_elapsed,throughput),file=csv_fp_dic['scan'])
                print ("%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,Backend,HdfsScanExecNode,time_elapsed,throughput),file=total_csv_fp['scan'])
                continue
               
            #exchange node serialization
            com = r"\[INFO\]\s+(\d+.\d+e\+\d+)\sns:.*ImpalaServer\[(\d+)\].*PlanFragmentExecutor\[(\d+)\].*\[Backend \(SRC\) (\d+)\] \[Backend \(DEST\) (\d+)\] \[Sender (\d+)\] \[ExchangeExecNode \(DEST\) (\d+)\] total time elapsed on serialization (\d+.\d+)"
            line_match = re.match(com,line)
            if line_match:
                timestamp,ImpalaServer,PlanFragmentExecutor,Backend_src,Backend_des,Sender,ExchangeExecNode,time_elapsed = line_match.groups()
                print ("%s,%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,Backend_src,Backend_des,Sender,ExchangeExecNode,float(time_elapsed) / 1000.0),file=csv_fp_dic['exchange_sender'])
                print ("%s,%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,Backend_src,Backend_des,Sender,ExchangeExecNode,float(time_elapsed) / 1000.0),file=total_csv_fp['exchange_sender'])
                continue    
               
            #exchange node row batch convertion
            com = r"\[INFO\]\s+(\d+.\d+e\+\d+)\sns:.*ImpalaServer\[(\d+)\].*PlanFragmentExecutor\[(\d+)\].*\[Backend (\d+)\] \[Plan Fragment (\d+)\] \[ExchangeExecNode (\d+)\] total time elapsed on row batch convertion (\d+.\d+)"
            line_match = re.match(com,line)
            if line_match:
                timestamp,ImpalaServer,PlanFragmentExecutor,Backend,PlanFragment,ExchangeExecNode,time_elapsed = line_match.groups()
                print ("%s,%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,'conversion',PlanFragment,Backend,ExchangeExecNode,float(time_elapsed) / 1000.0),file=csv_fp_dic['exchange_receiver'])
                print ("%s,%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,'conversion',PlanFragment,Backend,ExchangeExecNode,float(time_elapsed) / 1000.0),file=total_csv_fp['exchange_receiver'])
                continue 
               
            #exchange node row batch deserialization
            com = r"\[INFO\]\s+(\d+.\d+e\+\d+)\sns:.*ImpalaServer\[(\d+)\].*PlanFragmentExecutor\[(\d+)\].*\[Backend (\d+)\] \[Plan Fragment (\d+)\] \[ExchangeExecNode (\d+)\] total time elapsed row batch deserialization (\d+.\d+)"
            line_match = re.match(com,line)
            if line_match:
                timestamp,ImpalaServer,PlanFragmentExecutor,Backend,PlanFragment,ExchangeExecNode,time_elapsed = line_match.groups()
                print ("%s,%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,'deserialization',PlanFragment,Backend,ExchangeExecNode,float(time_elapsed) / 1000.0),file=csv_fp_dic['exchange_receiver'])
                print ("%s,%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,'deserialization',PlanFragment,Backend,ExchangeExecNode,float(time_elapsed) / 1000.0),file=total_csv_fp['exchange_receiver'])
                continue 
               
            #aggregation node
            com = r"\[INFO\]\s+(\d+.\d+e\+\d+)\sns:.*ImpalaServer\[(\d+)\].*PlanFragmentExecutor\[(\d+)\].*\[Backend (\d+)\] \[AggregationExecNode (\d+)\] build phase \(time elapsed\): (\d.\d+) sec"
            line_match = re.match(com,line)
            if line_match:
                timestamp,ImpalaServer,PlanFragmentExecutor,Backend,AggregationExecNode,build_phase_time_elapsed = line_match.groups()
                print ("%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,Backend,AggregationExecNode,build_phase_time_elapsed),file=csv_fp_dic['agg'])
                print ("%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,Backend,AggregationExecNode,build_phase_time_elapsed),file=total_csv_fp['agg'])
                continue 
               
            #hash join node
            com = r"\[INFO\]\s+(\d+.\d+e\+\d+)\sns:.*ImpalaServer\[(\d+)\].*PlanFragmentExecutor\[(\d+)\].*\[Backend (\d+)\] \[HashJoinExecNode (\d+)\] build phase \(time elapsed\): (\d+.\d+) sec, probe phase \(time elapsed\): (\d+.\d+) sec"
            line_match = re.match(com,line)
            if line_match:
                timestamp,ImpalaServer,PlanFragmentExecutor,Backend,HashJoinExecNode,build_phase_time_elapsed,probe_phase_time_elapsed = line_match.groups()
                print ("%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,Backend,HashJoinExecNode,build_phase_time_elapsed,probe_phase_time_elapsed),file=csv_fp_dic['hash'])
                print ("%s,%s,%s,%s,%s,%s,%s,%s"%(query_name,timestamp,"Server"+ImpalaServer,PlanFragmentExecutor,Backend,HashJoinExecNode,build_phase_time_elapsed,probe_phase_time_elapsed),file=total_csv_fp['hash'])
                continue
             
def csv_header(csv_fp_dic): 
    for key in csv_fp_dic.keys():
        if key == 'scan':
            print("query_name,timestamp,ImpalaServer,PlanFragmentExecutor,Backend,HdfsScanExecNode,time_elapsed,throughput",file=csv_fp_dic[key])           
        if key == 'exchange_sender':
            print("query_name,timestamp,ImpalaServer,PlanFragmentExecutor,Backend_src,Backend_des,Sender,ExchangeExecNode,time_elapsed",file=csv_fp_dic[key])
        if key == 'exchange_receiver':
            print("query_name,timestamp,ImpalaServer,PlanFragmentExecutor,stage,Planfragment,Backend,ExchangeExecNode,time_elapsed",file=csv_fp_dic[key])
        if key == 'agg':
            print("query_name,timestamp,ImpalaServer,PlanFragmentExecutor,Backend,AggregationExecNode,build_phase_time_elapsed",file=csv_fp_dic[key])
        if key == 'hash':
            print("query_name,timestamp,ImpalaServer,PlanFragmentExecutor,Backend,HashJoinExecNode,build_phase_time_elapsed,probe_phase_time_elapsed",file=csv_fp_dic[key])

def mk_totalcsv(output_dir): 
    csv_dic = {
               'scan':'hdfs_scan_node.csv',
               'exchange_sender':'exchange_node_sender.csv',
               'exchange_receiver':'exchange_node_receiver.csv',
               'agg':'aggregation_node.csv',
               'hash':'hash_join_node.csv'
               }
    csv_fp_dic = {}
    for key in csv_dic.keys():
        fp = open(os.path.join(output_dir,csv_dic[key]),'w')
        if key not in csv_fp_dic.keys():
            csv_fp_dic[key] = fp
            
    csv_header(csv_fp_dic)
    return csv_fp_dic
      
               
def mkcsv(file,output_dir,total_csv_fp):
    sub_output_dir = os.path.join(output_dir,os.path.basename(file))
    if not os.path.isdir(sub_output_dir):
        os.mkdir(sub_output_dir)
        
    csv_dic = {
               'scan':'hdfs_scan_node.csv',
               'exchange_sender':'exchange_node_sender.csv',
               'exchange_receiver':'exchange_node_receiver.csv',
               'agg':'aggregation_node.csv',
               'hash':'hash_join_node.csv'
               }
    csv_fp_dic = {}
    try:
        for key in csv_dic.keys():
            fp = open(os.path.join(sub_output_dir,csv_dic[key]),'w')
            if key not in csv_fp_dic.keys():
                csv_fp_dic[key] = fp
                
        csv_header(csv_fp_dic)
        SimTimeElapsed(file,csv_fp_dic,total_csv_fp)
        
    except:
        traceback.print_exc()
    finally:
        for key in csv_fp_dic.keys():
            csv_fp_dic[key].close()  

               
def prepare():
    import time
    s = time.time()
    #log_dir = r"C:\Users\deweiwax\workspaces\simulator_run\TEXT_3000_2.7G_10G_6DISK"
    log_dir = r"W:\junliu\Benchmark\Impala\Simulator(Parquet)\PARQUET_3000_2.7G_10G_8DISK\q59_PARQUET_NONE_3000_1395_1446255211676.log"
    #log_dir = r"W:\junliu\Benchmark\cofs_execution.log"
    output_dir = r".\output"
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    
    task_list = []    
    if os.path.isfile(log_dir):
        task_list.append((log_dir,output_dir))
    else:
        for root,dirs,files in os.walk(log_dir):
            for file in files:
                if file.endswith('.log') and (file.startswith('q') or file.startswith('ss_max')):
                    task_list.append((os.path.join(root,file),output_dir))
    task_list.sort()
    
    total_csv_fp = mk_totalcsv(output_dir)
    try:
        
        print(task_list)
        for task in task_list:
            csv_dic = mkcsv(task[0], task[1],total_csv_fp)
    except:
        traceback.print_exc()
    finally:
        for key in total_csv_fp.keys():
            total_csv_fp[key].close()  
    e = time.time()
    print(e-s)
    
def drawPic():
    os.system("Rscript.exe draw2.R")

if __name__ == '__main__':
   # prepare()
    drawPic()