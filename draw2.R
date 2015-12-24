library(ggplot2)
library(sqldf)

getdata <- function(){
	#agg
	agg_set <- read.csv("./output/aggregation_node.csv",header = TRUE)
	subagg_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'AggregationExecNode' || AggregationExecNode as node_type ,build_phase_time_elapsed as time_elapsed from agg_set")
	#exchange_receiver_conversion
	exchange_conversion_set <- read.csv("./output/exchange_node_receiver.csv",header = TRUE)
	subexchange_conversion_set <- sqldf(paste("select query_name ,ImpalaServer, PlanFragmentExecutor as  plan_fragment,'exchange_conversion'||ExchangeExecNode as node_type ,time_elapsed from exchange_conversion_set where stage = '","conversion","'",sep = ""))
	#exchange_receiver_deserialization
	exchange_deserialization_set <- read.csv("./output/exchange_node_receiver.csv",header = TRUE)
	subexchange_deserialization_set <- sqldf(paste("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'exchange_deserialization'||ExchangeExecNode as node_type ,time_elapsed from exchange_deserialization_set where stage = '","deserialization","'",sep = ""))
	#exchange_serialzation
	exchange_serialization_set <- read.csv("./output/exchange_node_sender.csv",header = TRUE)
	subexchange_serialization_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'exchange_serialization'||ExchangeExecNode as node_type ,time_elapsed from exchange_serialization_set")
	#hash_join_build
	hash_build_set <- read.csv("./output/hash_join_node.csv",header = TRUE)
	subhash_build_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'hash_build_phase'||HashJoinExecNode as node_type ,build_phase_time_elapsed as time_elapsed from hash_build_set")
	#hash_join_probe
	hash_probe_set <- read.csv("./output/hash_join_node.csv",header = TRUE)
	subhash_probe_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'hash_probe_phase'||HashJoinExecNode as node_type ,probe_phase_time_elapsed as time_elapsed from hash_probe_set")
	#hdfs_scan
	hdfs_scan_set <- read.csv("./output/hdfs_scan_node.csv",header = TRUE)
	subhdfs_scan_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'hdfs_scan'||HdfsScanExecNode as node_type , time_elapsed from hdfs_scan_set")
	
	
	dataset <- rbind(subagg_set,subexchange_conversion_set,subexchange_deserialization_set,subexchange_serialization_set,subhash_build_set,subhash_probe_set,subhdfs_scan_set)
	return (dataset)
}
addtheme <- function(pic){
	return (pic )
}

draw_OnePicAllServer <- function(){
	dataset <- getdata()
	query_name_set <- sqldf("select distinct query_name from dataset")
	for(query_name in query_name_set$query_name) {
		#serverList_set <- sqldf(paste("select distinct ImpalaServer from dataset where query_name = '",query_name,"'",sep = ""))
		#for(impalaserver in serverList_set$ImpalaServer){
			png(paste(query_name,".png",sep=""),width=1680,heigh=10500)
			subset <- dataset[which(dataset$query_name == query_name),]
			pic <- ggplot(subset,aes(x = plan_fragment , y = time_elapsed ,fill = factor(node_type)))+ 
					geom_bar(stat="identity",position = "stack" ) + 
					facet_wrap(~ImpalaServer,ncol = 4) +
					ylab("Accumulated Cost") + xlab("Plan Fragment Id") 
			print(addtheme(pic))
			dev.off()
	#	}
	}
}

draw_OnePicOneServer <- function(){
	dataset <- getdata()
	query_name_set <- sqldf("select distinct query_name from dataset")
	for(query_name in query_name_set$query_name) {
		serverList_set <- sqldf(paste("select distinct ImpalaServer from dataset where query_name = '",query_name,"'",sep = ""))
		for(impalaserver in serverList_set$ImpalaServer){
			png(paste(query_name,"_",impalaserver,".png",sep=""),width=864,heigh=179)
			subset <- dataset[which(dataset$query_name == query_name &
									dataset$ImpalaServer == impalaserver),]
			pic <- ggplot(subset,aes(x = plan_fragment , y = time_elapsed ,fill = factor(node_type)))+ 
					geom_bar(stat="identity",position = "stack" ) + 
					xlab("Plan Fragment Executer Id") + ylab("Accumulated Cost") + 
					coord_flip() 
			print(addtheme(pic) 
				+ theme(legend.position = "no" ))
			dev.off()
		}
	}
}

draw_OnePicAllServer()