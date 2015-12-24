library(ggplot2)
library(sqldf)

getdata <- function(){
	#agg
	agg_set <- read.csv("./output/aggregation_node.csv",header = TRUE)
	subagg_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'AggregationExecNode' as node_type ,build_phase_time_elapsed as time_elapsed from agg_set")
	#exchange_receiver_conversion
	exchange_conversion_set <- read.csv("./output/exchange_node_receiver.csv",header = TRUE)
	subexchange_conversion_set <- sqldf(paste("select query_name ,ImpalaServer, PlanFragmentExecutor as  plan_fragment,'exchange_conversion' as node_type ,time_elapsed from exchange_conversion_set where stage = '","conversion","'",sep = ""))
	#exchange_receiver_deserialization
	exchange_deserialization_set <- read.csv("./output/exchange_node_receiver.csv",header = TRUE)
	subexchange_deserialization_set <- sqldf(paste("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'exchange_deserialization' as node_type ,time_elapsed from exchange_deserialization_set where stage = '","deserialization","'",sep = ""))
	#exchange_serialzation
	exchange_serialization_set <- read.csv("./output/exchange_node_sender.csv",header = TRUE)
	subexchange_serialization_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'exchange_serialization' as node_type ,time_elapsed from exchange_serialization_set")
	#hash_join_build
	hash_build_set <- read.csv("./output/hash_join_node.csv",header = TRUE)
	subhash_build_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'hash_build_phase' as node_type ,build_phase_time_elapsed as time_elapsed from hash_build_set")
	#hash_join_probe
	hash_probe_set <- read.csv("./output/hash_join_node.csv",header = TRUE)
	subhash_probe_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'hash_probe_phase' as node_type ,probe_phase_time_elapsed as time_elapsed from hash_probe_set")
	#hdfs_scan
	hdfs_scan_set <- read.csv("./output/hdfs_scan_node.csv",header = TRUE)
	subhdfs_scan_set <- sqldf("select query_name ,ImpalaServer, PlanFragmentExecutor as plan_fragment,'hdfs_scan' as node_type , time_elapsed from hdfs_scan_set")
	
	
	dataset <- rbind(subagg_set,subexchange_conversion_set,subexchange_deserialization_set,subexchange_serialization_set,subhash_build_set,subhash_probe_set,subhdfs_scan_set)
	return (dataset)
}
addtheme <- function(pic){
	return (pic )
}

dataset <- getdata()
query_name_set <- sqldf("select distinct query_name from dataset")
for(query_name in query_name_set$query_name) {
	png(paste(query_name,".png"),width=800,heigh=600)
	subset <- dataset[which(dataset$query_name == query_name),]
	#p <- ggplot(subset,aes(x=plan_fragment,y=time_elapsed)) + geom_point()
	pic <- ggplot(subset,aes(x = plan_fragment , y = time_elapsed ,fill = factor(node_type)))+ 
			geom_bar(stat="identity",position = "stack" ) + 
			facet_wrap(~ImpalaServer,ncol = 2)
	print(addtheme(pic))
	dev.off()
}