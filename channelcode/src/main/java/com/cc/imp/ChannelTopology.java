package com.cc.imp;

import com.cc.util.TopologyConfig;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class ChannelTopology {
	
	/**
	 *  default configures of storm, kafka, hbase and opentsdb
	 */
	
	// storm conf
	public static final String KAFKA_SPOUT_ID = "kafka-spout";
	public static final String MESSAGE_SPLIT_BOLT_ID = "split-bolt";
	public static final String CALCULATE_BOLT_ID = "calculate-bolt";
	public static final String WRITE_OPENTSDB_BOLT_ID = "write-tsdb-bolt";
	public static final String WRITE_HBASE_BOLT_ID = "write-hbase-bolt";
	public static final String SEND_TRANSFER_BOLT_ID = "send-transfer-bolt";
	public static final String OPENTSDB_STREAM = "tsdb-stream";
	public static final String HBASE_STREAM = "hbase-stream";
	public static final String TRANSFER_STREAM = "transfer-stream";
	
	public static void main(String[] args) throws Exception{
		updateSettings(args);
		
		KafkaDataSpout spout = new KafkaDataSpout(TopologyConfig.kafkaTopic, TopologyConfig.kafkaZookeeperAddr,
				TopologyConfig.kafkaGroup);
		MessageSplitBolt splitBolt = new MessageSplitBolt();
		CalculateBolt calBolt = new CalculateBolt(Integer.valueOf(TopologyConfig.sendCheckFreq));
		WriteOpenTSDBBolt writeTsdbBolt = new WriteOpenTSDBBolt(TopologyConfig.tsdbUrl,
				TopologyConfig.tsdbCountMetric, TopologyConfig.tsdbRatioMetric);
		WriteHbaseBolt writeHbaseBolt = new WriteHbaseBolt(TopologyConfig.hbaseZookeeperQuorum,
				TopologyConfig.hbaseClusterDistirbuted,TopologyConfig.hbaseRootdir,TopologyConfig.hbaseMaster,
				TopologyConfig.hbaseTable,TopologyConfig.hbaseColumnFamlity);
		SendTransferBolt sendTransferBolt = new SendTransferBolt(TopologyConfig.tsdbCountMetric,
				TopologyConfig.tsdbRatioMetric, TopologyConfig.transferIp,TopologyConfig.transferPort);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(KAFKA_SPOUT_ID, spout, TopologyConfig.spoutNum);
		builder.setBolt(MESSAGE_SPLIT_BOLT_ID, splitBolt, TopologyConfig.splitBoltNum)
			.shuffleGrouping(KAFKA_SPOUT_ID);
		builder.setBolt(CALCULATE_BOLT_ID, calBolt, TopologyConfig.calculateBoltNum)
			.fieldsGrouping(MESSAGE_SPLIT_BOLT_ID, new Fields("channel"));
		builder.setBolt(WRITE_OPENTSDB_BOLT_ID, writeTsdbBolt, TopologyConfig.tsdbBoltNum)
			.shuffleGrouping(CALCULATE_BOLT_ID,OPENTSDB_STREAM);
		builder.setBolt(WRITE_HBASE_BOLT_ID, writeHbaseBolt, TopologyConfig.hbaseBoltNum)
			.shuffleGrouping(CALCULATE_BOLT_ID,HBASE_STREAM);
		builder.setBolt(SEND_TRANSFER_BOLT_ID, sendTransferBolt, TopologyConfig.transferBoltNum)
			.shuffleGrouping(CALCULATE_BOLT_ID,TRANSFER_STREAM);
		
		backtype.storm.Config conf = new backtype.storm.Config();
		conf.setNumWorkers(TopologyConfig.workerNum);
		conf.setNumAckers(0);

		if(args.length == 0){
			LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("test", conf, builder.createTopology());
	        Thread.sleep(3600000);
	        cluster.shutdown();
		}else{
			StormSubmitter.submitTopology(TopologyConfig.topologyName, conf, builder.createTopology());
		}
        
	}
	
	public static void updateSettings(String[] args){
    	Option name = Option.builder("name").required(false).hasArg()
    				.desc("The name of storm topology").build();
    	Option workers = Option.builder("workers").required(false).hasArg()
    				.desc("The num of storm workers").build();

    	Option spountCount = Option.builder("spountCount").required(false).hasArg()
					.desc("The num of spout tasks").build();
    	Option splitCount = Option.builder("splitCount").required(false).hasArg()
    				.desc("The num of split bolt tasks").build();
    	Option calCount = Option.builder("calCount").required(false).hasArg()
					.desc("The num of calculate bolt tasks").build();
    	Option hbaseCount = Option.builder("hbaseCount").required(false).hasArg()
					.desc("The num of write hbase bolt tasks").build();
    	Option tsdbCount = Option.builder("tsdbCount").required(false).hasArg()
					.desc("The num of write opentsdb bolt tasks").build();
    	Option transferCount = Option.builder("transferCount").required(false).hasArg()
					.desc("The num of send transfer bolt tasks").build();
    	
    	Option kafkaTopic = Option.builder("topic").required(false).hasArg()
					.desc("The topic of kafka").build();
    	Option kafkaGroupId = Option.builder("group").required(false).hasArg()
					.desc("The group id of kafka").build();
    	Option kafkaZookeeperAddr = Option.builder("kafkazk").required(false).hasArg()
					.desc("The address of kafka zookeeper").build();
    	
    	Option tsdbuUrl = Option.builder("tsdburl").required(false).hasArg()
					.desc("The url address of opentsdb").build();
    	Option tsdbCountMetric = Option.builder("count").required(false).hasArg()
					.desc("The metric of count item").build();
    	Option tsdbRatioMetirc = Option.builder("ratio").required(false).hasArg()
    				.desc("The metric of ratio item").build();
    	
    	Option hbaseZookeeperAddr = Option.builder("hbasezk").required(false).hasArg()
					.desc("The address of hbase zookeeper").build();
    	Option hbaseMaster = Option.builder("master").required(false).hasArg()
					.desc("The master address of hbase").build();
    	Option hbaseRootDir = Option.builder("rootdir").required(false).hasArg()
    				.desc("The root dir of hbase").build();
    	
    	Option transferIp = Option.builder("transferip").required(false).hasArg()
					.desc("The ip of transfer").build();
    	Option transferPort = Option.builder("transferport").required(false).hasArg()
    				.desc("The port of transfer").build();
    	
    	Options options = new Options();
    	options.addOption(name);	
    	options.addOption(workers);
    	options.addOption(kafkaTopic);
    	options.addOption(kafkaGroupId);
    	options.addOption(kafkaZookeeperAddr);
    	options.addOption(spountCount);
    	options.addOption(splitCount);
    	options.addOption(calCount);
    	options.addOption(hbaseCount);
    	options.addOption(tsdbCount);
    	options.addOption(transferCount);
    	options.addOption(tsdbuUrl);
    	options.addOption(tsdbCountMetric);
    	options.addOption(tsdbRatioMetirc);
    	options.addOption(hbaseZookeeperAddr);
    	options.addOption(hbaseMaster);
    	options.addOption(hbaseRootDir);
    	options.addOption(transferIp);
    	options.addOption(transferPort);
    	
    	// automatically generate the help statement
    	HelpFormatter formatter = new HelpFormatter();
    	// create the command line parser
    	CommandLineParser parser = new DefaultParser();
    	// parse the command line arguments
        try {
			CommandLine line = parser.parse(options, args);
		    if (line.hasOption("workers"))
		    	TopologyConfig.workerNum = Integer.valueOf(line.getOptionValue("workers"));
		    if(line.hasOption("name")) 
		    	TopologyConfig.topologyName = line.getOptionValue("name");
		    if(line.hasOption("topic")) 
		    	TopologyConfig.kafkaTopic = line.getOptionValue("topic");	
		    if(line.hasOption("group")) 
		    	TopologyConfig.kafkaGroup = line.getOptionValue("group");
		    if(line.hasOption("kafkazk"))
		    	TopologyConfig.kafkaZookeeperAddr = line.getOptionValue("kafkazk");
		    
		    if(line.hasOption("spountCount"))
		    	TopologyConfig.spoutNum = Integer.valueOf(line.getOptionValue("spountCount"));
		    if(line.hasOption("splitCount"))
		    	TopologyConfig.splitBoltNum = Integer.valueOf(line.getOptionValue("splitCount"));
		    if(line.hasOption("calCount"))
		    	TopologyConfig.calculateBoltNum = Integer.valueOf(line.getOptionValue("calCount"));
		    if(line.hasOption("hbaseCount"))
		    	TopologyConfig.hbaseBoltNum = Integer.valueOf(line.getOptionValue("hbaseCount"));
		    if(line.hasOption("tsdbCount"))
		    	TopologyConfig.tsdbBoltNum = Integer.valueOf(line.getOptionValue("tsdbCount"));
		    if(line.hasOption("transferCount"))
		    	TopologyConfig.transferBoltNum = Integer.valueOf(line.getOptionValue("transferCount"));
		    
		    if(line.hasOption("tsdburl"))
		    	TopologyConfig.tsdbUrl = line.getOptionValue("tsdburl");
		    if(line.hasOption("count"))
		    	TopologyConfig.tsdbCountMetric = line.getOptionValue("count");
		    if(line.hasOption("ratio"))
		    	TopologyConfig.tsdbRatioMetric = line.getOptionValue("ratio");
		    if(line.hasOption("hbasezk"))
		    	TopologyConfig.hbaseZookeeperQuorum = line.getOptionValue("hbasezk");
		    if(line.hasOption("master"))
		    	TopologyConfig.hbaseMaster = line.getOptionValue("master");
		    if(line.hasOption("rootdir"))
		    	TopologyConfig.hbaseRootdir = line.getOptionValue("rootdir");
		    if(line.hasOption("transferip"))
		    	TopologyConfig.transferIp = line.getOptionValue("transferip");
		    if(line.hasOption("transferport"))
		    	TopologyConfig.transferPort = line.getOptionValue("transferport");
		    
		} catch (ParseException e) {
			e.printStackTrace();
			formatter.printHelp("test", options);
		}

	}
}
