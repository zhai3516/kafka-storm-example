package com.cc.util;

public class TopologyConfig {

	/**
	 * class that provide program configuration
	 */

	public static int workerNum = 2;
	public static int ackerNum = 2;
	public static int spoutNum = 1;
	public static int splitBoltNum = 2;
	public static int calculateBoltNum = 2;
	public static int hbaseBoltNum = 1;
	public static int tsdbBoltNum = 1;
	public static int transferBoltNum = 1;
	public static String topologyName = "channel-code-test";

	// calculate conf
	public static String sendCheckFreq = "300";

	// kafka conf
	public static String kafkaTopic = "channelCode";
	public static String kafkaZookeeperAddr = "127.0.0.1:2181";
	public static String kafkaGroup = "channel-code-local-test-zhai";

	// openTSDB conf
	public static String tsdbUrl = "http://127.0.0.1:4242/api/put?details";
	public static String tsdbCountMetric = "imp.channel.code.count";
	public static String tsdbRatioMetric = "imp.channel.code.ratio";

	// hbase conf
	public static String hbaseZookeeperQuorum = "127.0.0.1";
	public static String hbaseClusterDistirbuted = "true";
	public static String hbaseRootdir = "hdfs://127.0.0,1:9000/hbase";
	public static String hbaseMaster = "127.0.0.1";
	public static String hbaseTable = "imp";
	public static String hbaseColumnFamlity = "channel";
	public static int hbaseFlushInterval = 60;

	// transfer conf
	public static String transferIp = "127.0.0.1";
	public static String transferPort = "8433";

}
