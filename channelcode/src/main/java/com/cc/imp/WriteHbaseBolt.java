package com.cc.imp;

import com.cc.util.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class WriteHbaseBolt extends BaseRichBolt {

    /**
     *  class that write calculate result to hbase
     *  hbase map structure:
     *  row-key
     */
	
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(WriteHbaseBolt.class);
    private Configuration configure;
    private HTable table;
    
    private String hbaseZookeeperQuorum;
    private String hbaseClusterDistirbuted;
    private String hbaseRootdir;
    private String hbaseMaster;
    private String hbaseTable;
    private String hbaseColumnFamlity;
    private int sendCheckFreq;
    
    public WriteHbaseBolt(){
    	//using default configuration which is not update by command
    	this(TopologyConfig.hbaseZookeeperQuorum, TopologyConfig.hbaseClusterDistirbuted,TopologyConfig.hbaseRootdir,
    			TopologyConfig.hbaseMaster,TopologyConfig.hbaseTable,TopologyConfig.hbaseColumnFamlity);
    }
    	
    public WriteHbaseBolt(String hbaseZookeeperQuorum,String hbaseClusterDistirbuted, String hbaseRootdir,
    		String hbaseMaster, String hbaseTable, String hbaseColumnFamlity){
    	//using configuration which has been updated by command
    	this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
    	this.hbaseClusterDistirbuted = hbaseClusterDistirbuted;
    	this.hbaseRootdir = hbaseRootdir;
    	this.hbaseMaster = hbaseMaster;
    	this.hbaseTable = hbaseTable;
    	this.hbaseColumnFamlity = hbaseColumnFamlity;
    	this.sendCheckFreq = TopologyConfig.hbaseFlushInterval;
    }

	public void execute(Tuple tuple) {
        try {
        	if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && 
    				tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)){
        		logger.info("flush commit hbase !");
        		table.flushCommits();
        	}else{
				writeHbase(this.configure, tuple.getStringByField("rowkey"),hbaseColumnFamlity,
						tuple.getStringByField("column"),tuple.getStringByField("columnvalue"));
        	}
		} catch (IOException e) {
			logger.error("Hbase save wrong !\n", e);
		}
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		configure = HBaseConfiguration.create();
        configure.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        configure.set("hbase.cluster.distributed", hbaseClusterDistirbuted);
        configure.set("hbase.rootdir",hbaseRootdir);
        configure.set("hbase.master", hbaseMaster);
        try {
			table = new HTable(configure, Bytes.toBytes(hbaseTable));
			table.setAutoFlush(false, true);
		} catch (IOException e) {
			logger.error("init hbase table wrong !\n", e);
		}
	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, this.sendCheckFreq);
		return conf;
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

    public void writeHbase(Configuration conf, String rowKey, String columnFamlity,
        String column, String columnValue) throws IOException {
        /**
         *  this method used to write map to hbase 
         *  hbase structure :
         *  row_key : channel_name#timestamp#code_name
         *  column family : "channel"
         *  column name : host_name
         *  value: code_count#toutal_count#ratio_count
         */        
    	
    	try{
        	Put put = new Put(Bytes.toBytes(rowKey));
        	put.add(Bytes.toBytes(columnFamlity),Bytes.toBytes(column),
                    Bytes.toBytes(columnValue));
    		table.put(put);
            logger.debug("hbase put date is :" + put.toString());
        }catch (Exception e) {
            logger.error("Write Hbase Wrong {" + rowKey + ":" + column + "}\n", e);
        }
    }
}
