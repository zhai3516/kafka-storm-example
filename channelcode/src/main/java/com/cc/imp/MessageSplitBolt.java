package com.cc.imp;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MessageSplitBolt extends BaseRichBolt {

	/**
	 *  class get log-statistics-info from kafka cluster 
	 */
	
	private static final long serialVersionUID = 1L;
	
	private OutputCollector collector;

	public void execute(Tuple tuple) {
		String[] channelLogs = tuple.getValue(0).toString().split("\n");
		for (String logString : channelLogs){
			String[] logContent = logString.split(" ");		
			String device = logContent[1];//device
			String channel = logContent[2];//channel
			String code = logContent[3];//code
			String count = logContent[4];//count
			String total = logContent[5];//total
			String ratio = logContent[6];//ratio
			collector.emit(new Values(device,channel,code,count,total,ratio));
		}
		this.collector.ack(tuple);
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("device","channel","code","count","total","ratio"));		
	}
}
