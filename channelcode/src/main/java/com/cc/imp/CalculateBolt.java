package com.cc.imp;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.apache.log4j.Logger;

import com.cc.util.TopologyConfig;

public class CalculateBolt extends BaseRichBolt{

	/**
	 *  class that statistics last 5min each channel-code info 
	 *  and calculate channel-code result 
	 *  and emit OpenTSDB key-value-tags to next function
	 */
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(CalculateBolt.class);
	
	private long timestamp;
	private Map<String, Long> channelCountMap;
	private Map<String, Long> tsdbMap;
	private Map<String, Map<String,String>> hbaseMap;
	private OutputCollector collector;
	private int sendCheckFreq;
	
	public CalculateBolt(){
		//using default configuration which is not update by command
		this(Integer.valueOf(TopologyConfig.sendCheckFreq));
	}
	
	public CalculateBolt(int sendCheckFreq){
		//using configuration which has been updated by command
		this.sendCheckFreq = sendCheckFreq;
	}
	
	public void execute(Tuple tuple) {	

		this.saveMaps(tuple);
		if(this.isNewTimeBucke(this.timestamp)){
			logger.info("Crontab time: Emit maps !");
			logger.info("Before clean , size is  : " + this.tsdbMap.size() + "-" + this.hbaseMap.size() + "-"
					+ this.channelCountMap.size());
			long start = System.currentTimeMillis();
			this.timestamp = System.currentTimeMillis()/1000/this.sendCheckFreq + 1;//save as next send timestamp
			this.emitTsdbMap(ChannelTopology.OPENTSDB_STREAM,ChannelTopology.TRANSFER_STREAM,
					this.collector, this.tsdbMap, this.channelCountMap);
			this.emitHbaseMap(ChannelTopology.HBASE_STREAM, this.collector, this.hbaseMap);
			this.channelCountMap.clear();
			this.tsdbMap.clear();
			this.hbaseMap.clear();
			logger.info("After clean , size is  : " + this.tsdbMap.size() + "-" + this.hbaseMap.size() + "-"
					+ this.channelCountMap.size());
			logger.info("clean maps successful cost : " + (System.currentTimeMillis()-start));
		}
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.channelCountMap = new HashMap<String, Long>();
		this.tsdbMap = new HashMap<String, Long>();
		this.hbaseMap = new HashMap<String, Map<String,String>>();
		this.timestamp = System.currentTimeMillis()/1000/300+1;
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(ChannelTopology.TRANSFER_STREAM, 
				new Fields("channel","code","timestamp","num","ratio"));
		declarer.declareStream(ChannelTopology.OPENTSDB_STREAM, 
				new Fields("channel","code","timestamp","num","ratio"));	
		declarer.declareStream(ChannelTopology.HBASE_STREAM, 
				new Fields("rowkey","column","columnvalue"));
	}
	
	private boolean isNewTimeBucke(long timestamp){
		
		/**
		 * check if a new 5-min timestamp (has been discard)
		 */
		
		long tmp = System.currentTimeMillis()/1000/this.sendCheckFreq + 1;
		boolean b = tmp > timestamp ? true:false;
		return b;
	}
	
	private boolean saveMaps(Tuple tuple) {
		
		/**
		 *  this method save the last 5min info to two maps
		 *  hbaseMap : {channel-code : {device:count}} 
		 *  openstdb_map : {channel-timestamp-code: [count,total]}
		 */		
		
		//save hbase map
        String hbaseRowKey = tuple.getStringByField("channel") + "#" + this.timestamp + "#" + tuple.getStringByField("code");//row-key 
        String colKey = tuple.getStringByField("device");//column-key : device
        String colValue = tuple.getStringByField("count") +"#"+tuple.getStringByField("total")+"#"+tuple.getStringByField("ratio");// value :count # tot
        Map<String, String> colMap;//hbase-column-map  { "devices":"count#total#ratio"}
        if(this.hbaseMap.containsKey(hbaseRowKey))
            colMap = this.hbaseMap.get(hbaseRowKey);
        else
            colMap = new HashMap<String,String>();
        colMap.put(colKey, colValue);

        //keep top ten devices in hbaseMap，remove the min count device-key
        if(colMap.size()>10){
            int minCount = 99999999;
            String minKey = "imp-min";
            for (String key : colMap.keySet()){
                int deviceCount = Integer.valueOf(colMap.get(key).split("#")[0]);
                if(deviceCount < minCount)
                    minKey = key;
            }
            colMap.remove(minKey);
        }
        this.hbaseMap.put(hbaseRowKey, colMap);

		//save opentsdb map
		String tsdbKey = tuple.getStringByField("channel") + "#" +tuple.getStringByField("code") 
				+ "#" + this.timestamp;//key : channel-timestamp-code
		long values = Long.valueOf(tuple.getStringByField("count"));// value list ：this code count # all code count
		if (this.tsdbMap.containsKey(tsdbKey)){
			long lastValues = this.tsdbMap.get(tsdbKey);
			values += lastValues;//count
		}
		this.tsdbMap.put(tsdbKey, values);
		
		//save channel total count
		String code = tuple.getStringByField("code");
		if (code.contains("xx") && code != "9xx"){
			String channelKey = tuple.getStringByField("channel");
			Long count = this.channelCountMap.get(channelKey);
			long value = Integer.valueOf(tuple.getStringByField("count"));
			if (count == null){
				count = value;
			} else {
				count += value;
			}
			this.channelCountMap.put(channelKey, count);
		}
		
		return true;
	}
	
	private boolean emitTsdbMap(String tsdbStreamId, String transferStreamId, OutputCollector collector, 
			Map<String, Long> tsdbMap, Map<String, Long> channelCountMap){
		
		/**
		 *  emit date in opentsdb-map to write-opentsdb-bold as a stream
		 */
		
		for (String tsdbKey : tsdbMap.keySet()){
			String[] params = tsdbKey.split("#");
			String channel = params[0];
			String code = params[1];
			long timestamp = (Long.valueOf(params[2]))*300;
			
			Long total = channelCountMap.get(channel);
			if (total == null){
				logger.error("compute channel count wrong : " + channel);
				continue;
			}
			double num = (tsdbMap.get(tsdbKey));
			double ratio = num/total;
			
			//future : separate transfer stream from tsdb stream 
			collector.emit(tsdbStreamId, new Values(channel, code, timestamp, num, ratio));
			collector.emit(transferStreamId, new Values(channel, code, timestamp, num, ratio));
			logger.debug(channel + "-" + code + ":" + num + "-" + ratio);
		}
		return true;
	}
	
	private boolean emitHbaseMap(String streamId, OutputCollector collector,
			Map<String, Map<String,String>> hbaseMap){
		for (String rowKey : hbaseMap.keySet()){
            Map<String, String> rowMap = hbaseMap.get(rowKey);
            for (String column : rowMap.keySet()){
            	collector.emit(streamId, new Values(rowKey, column, rowMap.get(column)));
            	logger.debug("emit hbase date" + hbaseMap.toString());
            }
        }
		return true;	
	}

}
