package com.cc.imp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.cc.util.TopologyConfig;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaDataSpout extends BaseRichSpout {

	/**
	 *  storm spout that consume date from kafka cluster
	 *  
	 */
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(KafkaDataSpout.class);
	private ConcurrentHashMap<UUID, Values> pending;

	private ConsumerConnector conn;
	private SpoutOutputCollector collector;
	private String kafkaTopic;
	private String kafkaZookeeperAddr;
	private String kafkaGroup;
	
	public KafkaDataSpout(){
		//using default configuration which is not update by command
		this(TopologyConfig.kafkaTopic, TopologyConfig.kafkaZookeeperAddr, TopologyConfig.kafkaGroup);
	}
	
	public KafkaDataSpout(String kafkaTopic, String kafkaZookeeperAddr, String kafkaGroup){
		//using configuration which has been updated by command
		this.kafkaTopic = kafkaTopic;
		this.kafkaZookeeperAddr = kafkaZookeeperAddr;
		this.kafkaGroup = kafkaGroup;
	}
	
	public void nextTuple() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TopologyConfig.kafkaTopic, 1);//one excutor - one thread
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = conn.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(kafkaTopic);
		ConsumerIterator<byte[], byte[]> iter = streams.get(0).iterator();
		while(true){
			while(iter.hasNext()){
				
				String s = new String(iter.next().message());
				collector.emit(new Values(s));
				 
				UUID msgId = UUID.randomUUID();
				this.pending.put(msgId, new Values(s));
			}
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				logger.error("Spout : sleep wrong \n", e);
			}
		}
	}

	public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
		ConsumerConfig conf = createConsumerConfig(kafkaZookeeperAddr, kafkaGroup);
		this.conn = Consumer.createJavaConsumerConnector(conf);
		this.collector = collector;
		this.pending = new ConcurrentHashMap<UUID, Values>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("log"));	
	}
	
	public void ack(Object msgId){
		this.pending.remove(msgId);
	}
	
	public void fail(Object msgId){
		
		logger.error(this.pending.get(msgId).toString() + " is loss !");
		this.pending.remove(msgId);
	}
	
	private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		
		/**
		 * this method used to set kafka-consumer configuration
		 * 
		 * Args :
		 * 	m_zookeeper: zookeeper address with port
		 * 	m_groupId  : kafka-consumer consumer group
		 * 
		 * Return :
		 * 	an object of ConnsumerConfig 
		 * 
		 */
		
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
}
