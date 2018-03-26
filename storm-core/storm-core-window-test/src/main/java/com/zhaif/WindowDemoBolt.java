package com.zhaif;

import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class WindowDemoBolt extends BaseWindowedBolt {

	private static final long serialVersionUID = 1L;

	private List<Tuple> newTuples;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
			OutputCollector collector) {
	}

	public void execute(TupleWindow inputWindow) {

		System.out.println("start time :" + System.currentTimeMillis());

		newTuples = inputWindow.getExpired();
		for (Tuple tuple : newTuples) {
			System.out.println("expired : " + tuple.toString());
		}

		newTuples = inputWindow.get();

		for (Tuple tuple : newTuples) {
			System.out.println("get : " + tuple.getLongByField("time"));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}