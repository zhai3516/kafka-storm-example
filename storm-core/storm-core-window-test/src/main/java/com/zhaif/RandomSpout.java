package com.zhaif;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class RandomSpout extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	public void nextTuple() {
		// TODO Auto-generated method stub
		int i = 0;

		// Code for test withLag
		// while (i < 10) {
		// long t = 0L;
		// if (i % 3 == 0)
		// t = System.currentTimeMillis() - 2000L; //
		// else
		// t = System.currentTimeMillis();
		//
		// collector.emit(new Values(t));
		// System.out.println("Emit : " + t);
		//
		// i++;
		// try {
		// Thread.sleep(1000L);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// }

		// Code for test watermark
		collector.emit(new Values(1488301715440L));
		collector.emit(new Values(1488301725440L));
		collector.emit(new Values(1488301735440L));
		collector.emit(new Values(1488301745440L));
		collector.emit(new Values(1488301755440L));
		collector.emit(new Values(1488301765440L));
		collector.emit(new Values(1488301775440L));

		while (i++ < 10) {
			collector.emit(new Values(System.currentTimeMillis()));
			try {
				Thread.sleep(6000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("time"));
	}

}
