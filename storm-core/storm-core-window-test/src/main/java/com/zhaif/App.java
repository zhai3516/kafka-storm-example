package com.zhaif;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args)
			throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		RandomSpout r = new RandomSpout();

		// Sliding window used to test withLag
		// BaseWindowedBolt b = new WindowDemoBolt().withTimestampField("time")
		// .withWindow(new Duration(5, TimeUnit.SECONDS), new Duration(3,
		// TimeUnit.SECONDS))
		// // When get tuple with timestamp
		// // t1(10:00:10)，t2(10:00:14)，t3(10:00:12)，t4(10:00:16) in turn,
		// // If we won't set withLag, storm would throw t3 away. But if
		// // setted and (lag + t3) > t2, t3 will not be dropped.
		// .withLag(new Duration(2, TimeUnit.SECONDS));

		// Sliding window used to test watermark
		BaseWindowedBolt b = new WindowDemoBolt().withTimestampField("time")
				.withWindow(new Duration(20, TimeUnit.SECONDS), new Duration(10, TimeUnit.SECONDS))
				.withLag(new Duration(5, TimeUnit.SECONDS));

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", r);
		builder.setBolt("window-bolt", b).shuffleGrouping("spout");

		Config conf = new Config();
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Thread.sleep(3600000L);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology("test-window", conf, builder.createTopology());
		}
	}
}
