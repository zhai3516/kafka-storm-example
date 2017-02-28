package com.cc.imp;

import com.cc.util.TopologyConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.googlecode.jsonrpc4j.JsonRpcClient;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class SendTransferBolt extends BaseRichBolt {

	/**
	 * Class that send data to falcon-transfer for alarming. Data structure is
	 * same to the data write to opentsdb.
	 */

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(SendTransferBolt.class);

	private String tsdbCountMetric;
	private String tsdbRatioMetric;
	private String transferIp;
	private String transferPort;

	private static Socket socket;
	private static JsonRpcClient client;
	private static OutputStream ops;

	public SendTransferBolt() {
		// using default configuration which is not update by command
		this(TopologyConfig.tsdbCountMetric, TopologyConfig.tsdbRatioMetric, TopologyConfig.transferIp,
				TopologyConfig.transferPort);
	}

	public SendTransferBolt(String tsdbCountMetric, String tsdbRatioMetric, String transferIp, String transferPort) {
		// using configuration which has been updated by command
		this.tsdbCountMetric = tsdbCountMetric;
		this.tsdbRatioMetric = tsdbRatioMetric;
		this.transferIp = transferIp;
		this.transferPort = transferPort;
	}

	public void execute(Tuple tuple) {
		JSONObject jsonCount = new JSONObject();
		jsonCount.put("metric", tsdbCountMetric);
		jsonCount.put("channel", tuple.getStringByField("channel"));
		jsonCount.put("code", tuple.getStringByField("code"));
		jsonCount.put("endpoint", tuple.getStringByField("channel"));
		jsonCount.put("value", String.valueOf(tuple.getDoubleByField("num")));

		JSONObject jsonRatio = new JSONObject();
		jsonRatio.put("metric", tsdbRatioMetric);
		jsonRatio.put("channel", tuple.getStringByField("channel"));
		jsonRatio.put("code", tuple.getStringByField("code"));
		jsonRatio.put("endpoint", tuple.getStringByField("channel"));
		jsonRatio.put("value", String.valueOf(tuple.getDoubleByField("ratio")));
		try {
			client.invoke("Transfer.UpdateStrom", new Object[] { jsonCount.toString() }, ops);
			client.invoke("Transfer.UpdateStrom", new Object[] { jsonRatio.toString() }, ops);
			logger.debug(jsonCount.toString());
			logger.debug(jsonRatio.toString());

			// String reply =
			// client.invokeAndReadResponse("Transfer.UpdateStrom",
			// new Object[]{jsonCount.toString()}, String.class, ops, ips);
			// logger.info(reply);
			//
			// reply = client.invokeAndReadResponse("Transfer.UpdateStrom",
			// new Object[]{jsonRatio.toString()}, String.class, ops, ips);
			// logger.info(reply);
		} catch (Throwable e) {
			if (initSocketConnection(this.transferIp, Integer.valueOf(this.transferPort))) {
				try {
					client.invoke("Transfer.UpdateStrom", new Object[] { jsonCount.toString() }, ops);
					client.invoke("Transfer.UpdateStrom", new Object[] { jsonRatio.toString() }, ops);
				} catch (IOException e1) {
					logger.error("send json-rpc msg wrong\n", e1);
				}
			}
			logger.error("socket reconnection failed !");
			logger.error("drop message : " + "\n" + jsonCount.toString() + "\n" + jsonCount.toString());
		}
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		initSocketConnection(this.transferIp, Integer.valueOf(this.transferPort));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	private boolean initSocketConnection(String ip, int port) {
		return initSocketConnection(ip, port, 5);
	}

	private boolean initSocketConnection(String ip, int port, int retry) {

		/**
		 * create json-rpc client which establish connection with target port
		 * Input : ip: target ip port: target port retry: retry times if
		 * established faild. this parameter should between (0,10), or retry=10.
		 * default is 5.
		 */

		if (retry >= 10 || retry <= 0)
			retry = 10;
		int i = 0;
		while (i < retry) {
			try {
				socket = new Socket(ip, port);
				client = new JsonRpcClient();
				ops = socket.getOutputStream();
				break;
			} catch (IOException e) {
				i++;
				logger.info("init failed, retry times : " + i);
				logger.error("init json-rpc client error \n", e);
			}
			if (i == retry) {
				logger.error("init json-rpc failed, retry " + retry);
				return false;
			}
		}
		return true;
	}

}
