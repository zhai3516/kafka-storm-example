package com.cc.imp;

import com.cc.util.*;
import com.cc.util.CodeEncodeTool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class WriteOpenTSDBBolt extends BaseRichBolt {
	
    /** 
     *  class that write map info to OpenTSDB
     *  OpentTSDB structure :
     *  key1 : "imp.channel.code.ratio"
     *  key2 : "imp.channel.code.count"
     *  tags:
     *      code : code_name
     *      channel : channel_name
     *  value : code-ratio (float)
     */

	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(WriteOpenTSDBBolt.class);
	
	public String tsdbUrl = "http://127.0.0.1:4242/api/put?details";
	public String tsdbCountMetric = "imp.channel.code.count";
	public String tsdbRatioMetric = "imp.channel.code.ratio";

	public HttpPost post;
	public RequestConfig config;
	
	public WriteOpenTSDBBolt(){
		//using default configuration which is not update by command
		this(TopologyConfig.tsdbUrl, TopologyConfig.tsdbCountMetric, TopologyConfig.tsdbRatioMetric);
	}
	
	public WriteOpenTSDBBolt(String tsdbUrl, String tsdbCountMetric, String tsdbRatioMetric){
		//using configuration which has been updated by command
		this.tsdbUrl = tsdbUrl;
		this.tsdbCountMetric = tsdbCountMetric;
		this.tsdbRatioMetric = tsdbRatioMetric;
	}
	
	public void execute(Tuple tuple) {
		Long timestamp = tuple.getLongByField("timestamp");
		String channel = CodeEncodeTool.encoding(tuple.getStringByField("channel"));
		String code = tuple.getStringByField("code");
		double num = tuple.getDoubleByField("num");
		double ratio = tuple.getDoubleByField("ratio");
		Map<String,String> tags = new HashMap<String,String>();
		tags.put("code", code);
		tags.put("channel", channel);
		
		JSONObject jsonCount = new JSONObject();
		jsonCount.put("metric", tsdbCountMetric);
		jsonCount.put("timestamp",timestamp);  
		jsonCount.put("value",num);  
		jsonCount.put("tags",tags);	
		JSONObject jsonRatio = new JSONObject();
		jsonRatio.put("metric", tsdbRatioMetric);
		jsonRatio.put("timestamp",timestamp);  
		jsonRatio.put("value",ratio);  
		jsonRatio.put("tags",tags);	
		List<JSONObject> jsonBatch = new ArrayList<JSONObject>();
		jsonBatch.add(jsonCount);
		jsonBatch.add(jsonRatio);
		
		//future : cache date and send by batch 
	    this.writeTsdb(post, jsonBatch); 
	}

	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		post = new HttpPost(tsdbUrl);
		config = RequestConfig.custom().setSocketTimeout(5000).setConnectTimeout(5000).build();
	    //post.setConfig(config);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public void writeTsdb(HttpPost post, List<JSONObject> jsonParam){
		
		/**
		 *  post http-requests to opentsdb
		 *  params contains : metric time-stamp value tags
		 */
		
	    CloseableHttpResponse response = null;
	    StringEntity entity = new StringEntity(jsonParam.toString(),"utf-8");
	    entity.setContentType("application/json");
	    entity.setContentEncoding("UTF-8");
	    post.setEntity(entity); 
	    CloseableHttpClient tsdbClient = HttpClients.createDefault();	
	    
	    try {
	    	response = tsdbClient.execute(post);
			String responseData = EntityUtils.toString(response.getEntity());
			JSONObject responseJson = new JSONObject(responseData); 
			tsdbClient.close();
			logger.debug(responseJson.toString());
	    } catch (ParseException e) {
	    	logger.error("Write OpenTSDB Wrong {" + jsonParam.toString() + "}\n", e);
	    } catch (IOException e) {
	    	logger.error("Write OpenTSDB Wrong {" + jsonParam.toString() + "}\n", e );
	    }	
	}
	
}
