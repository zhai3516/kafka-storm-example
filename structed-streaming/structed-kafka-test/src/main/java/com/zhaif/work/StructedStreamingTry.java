package com.zhaif.work;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.Column;

public class StructedStreamingTry {

    public static void structedTry() throws StreamingQueryException {

        String topic = "test_spark";
        Column value = new Column("value");
        StructType type = new StructType().add("request.host", "string").add("request.url", "string").add("time",
                "long");
        Encoder<MirrorRequest> mirrorEncoder = Encoders.bean(MirrorRequest.class);

        SparkConf conf = new SparkConf().setAppName("StructedTest").setMaster("local");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        // define kafka source
        Dataset<Row> df = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                .option("subscribe", topic)
                .load();

        // set kafka source data schema
        Dataset<MirrorRequest> requests = df
                .selectExpr("CAST(value AS STRING)")
                .select("value")
                .select(functions.from_json(value, type).as("data"))
                .select("data.*")
                .map((MapFunction<Row, MirrorRequest>) data -> {
                    if (data.size() == 3)
                        // use event time
                        return new MirrorRequest(data.getString(0), data.getString(1), data.getLong(2) * 1000);
                    return new MirrorRequest("other", "other");
                }, mirrorEncoder);
        // requests.printSchema();
        // host, path, timeStamp

        // count host count by window
        Dataset<Row> result = requests
                .withWatermark("timeStamp", "10 seconds")
                .groupBy(functions.window(requests.col("timeStamp"), "1 minutes", "1 minutes"), requests.col("host"))
                .count()
                .select("window.start", "window.end", "host", "count");
        // result.printSchema();
        // start, end, host, count

        // format dato to json which kafka need!
        StreamingQuery ds = result
                .select(functions.to_json(functions.struct(result.col("end"), result.col("host"), result.col("count")))
                        .alias("value"))
                .writeStream()
                .outputMode("update")
                // .format("console")
                .format("kafka")
                .option("checkpointLocation", "/Users/zhaif/work/test_data/")
                .option("kafka.bootstrap.servers", "127.0.0.1:9092")
                .option("topic", "test_output")
                .start();

        ds.awaitTermination();

    }
}
