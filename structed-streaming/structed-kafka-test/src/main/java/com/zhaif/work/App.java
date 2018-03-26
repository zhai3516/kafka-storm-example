package com.zhaif.work;

import org.apache.spark.sql.streaming.StreamingQueryException;

public class App {
    public static void main(String[] args) throws StreamingQueryException {
        System.out.println("Hello World!");

        StructedStreamingTry.structedTry();
    }
}
