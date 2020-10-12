package com.microsoft.example.SparkSStream;

import io.netty.handler.timeout.TimeoutException;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.kafka010.*;

import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class SparkSStream {

    private static transient Logger log = LoggerFactory.getLogger(
            SparkSStream.class);




    public static void main(String [] args) throws StreamingQueryException {

        Serdes deserialize = new Serdes();

        String brokerList = args[0];

        SparkSession spark = SparkSession
                .builder()
                //.master("yarn")
                .appName("SSSKafka")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokerList)
                .option("subscribe", "test")
                .option("startingOffsets", "earliest")
                //.option("endingOffsets", "latest")
                .load();



        System.out.println("========================SHOWING THE DATASET HERE!! =========================\n\n\n");
        Dataset<Row> readableDF = lines.selectExpr("CAST(value AS STRING)");
        System.out.println("============================================================================\n\n\n\n");

        readableDF.printSchema();
        //readableDF.show();

        StreamingQuery df = lines
                .writeStream()
                .outputMode("update")
                //.outputMode("complete")
                .format("console")
                //.trigger(Trigger.Continuous("5 seconds"))
                .start();

        log.debug("-> start()");
        try {
            df.awaitTermination();
        } catch (StreamingQueryException e) {
            log.error("Exception while waiting for query to end {}.", e
                            .getMessage(),
                    e);
        }




    }

}
