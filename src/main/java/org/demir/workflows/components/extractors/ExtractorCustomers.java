package org.demir.workflows.components.extractors;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.demir.utils.ConfigKafka;
import org.demir.utils.Log;
import org.demir.utils.Params;

public class ExtractorCustomers {

    private static DataStream<String> streamCustomers;

    public void run(StreamExecutionEnvironment env, StreamTableEnvironment Tenv) {

        Log.info("ExtractorCustomers started");

        ConfigKafka.setConsumer_id("flink-kafka-sink-2025-05-16");

        setStreamCustomers(ConfigKafka.getKafkaConsumerStream(Params.getParams().get("data-source-0","customers"), env, "GROUP_OFFSETS"));

    }

    public static DataStream<String> getStreamCustomers() {
        return streamCustomers;
    }

    public static void setStreamCustomers(DataStream<String> streamCustomers) {
        ExtractorCustomers.streamCustomers = streamCustomers;
    }
}
