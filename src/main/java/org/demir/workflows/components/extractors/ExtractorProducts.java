package org.demir.workflows.components.extractors;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.demir.utils.ConfigKafka;
import org.demir.utils.Log;
import org.demir.utils.Params;

public class ExtractorProducts {

    private static DataStream<String> streamProducts;

    public void run(StreamExecutionEnvironment env, StreamTableEnvironment Tenv) {

        Log.info("ExtractorProducts started");

        ConfigKafka.setConsumer_id("flink-kafka-sink-2025-05-16");

        setStreamProducts(ConfigKafka.getKafkaConsumerStream(Params.getParams().get("data-source-1","products"), env, "GROUP_OFFSETS"));

    }

    public static DataStream<String> getStreamProducts() {
        return streamProducts;
    }

    public static void setStreamProducts(DataStream<String> streamProducts) {
        ExtractorProducts.streamProducts = streamProducts;
    }
}
