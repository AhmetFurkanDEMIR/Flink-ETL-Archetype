package org.demir.utils;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class ConfigKafka {

    private static String brokers = "kafka_brokers";
    private static Properties properties;
    private static String consumer_id;
    private static int parallelism=1;

    public static Properties createKafka(){

        if (Params.getParams().get("deploy-mode", "stage").equals("stage")) {
            setBrokers("kafka_brokers");
        }

        // Kafka konfigürasyonlarını ayarla
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getConsumer_id());

        return properties;
    }


    public static DataStream<String> getKafkaConsumerStream(String topic, StreamExecutionEnvironment env, String startup_modes){

        setProperties(createKafka());

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                getProperties()
        );

        if (startup_modes.equals("EARLIEST")) {
            kafkaConsumer.setStartFromEarliest();
        } else if (startup_modes.equals("LATEST")) {
            kafkaConsumer.setStartFromLatest();
        } else if (startup_modes.equals("GROUP_OFFSETS")) {
            kafkaConsumer.setStartFromGroupOffsets();
        } else if (startup_modes.equals("TIMESTAMP")) {
            //kafkaConsumer.setStartFromTimestamp();
        }

        DataStream<String> stream = env.addSource(kafkaConsumer).name(topic).setParallelism(parallelism);
        Log.info("Created Kafka consumer with properties: " + properties);

        return stream;

    }

    public static String getBrokers() {
        return brokers;
    }

    public static void setBrokers(String brokers) {
        ConfigKafka.brokers = brokers;
    }

    public static Properties getProperties() {
        return properties;
    }

    public static void setProperties(Properties properties) {
        ConfigKafka.properties = properties;
    }

    public static int getParallelism() {
        return parallelism;
    }

    public static void setParallelism(int parallelism) {
        ConfigKafka.parallelism = parallelism;
    }

    public static String getConsumer_id() {
        return consumer_id;
    }

    public static void setConsumer_id(String consumer_id) {
        String consumer_id_mode = Params.getParams().get("deploy-mode")+"-"+consumer_id;
        ConfigKafka.consumer_id = consumer_id_mode;
        Log.info("Consumer ID: " + consumer_id_mode);
    }
}
