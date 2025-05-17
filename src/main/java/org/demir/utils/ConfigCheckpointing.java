package org.demir.utils;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class ConfigCheckpointing {

    private static String project_name;

    public static void setCheckpointg(StreamExecutionEnvironment env) throws IOException {

        env.enableCheckpointing(10000); // 10 saniyede bir checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000); // 5 saniye ara
        env.getCheckpointConfig().setCheckpointTimeout(60000); // 1 dakika timeout
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        Log.info("Checkpoint config set");


    }

    public static void setStateBackend(StreamExecutionEnvironment env) throws IOException {

        if(Params.getParams().get("deploy-mode","stgage").equals("local")){
            Log.warn("deploy-mode is local. No State backend");
        }
        else {
            String RocksDB_path =  "s3a://flink/checkpoints/"+Params.getParams().get("deploy-mode", "stage")+"-"+getProject_name();

            Log.info("RocksDB_path: "+ RocksDB_path);
            env.setStateBackend(new RocksDBStateBackend(RocksDB_path, true));

            Log.info("State backend set");
        }
    }

    public static String getProject_name() {
        return project_name;
    }

    public static void setProject_name(String project_name) {
        ConfigCheckpointing.project_name = project_name;
    }

}