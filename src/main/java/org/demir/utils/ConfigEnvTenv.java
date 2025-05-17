package org.demir.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.configuration.Configuration;
import java.io.IOException;


public class ConfigEnvTenv {

    private static StreamExecutionEnvironment env;
    private static int parallelism;

    public static StreamExecutionEnvironment getEnv(){

        try {

            if (Params.getParams().get("deploy-mode", "stgae").equals("local")) {
                Configuration conf = new Configuration();
                setEnv(StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf));

            }

            else{
                setEnv(StreamExecutionEnvironment.getExecutionEnvironment());
            }

            env.setParallelism(getParallelism());
            ConfigCheckpointing.setCheckpointg(env);
        } catch (IOException e) {
            Log.error("getEnv Failed");
            throw new RuntimeException(e);
        }

        Log.info("ConfigEnvTenv set env");
        return env;

    }

    public static StreamTableEnvironment getTenv(){

        return StreamTableEnvironment.create(getEnv());
    }

    public static int getParallelism() {
        return parallelism;
    }

    public static void setParallelism(int parallelism) {
        ConfigEnvTenv.parallelism = parallelism;
    }

    public static void setEnv(StreamExecutionEnvironment env) {
        ConfigEnvTenv.env = env;
    }
}

