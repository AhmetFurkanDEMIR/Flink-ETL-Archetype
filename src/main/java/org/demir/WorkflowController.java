package org.demir;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.demir.utils.ConfigCheckpointing;
import org.demir.utils.Log;
import org.demir.utils.Params;
import java.io.IOException;
import static org.demir.utils.ConfigEnvTenv.getEnv;
import static org.demir.utils.ConfigEnvTenv.getTenv;
import static org.demir.utils.ConfigEnvTenv.setParallelism;

public class WorkflowController {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);

        // config Parallelism
        setParallelism(2);

        // Args
        Params.setParams(args);

        StreamExecutionEnvironment env = getEnv();
        StreamTableEnvironment Tenv = getTenv();

        WorkFlowManager manager = new WorkFlowManager(env, Tenv);
        manager.manageWorkFlow();

        // Project name config
        // s3a://flink/checkpoints/project_name
        ConfigCheckpointing.setProject_name("demir-kafka-sink");

        try {
            ConfigCheckpointing.setStateBackend(env);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Log.info(ConfigCheckpointing.getProject_name()+" Project Start.");

        env.execute(ConfigCheckpointing.getProject_name());

    }

}
