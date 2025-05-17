package org.demir.workflows.workflow;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.demir.utils.Log;

public class WorkFlow {

    public static void run(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        Log.info("Starting workflow...");

        WorkFlowProducts.run(env, Tenv);

        WorkFlowCustomers.run(env, Tenv);

    }

}
