package org.demir;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.demir.workflows.workflow.WorkFlow;

public class WorkFlowManager {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment Tenv;

    public WorkFlowManager(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){
        setEnv(env);
        setTenv(Tenv);
    }

    public void manageWorkFlow() {

        WorkFlow.run(env, Tenv);

    }

    public StreamExecutionEnvironment getEnv() {
        return env;
    }

    public StreamTableEnvironment getTenv() {
        return Tenv;
    }

    public void setTenv(StreamTableEnvironment tenv) {
        Tenv = tenv;
    }

    public void setEnv(StreamExecutionEnvironment env) {
        this.env = env;
    }
}
