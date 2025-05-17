package org.demir.workflows.workflow;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.demir.utils.Log;
import org.demir.workflows.components.extractors.ExtractorCustomers;
import org.demir.workflows.components.extractors.ExtractorProducts;
import org.demir.workflows.components.loaders.LoaderCustomers;
import org.demir.workflows.components.transformers.TransformerCustomers;

public class WorkFlowCustomers {

    public static void run(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        extractors(env, Tenv);
        transformers(env, Tenv);
        loaders(env, Tenv);

    }


    public static void extractors(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        Log.info("Starting extractors");
        ExtractorCustomers extractor = new ExtractorCustomers();
        extractor.run(env, Tenv);

    }

    public static void transformers(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        Log.info("Starting transformers");
        TransformerCustomers transformer = new TransformerCustomers();
        transformer.run(env, Tenv);

    }

    public static void loaders(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        Log.info("Starting loaders");
        LoaderCustomers loader = new LoaderCustomers();
        loader.run(env, Tenv);

    }


}





