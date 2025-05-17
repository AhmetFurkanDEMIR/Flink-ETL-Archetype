package org.demir.workflows.workflow;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.demir.utils.Log;
import org.demir.workflows.components.extractors.ExtractorProducts;
import org.demir.workflows.components.loaders.LoaderProducts;
import org.demir.workflows.components.transformers.TransformerProducts;

public class WorkFlowProducts {

    public static void run(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        extractors(env, Tenv);
        transformers(env, Tenv);
        loaders(env, Tenv);

    }

    public static void extractors(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        Log.info("Starting extractors");
        ExtractorProducts extractor = new ExtractorProducts();
        extractor.run(env, Tenv);

    }

    public static void transformers(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        Log.info("Starting transformers");
        TransformerProducts transformer = new TransformerProducts();
        transformer.run(env, Tenv);

    }

    public static void loaders(StreamExecutionEnvironment env, StreamTableEnvironment Tenv){

        Log.info("Starting loaders");
        LoaderProducts loader = new LoaderProducts();
        loader.run(env, Tenv);

    }


}





