package org.demir.workflows.components.loaders;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.demir.utils.ConfigIceberg;
import org.demir.utils.Log;
import org.demir.utils.Params;
import org.demir.workflows.components.transformers.TransformerProducts;

public class LoaderProducts {


    public void run(StreamExecutionEnvironment env, StreamTableEnvironment Tenv) {

        Log.info("Write LoaderProducts ");


        TableLoader icebergTableLoader = ConfigIceberg.getTableLoader(Params.getParams().get("target-schema-0","dev"), Params.getParams().get("target-table-1", "products"));

        // Write to Iceberg table using Flink sink
        FlinkSink.forRowData(TransformerProducts.getProductsStream())
                .tableLoader(icebergTableLoader)
                .writeParallelism(1)
                .uidPrefix("loader_products_sink_" + System.currentTimeMillis())
                .set("write-format", "parquet")
                .set("format-version", "2")
                .append();


    }
}
