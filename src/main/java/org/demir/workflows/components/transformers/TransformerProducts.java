package org.demir.workflows.components.transformers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.demir.workflows.components.extractors.ExtractorProducts;
import org.json.JSONObject;
import org.demir.utils.Log;
import org.demir.utils.ParseTimestamp;

import java.time.LocalDateTime;


public class TransformerProducts {

    private static DataStream<RowData> ProductsStream;

    public void run(StreamExecutionEnvironment env, StreamTableEnvironment Tenv) {

        Log.info("TransformerProducts started");

        DataStream<String> rawStream = ExtractorProducts.getStreamProducts();

        ProductsStream = rawStream
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String jsonStr) throws Exception {
                        // JSON parse işlemi
                        JSONObject json = new JSONObject(jsonStr);

                        // Toplam 5 kolom
                        GenericRowData row = new GenericRowData(RowKind.INSERT, 5);

                        row.setField(0, json.getInt("product_id"));

                        row.setField(1, StringData.fromString(json.getString("product_name")));

                        row.setField(2, json.getInt("product_price"));


                        String createdOn = json.getString("created_on");
                        row.setField(3, ParseTimestamp.parseTimestamp(createdOn));

                        // loaded_at (Timestamp) - mevcut zaman
                        row.setField(4, TimestampData.fromLocalDateTime(LocalDateTime.now()));

                        return row;
                    }
                }).uid("transformer_Products_" + System.currentTimeMillis());


    }

    public static DataStream<RowData> getProductsStream() {
        return ProductsStream;
    }

    public static void setProductsStream(DataStream<RowData> processedStreamFinal) {
        TransformerProducts.ProductsStream = processedStreamFinal;
    }
}