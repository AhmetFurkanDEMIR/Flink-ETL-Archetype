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
import org.demir.workflows.components.extractors.ExtractorCustomers;
import org.json.JSONObject;
import org.demir.utils.Log;

import java.time.LocalDateTime;


public class TransformerCustomers {

    private static DataStream<RowData> customersStream;

    public void run(StreamExecutionEnvironment env, StreamTableEnvironment Tenv) {

        Log.info("TransformerCustomers started");

        DataStream<String> rawStream = ExtractorCustomers.getStreamCustomers();

        customersStream = rawStream
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String jsonStr) throws Exception {
                        JSONObject json = new JSONObject(jsonStr);
                        GenericRowData row = new GenericRowData(RowKind.INSERT, 6);

                        // 0 - Id
                        row.setField(0, json.getInt("id"));

                        // 1 - Name
                        row.setField(0, StringData.fromString(json.getString("name")));

                        // 2 - SurName
                        row.setField(0, StringData.fromString(json.getString("surname")));

                        // 3 - Region
                        int channelId = json.getInt("region");
                        String channelNameStr;
                        switch (channelId) {
                            case 0: channelNameStr = "Ege"; break;
                            case 1: channelNameStr = "İç Anadolu"; break;
                            case 2: channelNameStr = "Doğu Anadolu"; break;
                            case 3: channelNameStr = "Güney Doğu Anadolu"; break;
                            case 4: channelNameStr = "Marmara"; break;
                            case 5: channelNameStr = "Karadeniz"; break;
                            default: channelNameStr = "";
                        }

                        // 4 - registration date (Timestamp)
                        long createEpochSec = json.getLong("registration_date");
                        row.setField(4, TimestampData.fromEpochMillis(createEpochSec));

                        // 5 - loaded_at (şu anki zaman)
                        row.setField(5, TimestampData.fromLocalDateTime(LocalDateTime.now()));

                        return row;
                    }
                }).uid("transformer_customers_map_" + System.currentTimeMillis());


    }

    public static DataStream<RowData> getCustomersStream() {
        return customersStream;
    }

    public static void setCustomersStream(DataStream<RowData> processedStreamFinal) {
        TransformerCustomers.customersStream = processedStreamFinal;
    }
}