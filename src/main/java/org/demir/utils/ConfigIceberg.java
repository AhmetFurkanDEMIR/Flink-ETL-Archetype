package org.demir.utils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.apache.iceberg.flink.source.assigner.SimpleSplitAssignerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class ConfigIceberg {

    private static String catalogName="iceberg";
    private static String warehouse="s3://lakehause/";
    private static String uri="thrift://metastore_host:9083";
    private static CatalogLoader catalogLoader = getIcebergCatalogLoader();
    private static int parallelism=1;

    public static CatalogLoader getIcebergCatalogLoader() {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "iceberg");
        properties.put("catalog-type", "hive");
        properties.put("property-version", "1");
        properties.put("uri", uri);
        properties.put("warehouse", warehouse);

        CatalogLoader loader = CatalogLoader.hive(
                catalogName,
                new org.apache.hadoop.conf.Configuration(),
                properties
        );

        Log.info("Created IcebergCatalogLoader");

        return loader;
    }


    public static DataStream<RowData> getIcebergTableSource(String schema, String table, StreamExecutionEnvironment env) {


        IcebergSource source = IcebergSource.forRowData()
                .tableLoader(getTableLoader(schema, table))
                .assignerFactory(new SimpleSplitAssignerFactory())
                .streaming(true)
                .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
                .monitorInterval(Duration.ofSeconds(5))
                .build();

        DataStream<RowData> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                table,
                TypeInformation.of(RowData.class))
                .setParallelism(getParallelism());

        Log.info("Created IcebergTableSource");

        return stream;

    }

    public static TableLoader getTableLoader(String schema, String table){

        return TableLoader.fromCatalog(getCatalogLoader(),
                TableIdentifier.of(schema, table));
    }

    public static String getCatalogName() {
        return catalogName;
    }

    public static String getUri() {
        return uri;
    }

    public static String getWarehouse() {
        return warehouse;
    }

    public static void setCatalogName(String catalogName) {
        ConfigIceberg.catalogName = catalogName;
    }

    public static void setUri(String uri) {
        ConfigIceberg.uri = uri;
    }

    public static void setWarehouse(String warehouse) {
        ConfigIceberg.warehouse = warehouse;
    }

    public static CatalogLoader getCatalogLoader() {
        return catalogLoader;
    }

    public static void setCatalogLoader(CatalogLoader catalogLoader) {
        ConfigIceberg.catalogLoader = catalogLoader;
    }

    public static void setParallelism(int parallelism) {
        ConfigIceberg.parallelism = parallelism;
    }

    public static int getParallelism() {
        return parallelism;
    }
}
