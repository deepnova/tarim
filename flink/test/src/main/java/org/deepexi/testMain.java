package org.deepexi;

import jdk.nashorn.internal.runtime.Context;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class testMain {
    public static void main(String[] args) throws Exception {
        final Logger logger = LoggerFactory.getLogger(testMain.class);
        try {

            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inStreamingMode()
                    .build();

            StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            bsEnv.setParallelism(1);
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            bsEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            bsEnv.enableCheckpointing(120 * 1000);
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, settings);

            String CATALOG_NAME = "tarim_catalog";
            String DATABASE_NAME = "tarim_db";
            //Map<String, String> properties = new HashMap<>();
            //properties.put("warehouse", "hdfs://10.201.0.82:9000/wpf0220");
            //CatalogLoader loader = CatalogLoader.hadoop(CATALOG_NAME, new Configuration(), properties);

            FlinkTarimCatalog flinkCatalog = new FlinkTarimCatalog(CATALOG_NAME, DATABASE_NAME, org.apache.iceberg.catalog.Namespace.empty());
            tableEnv.registerCatalog(CATALOG_NAME, flinkCatalog);
            tableEnv.useCatalog(CATALOG_NAME);
            tableEnv.useDatabase(DATABASE_NAME);

            ObjectPath tablePath = new ObjectPath(DATABASE_NAME, "new_table1");
            boolean b = flinkCatalog.tableExists(tablePath);
            if (!b) {
                tableEnv.executeSql("CREATE TABLE `tarim_table1` (\n" +
                        //"date TIMESTAMP(3),\n" +
                        "currency STRING,\n" +
                        "userid int,\n" +
                        "test1 VARCHAR(1),\n" +
                        "PRIMARY KEY(userid) NOT ENFORCED\n" +
                        // "WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND\n" +
                        ")").print();
            }else{
                tableEnv.executeSql("insert into tarim_table1 values('d00',100, '100'), ('d01',101, '101'),('d02',102, '102')");
            }
            System.out.println(b);
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            System.out.println("=========结束=========");
        }
    }
}
