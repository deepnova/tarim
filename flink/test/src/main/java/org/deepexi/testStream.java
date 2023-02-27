package org.deepexi;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class testStream {
    final static Logger logger = LoggerFactory.getLogger(testMain.class);

    public static void main(String[] args) throws Exception {
        try {

            EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .useBlinkPlanner()
                    .inStreamingMode()
                    .build();

            StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            bsEnv.setParallelism(1);
            bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            bsEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            bsEnv.enableCheckpointing(20000);
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, settings);

            String CATALOG_NAME = "tarim_catalog";
            String DATABASE_NAME = "tarim_db";
            //Map<String, String> properties = new HashMap<>();
            //properties.put("warehouse", "hdfs://10.201.0.82:9000/wpf0220");
            //CatalogLoader loader = CatalogLoader.hadoop(CATALOG_NAME, new Configuration(), properties);
            FlinkTarimCatalog flinkCatalog = new FlinkTarimCatalog(CATALOG_NAME, DATABASE_NAME, org.apache.iceberg.catalog.Namespace.empty());
            tableEnv.registerCatalog(CATALOG_NAME, flinkCatalog);

            logger.info("test...");
            String ddl1 = "CREATE TABLE source_table ( \n" +
                    "    user_id INT,\n" +
                    "    test_id INT" +
                    " )WITH (\n" +
                    "'connector' = 'datagen',\n" +
                    "'rows-per-second'='1',\n" +
                    "'fields.user_id.kind'='random',\n" +
                    "'fields.user_id.min'='1',\n" +
                    "'fields.user_id.max'='10',\n" +
                    "'fields.test_id.kind'='random',\n" +
                    "'fields.test_id.min'='1',\n" +
                    "'fields.test_id.max'='10'\n" +
                    ")";
            TableResult tableResult = tableEnv.executeSql(ddl1);
            //tableEnv.executeSql("select * from source_table").print();
            //tableEnv.useCatalog(CATALOG_NAME);
            //tableEnv.useDatabase(DATABASE_NAME);

            ObjectPath tablePath = new ObjectPath(DATABASE_NAME, "new_table1");
            boolean b = flinkCatalog.tableExists(tablePath);
            if (!b) {
                tableEnv.executeSql("CREATE TABLE `tarim_table1` (\n" +
                        //"date TIMESTAMP(3),\n" +
                        "user_id int,\n" +
                        "test_id int\n" +
                        // "WATERMARK FOR user_action_time AS user_action_time - INTERVAL '5' SECOND\n" +
                        ")").print();
            }else{
                tableEnv.executeSql("insert into tarim_catalog.tarim_db.tarim_table1 select * from source_table");
            }
            System.out.println(b);
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            System.out.println("=========结束=========");
        }

    }
}
