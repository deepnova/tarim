package org.deepexi.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.deepexi.TarimDbAdapt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


public class LookupSourceFunction<OUT> extends RichSourceFunction<OUT> {
    private static final Logger logger = LoggerFactory.getLogger(LookupSourceFunction.class);
    private transient TypeSerializer<OUT> serializer;
    private volatile boolean isRunning = true;
    private TypeInformation<OUT> typeInfo;

    private Schema schema;
    private int tableID;
    private String PartitionKey;
    private String primaryKey;

    private String host;
    private int port;
    private TarimDbAdapt tarimDbAdapt;
    private String schemaJson;
    private transient RowDataWrapper wrapper;

    public LookupSourceFunction(Schema schema, int tableID, String schemaJson,
                                String PartitionKey, String primaryKey, TypeInformation<OUT> typeInfo,
                                String host, int port, TarimDbAdapt tarimDbAdapt){

        this.schema = schema;
        this.typeInfo = typeInfo;
        this.tableID = tableID;
        this.PartitionKey = PartitionKey;
        this.primaryKey = primaryKey;
        this.host = host;
        this.port = port;
        this.schemaJson = schemaJson;
        this.tarimDbAdapt = tarimDbAdapt;
        //serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
    }

    public void open(Configuration parameters) throws Exception {
        //StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

        serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
        this.wrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
    }

    @Override
    public void run(SourceContext<OUT> sourceContext) throws Exception {

        int i = 0;
        OUT nextElement = serializer.createInstance();
        while (isRunning) {

            isRunning = false;

            Tuple2<RowData, Integer> result = tarimDbAdapt.lookupData(tableID, FlinkSchemaUtil.convert(schema), schemaJson,
                    PartitionKey, primaryKey, host, port);

            if (result._2() != 0){
                logger.error("get lookup data error!!");
                break;
            }

            nextElement = (OUT) result._1();
            sourceContext.collect((OUT) nextElement);
            isRunning = false;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
