package org.deepexi.sink;

import com.deepexi.TarimMetaClient;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.flink.calcite.shaded.org.apache.commons.codec.digest.MurmurHash3;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.deepexi.TarimDbClient;
import org.deepexi.WResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.deepexi.KvNode;

import static org.deepexi.TarimUtil.serialize;

public class TaskWriter implements TarimTaskWriter<RowData> {
    private int tableId;
    private int parallelism;
    private Schema icebergSchema;
    private PartitionSpec partitionSpec;
    private PartitionKey partitionKey;

    private List<byte[]> completeList = new ArrayList<>();
    private Map<String, DataWriter> writers = Maps.newHashMap();

    private org.apache.avro.Schema avroSchema;
    private RowDataWrapper wrapper;
    private RowDataToAvroConverters.RowDataToAvroConverter converter;
    private List<byte[]> dataList = new ArrayList<>();
    private static final Logger LOG = LoggerFactory.getLogger(TaskWriter.class);
    private String primaryKey;

    private TarimMetaClient metaClient;

    public TaskWriter(int tableId, String primaryKey, RowType flinkSchema, int parallelism, Schema icebergSchema, PartitionSpec partitionSpec, String schemaJson, TarimMetaClient metaClient){
        this.tableId = tableId;
        this.primaryKey = primaryKey;
        this.parallelism = parallelism;
        this.icebergSchema = icebergSchema;
        this.partitionSpec = partitionSpec;
        this.partitionKey = new PartitionKey(partitionSpec, icebergSchema);
        this.avroSchema = new org.apache.avro.Schema.Parser().parse(schemaJson);
        this.converter = RowDataToAvroConverters.createConverter(flinkSchema);
        this.wrapper = new RowDataWrapper(flinkSchema, icebergSchema.asStruct());
        this.metaClient = metaClient;
    }


    @Override
    public void write(RowData row) throws IOException {
        DataWriter writer = getWriterByPartitionData(row);

        RowKind rowKind = row.getRowKind();
        switch (rowKind) {
            case INSERT:
            case UPDATE_AFTER:
                writer.write(row);
                break;
            case DELETE:
            case UPDATE_BEFORE:
            default:
                break;
        }
    }

    @Override
    public WResult complete() throws IOException {

        writers.forEach((key,value)->{
            try {
                value.writeByCheckpoint();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return new WResult(completeList);
    }

    DataWriter getWriterByPartitionData(RowData row){

        partitionKey.partition(wrapper.wrap(row));
        String partitionID = partitionSpec.partitionToPath(partitionKey);
        System.out.println("chunk:" + partitionID);

        //todo get the port and host from data distribution by "partitionForChunk"
        //tmp for test
        final int port = 1303;
        final String host = "127.0.0.1";
        DataWriter writer = writers.get(partitionID);
        if (writer == null){

            long chunkID = partitionID.hashCode() & 0x00000000FFFFFFFFL;
            long hash = MurmurHash3.hash32(chunkID) & 0x00000000FFFFFFFFL;
            KvNode node = metaClient.getMasterReplicaNode(hash);

            writer = new DataWriter(node.host, node.port, partitionID);
            writers.put(partitionID, writer);
        }

        return writer;
    }
    @Override
    public void close() throws IOException {
        if (!writers.isEmpty()) {
            for (String key : writers.keySet()) {
                writers.get(key).close();
            }
            writers.clear();
        }
    }

    public class DataWriter implements Closeable {
        TarimDbClient client;
        int batchNum = 10; //tmp
        int dataIndex = 0;
        private String partitionID;
        ByteArrayOutputStream bytesOS = new ByteArrayOutputStream();
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(bytesOS, null); // or binaryEncoder() to create BufferedBinaryEncoder
        DatumWriter datumWriter = new GenericDatumWriter(avroSchema);

        private String host;
        private int port;
        public DataWriter(String host, int port, String partitionID){
            this.host = host;
            this.port = port;
            this.client = new TarimDbClient(host, port);
            this.partitionID = partitionID;

        }

        public void write(RowData row) throws IOException {
            Object record = converter.convert(avroSchema, row);
            datumWriter.write(record, encoder);

            int size = bytesOS.size();
            System.out.println("size:" + size);

            dataIndex++;
            if (dataIndex >= batchNum){

                byte[] recordByte = bytesOS.toByteArray();
                int result = client.insertRequest(tableId, partitionID, recordByte, primaryKey);
                if (result != 0){
                    LOG.error("write, send the data fail! tableId={}, partitionID={}", tableId, partitionID);
                    throw new RuntimeException();
                }

                CheckPointData data = new CheckPointData(host, port, partitionID, recordByte);
                dataList.add(serialize(data));
                bytesOS.reset();
                dataIndex = 0;
            }
        }
        public void writeByCheckpoint() throws IOException {
            if (bytesOS.size() > 0){

                byte[] recordByte = bytesOS.toByteArray();
                int result = client.insertRequest(tableId, partitionID, recordByte, primaryKey);
                if (result != 0){
                    LOG.error("Checkpoint send the data fail! tableId={}, partitionID={}", tableId, partitionID);
                    throw new RuntimeException();
                }
                CheckPointData data = new CheckPointData(host, port, partitionID, recordByte);
                dataList.add(serialize(data));

                bytesOS.reset();
                dataIndex = 0;
            }

            if (dataList.size() > 0) {
                completeList.addAll(dataList);
                dataList.clear();
            }

        }
        @Override
        public void close() throws IOException {
            dataList.clear();
            dataIndex = 0;
            bytesOS.reset();
        }
    }
}
