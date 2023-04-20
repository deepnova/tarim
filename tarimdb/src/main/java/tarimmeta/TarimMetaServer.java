package tarimmeta;

import com.deepexi.KvNode;
import com.deepexi.TarimMetaClient;
import com.deepexi.rpc.TarimExecutor;
import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimProto;
import com.deepexi.tarimdb.tarimkv.KeyValueCodec;
import com.deepexi.tarimdb.util.Common;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.deepexi.tarimdb.tarimkv.KeyValueCodec.schemaKeyEncode;
import static com.deepexi.tarimdb.util.Common.*;
import static org.apache.iceberg.expressions.Expressions.and;


public class TarimMetaServer {
    private final static Logger logger = LogManager.getLogger(TarimMetaServer.class);
    MetaKVClient kvClient;
    KVMetadata metadata;

    //the client to get the KV-meta-data by local, one meta node has only one localKVMetaClient
    private TarimMetaClient localKVMetaClient;

    public TarimMetaServer(KVMetadata metadata) {

        this.metadata = metadata;

        TarimKVProto.DistributionInfo distribution = metadata.toDistributionInfo(false);
        localKVMetaClient = new TarimMetaClient(distribution);
        localKVMetaClient.refreshDistribution(distribution);

        if (metadata.metaMode.equals("standalone")) {
            //local mode
            this.kvClient = new MetaKVLocal(metadata);

        } else if (metadata.metaMode.equals("remote")) {
            //remote mode
            //todo, only support standalone now,  host and  port are from the metadata in the remote mode.
            String host = "127.0.0.1";
            int port = 1302;
            this.kvClient = new MetaKVRemote(host, port);
        } else {
            throw new RuntimeException();
        }
    }

    public int setPartitionMsg(int tableID, String partitionID) {


        String key = schemaKeyEncode(tableID, partitionID);
        long chunkID = toChunkID(partitionID);

        logger.info("setPartitionMsg! key={}, chunkID={}", key, chunkID);
        kvClient.put(key, String.valueOf(chunkID));
        //if put fail, there should be exception
        return 0;
    }

    public TarimProto.PrepareScanResponse prepareScan(int tableID, boolean allFlag, TarimExecutor.ExecType execType, byte[] conditions, List<String> partitionIDs) {

        switch (execType) {
            case TypePartitionTableScan:
                return partitionTableScan(tableID, allFlag, conditions, partitionIDs);
            case TypeTableScan:
            case TypeIndexScan:
            case TypeSelection:
            case TypeAggregation:
            case TypeTopN:
            case TypeLimit:
            case TypeProjection:
            case TypeSort:
            case UNRECOGNIZED:
                logger.info("prepareScan execType=%d", execType.getNumber());

            default:
                throw new IllegalStateException("Unexpected value: " + execType);
        }

    }

    Evaluator changeConditionsToEval(byte[] conditions){
        String jsonString;

        //todo the json from the meta data
        try {
            jsonString = Common.loadTableMeta("partitionKey.json");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("The table meta is incorrect!");
        }

        Object object = deserialize(conditions);

        Expression expression;

        List list = (List)object;
        if (list.size() == 0){
            expression = (Expression) list.get(0);
        }else{
            expression = and((Expression) list.get(0), (Expression) list.get(1));
            for (int i = 2; i < list.size(); i++){
                expression = and(expression, (Expression) list.get(i));
            }
        }

        org.apache.iceberg.shaded.org.apache.avro.Schema avroSchema = new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(jsonString);
        Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Evaluator evaluator = new Evaluator(icebergSchema.asStruct(), expression);

        return evaluator;
    }

    TarimProto.PrepareScanResponse partitionTableScan(int tableID, boolean allFlag,  byte[] conditions, List<String> partitionIDs){
        List<TarimProto.Partition> partitionAll = new ArrayList<>();
        List<TarimProto.FileInfo> fileInfoList = new ArrayList<>();
        TarimProto.ScanInfo.Builder scanBuilder = TarimProto.ScanInfo.newBuilder();
        TarimProto.MainAccount.Builder accountBuilder = TarimProto.MainAccount.newBuilder();
        accountBuilder.setAccountType(2);
        accountBuilder.setToken("");
        accountBuilder.setUsername("");

        TarimProto.PrepareScanResponse.Builder responseBuilder = TarimProto.PrepareScanResponse.newBuilder();
        responseBuilder.setCode(0);
        responseBuilder.setMsg("OK");
        scanBuilder.setMainAccount(accountBuilder.build());
        //todo, get the partition key, the filepath and file info from the meta data
        String partitionKey = "class";
        List<String> filePath = new ArrayList<>();
        List<String> partitionList = new ArrayList<>();

        Evaluator evaluator = changeConditionsToEval(conditions);
        String prefix;
        if (allFlag){
            prefix = String.format("%d_" ,tableID);
        }else{
            prefix = schemaKeyEncode(tableID, partitionKey);
        }

        List<TarimKVProto.KeyValue> keyValues = kvClient.prefixSeek("default", prefix);
        for (TarimKVProto.KeyValue keyValue : keyValues) {
            boolean result = false;
            if (!allFlag) {
                String key[] = keyValue.getKey().split("=");
                if (key.length != 2){
                    logger.error("get partitionKey error!");
                    continue;
                }else{
                    result = evaluator.eval(Common.Row.of(key[1]));
                }

            }else{
                result = true;
            }

            if (result) {

                Long hash = chunkIDHash(Long.parseLong(keyValue.getValue()));
                KvNode node = localKVMetaClient.getMasterReplicaNode(hash);
                if (node == null){
                    throw new RuntimeException("the node is null!");
                }else{
                    TarimProto.Partition.Builder builder = TarimProto.Partition.newBuilder();

                    //TarimProto.FileInfo.Builder fileBuilder = TarimProto.FileInfo.newBuilder();
                    //todo get the file info from the meta
                    //fileBuilder.setFormat("parquet");
                    //fileInfoList.add(0, fileBuilder.build());

                    fileSimulate(fileInfoList);

                    builder.setPartitionID(keyValue.getKey());
                    builder.setMergePolicy(1);
                    builder.setHost(node.host);
                    builder.setPort(node.port);
                    builder.addAllFileInfo(fileInfoList);

                    partitionAll.add(builder.build());
                }
            }
        }

        scanBuilder.addAllPartitions(partitionAll);
        responseBuilder.setScanInfo(scanBuilder.build());
        return responseBuilder.build();
    }

    private void fileSimulate(List<TarimProto.FileInfo> fileInfoList){
        TarimProto.FileInfo.Builder fileBuilder1 = TarimProto.FileInfo.newBuilder();

        fileBuilder1.setFormat("PARQUET")
                .setRowCount(2)
                .setSizeInBytes(991)
                .setPath("hdfs://10.201.0.82:9000/wpf0220/default_db/new_table12/data/class=class7/00000-0-f7af0401-7dfb-4e21-b758-d7be09f1c790-00003.parquet")
                .putAllLowerBounds(Map.of(1,ByteString.copyFrom(new byte[]{1, 0, 0, 0}),
                        2, ByteString.copyFrom(new byte[]{10, 0, 0, 0}),
                        3, ByteString.copyFrom("class7".getBytes())))
                .putAllUpperBounds(Map.of(1,ByteString.copyFrom(new byte[]{5, 0, 0, 0}),
                        2, ByteString.copyFrom(new byte[]{50, 0, 0, 0}),
                        3, ByteString.copyFrom("class7".getBytes())));

        fileInfoList.add(fileBuilder1.build());

        TarimProto.FileInfo.Builder fileBuilder2 = TarimProto.FileInfo.newBuilder();

        fileBuilder2.setFormat("PARQUET")
                .setRowCount(1)
                .setSizeInBytes(932)
                .setPath("hdfs://10.201.0.82:9000/wpf0220/default_db/new_table12/data/class=class7/00000-0-f82200ee-38dd-4ec9-9b61-cb61c6438fdc-00003.parquet")
                .putAllLowerBounds(Map.of(1,ByteString.copyFrom(new byte[]{7, 0, 0, 0}),
                        2, ByteString.copyFrom(new byte[]{70, 0, 0, 0}),
                        3, ByteString.copyFrom("class7".getBytes())))
                .putAllUpperBounds(Map.of(1,ByteString.copyFrom(new byte[]{7, 0, 0, 0}),
                        2, ByteString.copyFrom(new byte[]{70, 0, 0, 0}),
                        3, ByteString.copyFrom("class7".getBytes())));

        fileInfoList.add(fileBuilder2.build());

        TarimProto.FileInfo.Builder fileBuilder3 = TarimProto.FileInfo.newBuilder();

        fileBuilder3.setFormat("PARQUET")
                .setRowCount(2)
                .setSizeInBytes(991)
                .setPath("hdfs://10.201.0.82:9000/wpf0220/default_db/new_table12/data/class=class7/00000-0-70fa3de1-57dd-47e6-b7ff-13e44f781e9f-00003.parquet")
                .putAllLowerBounds(Map.of(1,ByteString.copyFrom(new byte[]{9, 0, 0, 0}),
                        2, ByteString.copyFrom(new byte[]{90, 0, 0, 0}),
                        3, ByteString.copyFrom("class7".getBytes())))
                .putAllUpperBounds(Map.of(1,ByteString.copyFrom(new byte[]{10, 0, 0, 0}),
                        2, ByteString.copyFrom(new byte[]{100, 0, 0, 0}),
                        3, ByteString.copyFrom("class7".getBytes())));

        fileInfoList.add(fileBuilder3.build());


    }
    //don't prepareScan to tarimDB for test , but keep the codes below
    /*

    Map<String, List<String>> filterByConditions(int tableID, boolean allFlag, byte[] conditions){
        //todo, get the partition key from the schema
        String partitionKey = "class";

        List<String> partitionList = new ArrayList<>();

        Map<String, List<String>> mapNodeToChunk = new HashMap<>();

        String jsonString;
        
        try {
            jsonString = Common.loadTableMeta("partitionKey.json");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("The table meta is incorrect!");
        }

        Object object = deserialize(conditions);

        Expression expression;

        List list = (List)object;
        if (list.size() == 0){
            expression = (Expression) list.get(0);
        }else{
            expression = and((Expression) list.get(0), (Expression) list.get(1));
            for (int i = 2; i < list.size(); i++){
                expression = and(expression, (Expression) list.get(i));
            }
        }

        org.apache.iceberg.shaded.org.apache.avro.Schema avroSchema = new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(jsonString);
        Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Evaluator evaluator = new Evaluator(icebergSchema.asStruct(), expression);


        String prefix;
        if (allFlag){
            prefix = String.format("%d_" ,tableID);
        }else{
            prefix = schemaKeyEncode(tableID, partitionKey);
        }

        List<TarimKVProto.KeyValue> keyValues = kvClient.prefixSeek("default", prefix);
        for (TarimKVProto.KeyValue keyValue : keyValues) {
            boolean result = false;
            if (!allFlag) {
                String key[] = keyValue.getKey().split("=");
                if (key.length != 2){
                    logger.error("get partitionKey error!");
                    continue;
                }else{
                    result = evaluator.eval(Common.Row.of(key[1]));
                }

            }else{
                result = true;
            }

            if (result) {

                Long hash = chunkIDHash(Long.parseLong(keyValue.getValue()));
                KvNode node = localKVMetaClient.getMasterReplicaNode(hash);
                if (node == null){
                    throw new RuntimeException("the node is null!");
                }else{
                    String nodeStr = node.toString();
                    if (mapNodeToChunk.get(nodeStr) == null) {
                        List<String> chunkList = new ArrayList<>();
                        chunkList.add(keyValue.getKey());
                        mapNodeToChunk.put(nodeStr, chunkList);
                    }else{
                        List<String> chunkList = mapNodeToChunk.get(nodeStr);
                        chunkList.add(keyValue.getKey());
                    }
                }
            }
        }

        return mapNodeToChunk;
    }
    TarimProto.PrepareScanResponse partitionTableScan(int tableID, boolean allFlag,  byte[] conditions, List<String> partitionIDs) {

        Map<String, List<String>>  mapNodeToChunk = filterByConditions(tableID, allFlag, conditions);
        List<TarimProto.Partition> partitionAll = new ArrayList<>();

        TarimProto.ScanInfo.Builder scanBuilder = TarimProto.ScanInfo.newBuilder();
        TarimProto.MainAccount.Builder accountBuilder = TarimProto.MainAccount.newBuilder();
        accountBuilder.setAccountType(2);
        accountBuilder.setToken("");
        accountBuilder.setUsername("");

        TarimProto.PrepareScanResponse.Builder responseBuilder = TarimProto.PrepareScanResponse.newBuilder();
        responseBuilder.setCode(0);
        responseBuilder.setMsg("OK");
        scanBuilder.setMainAccount(accountBuilder.build());

        if (mapNodeToChunk == null){
            responseBuilder.setScanInfo(scanBuilder.build());
        }else{
            for (Map.Entry<String, List<String>> entry : mapNodeToChunk.entrySet()){
                String key = entry.getKey();
                String[] result = key.split(":");
                if (result.length != 2){
                    throw new RuntimeException("key is error:" + key);
                }
                String host = result[0];
                int port = Integer.parseInt(result[1]);



                TarimDBMetaClient client = new TarimDBMetaClient(host, port);
                TarimProto.PrepareScanResponse response = client.prepareRequest(tableID, entry.getValue());
                if (response.getCode() == 0){
                    List<TarimProto.Partition> partitionList = response.getScanInfo().getPartitionsList();

                    partitionAll.addAll(partitionList);

                }else{
                    logger.error("fail to prepareScan to tarimDB!");
                }

            }

            scanBuilder.addAllPartitions(partitionAll);
            responseBuilder.setScanInfo(scanBuilder.build());
        }

        return responseBuilder.build();
    }
     */
}
