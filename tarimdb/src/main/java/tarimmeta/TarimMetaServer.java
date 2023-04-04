package tarimmeta;

import com.deepexi.KvNode;
import com.deepexi.TarimMetaClient;
import com.deepexi.rpc.TarimExecutor;
import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimProto;
import com.deepexi.tarimdb.util.Common;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.deepexi.tarimdb.tarimkv.KeyValueCodec.schemaKeyEncode;
import static com.deepexi.tarimdb.util.SerializeUtil.deserialize;


public class TarimMetaServer {
    private final static Logger logger = LogManager.getLogger(TarimMetaServer.class);
    MetaKVClient kvClient;
    KVMetadata metadata;
    public TarimMetaServer(KVMetadata metadata) {

        this.metadata = metadata;

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
        long chunkID = partitionID.hashCode() & 0x00000000FFFFFFFFL;

        kvClient.put(key, String.valueOf(chunkID));
        //if put fail, there should be exception
        return 0;
    }

    public TarimProto.PrepareScanResponse prepareScan(int tableID, TarimExecutor.ExecType execType, byte[] conditions, List<String> partitionIDs) {

        switch (execType) {
            case TypePartitionTableScan:
                return partitionTableScan(tableID, conditions, partitionIDs);
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


    Map<String, List<String>> filterByConditions(int tableID, byte[] conditions){
        //todo, get the partition key from the schema

        String partitionKey = "class";

        Object object = deserialize(conditions);
        Expression expression = (Expression) object;
        List<String> partitionList = new ArrayList<>();

        Map<String, List<String>> mapNodeToChunk = new HashMap<>();

        TarimKVProto.DistributionInfo distribution = metadata.toDistributionInfo(false);
        TarimMetaClient localMetaClient = new TarimMetaClient(distribution);

        String jsonString;
        
        try {
            jsonString = Common.loadTableMeta("tablemeta.json");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("The table meta is incorrect!");
        }

        org.apache.iceberg.shaded.org.apache.avro.Schema avroSchema = new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(jsonString);
        Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
        Evaluator evaluator = new Evaluator(icebergSchema.asStruct(), expression);

        String prefix = schemaKeyEncode(tableID, partitionKey);
        List<TarimKVProto.KeyValue> keyValues = kvClient.prefixSeek("default", prefix);
        for (TarimKVProto.KeyValue keyValue : keyValues) {
            boolean result = evaluator.eval(Common.Row.of(keyValue.getKey()));

            if (result) {
                Long chunkID = Long.parseLong(keyValue.getValue());
                KvNode node = localMetaClient.getMasterReplicaNode(chunkID);
                if (node != null){
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
    TarimProto.PrepareScanResponse partitionTableScan(int tableID, byte[] conditions, List<String> partitionIDs) {

        Map<String, List<String>>  mapNodeToChunk = filterByConditions(tableID, conditions);

        if (mapNodeToChunk == null){
            //todo
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

                }

            }
        }


        return null;
    }
}
