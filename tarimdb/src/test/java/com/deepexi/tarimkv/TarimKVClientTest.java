package com.deepexi.tarimkv;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.UnknownFormatConversionException;

import com.deepexi.KvNode;
import com.deepexi.TarimMetaClient;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.springframework.boot.test.context.SpringBootTest;

import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimKVProto.PutRequest;
import com.deepexi.rpc.TarimKVProto.*;
import com.deepexi.tarimdb.tarimkv.*;
import com.deepexi.tarimdb.TarimServer;
import com.deepexi.tarimdb.util.TarimKVException;
import com.deepexi.tarimdb.util.Common;

@SpringBootTest(classes = TarimServer.class)
class TarimKVClientTest {

    private final static Logger logger = LogManager.getLogger(TarimKVClientTest.class);

    private TarimMetaClient metaClient;
    private TarimKVClient kvClient;
    private KVLocalMetadata lMetadata;

    public TarimKVClientTest(){
    }

    public void init()
    {
        metaClient = mock(TarimMetaClient.class);
        initLocalMetadata();

        doNothing().doThrow(new RuntimeException()).when(metaClient).refreshDistribution();
        when(metaClient.getDistribution()).thenReturn(TarimKVProto.DistributionInfo.newBuilder().build());
        when(metaClient.getMasterReplicaSlot(1)).thenReturn("sl-1");
        when(metaClient.getMasterReplicaSlot(2)).thenReturn("sl-1");
        when(metaClient.getMasterReplicaSlot(3)).thenReturn("sl-2");
        KVLocalMetadata.Node node1 = new KVLocalMetadata.Node("127.0.0.1", 1302);
        KVLocalMetadata.Node node2 = new KVLocalMetadata.Node("127.0.0.1", 1302);

        when(metaClient.getReplicaNode("sl-1")).thenReturn(new KvNode(node1.host, node1.port));
        when(metaClient.getReplicaNode("sl-2")).thenReturn(new KvNode(node2.host, node2.port));

        kvClient = new TarimKVClient(metaClient, lMetadata);
    }

    private void initLocalMetadata(){
        lMetadata = new KVLocalMetadata();

        lMetadata.id = "dn-1"; lMetadata.address = "127.0.0.1";
        lMetadata.port = 1302;

        TarimKVProto.Node.Builder nodeBuiler = TarimKVProto.Node.newBuilder();
        nodeBuiler.setId("mn-1");
        nodeBuiler.setHost("127.0.0.1");
        nodeBuiler.setPort(1302);
        lMetadata.mnodes = new ArrayList();
        lMetadata.mnodes.add(nodeBuiler.build());

        lMetadata.slots = new ArrayList();
        TarimKVProto.Slot.Builder slotBuiler = TarimKVProto.Slot.newBuilder();

        slotBuiler.setId("sl-1");
        slotBuiler.setDataPath("target/slot1");
        slotBuiler.setRole(TarimKVProto.SlotRole.SR_MASTER);
        slotBuiler.setStatus(TarimKVProto.SlotStatus.SS_USING);
        lMetadata.slots.add(slotBuiler.build());

        slotBuiler.setId("sl-2");
        slotBuiler.setDataPath("target/slot2");
        slotBuiler.setRole(TarimKVProto.SlotRole.SR_MASTER);
        slotBuiler.setStatus(TarimKVProto.SlotStatus.SS_USING);
        lMetadata.slots.add(slotBuiler.build());

        lMetadata.mainAccount = new KVSchema.MainAccount();
        lMetadata.mainAccount.accountType = 1;
        lMetadata.mainAccount.username = "admin";
        lMetadata.mainAccount.token = "admin987";
    }

    @Test
    void contextLoads() {
    }

    @Test
    void testPut()
    {
        init();

        try{
            kvClient.init();
        }catch(TarimKVException e){
            System.out.println("client init TarimKVException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(Exception e){
            System.out.println("client init Exception: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }

        doPut(kvClient, 100, 1);
        doPut(kvClient, 100, 2);
        doPut(kvClient, 100, 3);
    }

    void doPut(TarimKVClient kvClient, int tableID, long chunkID)
    {
        PutRequest.Builder putReqBuilder = PutRequest.newBuilder();
        TarimKVProto.KeyValueByte.Builder kvBuilder = TarimKVProto.KeyValueByte.newBuilder();
        putReqBuilder.setTableID(tableID);
        putReqBuilder.setChunkID(chunkID);
        for(long i = 100 * chunkID; i < 100 + 100 * chunkID; i++){
            kvBuilder.setKey("key-" + Long.toString(i));
            kvBuilder.setValue(ByteString.copyFromUtf8("value-" + Long.toString(i)));
            putReqBuilder.addValues(kvBuilder);
        }

        try{
            kvClient.put(putReqBuilder.build());
        }catch(TarimKVException e){
            System.out.println("TarimKVException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(NullPointerException e){
            System.out.println("NullPointerException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(UnknownFormatConversionException e){
            System.out.println("UnknownFormatConversionException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }
    }

    @Test
    void testGet()
    {
        init();

        GetRequest.Builder getReqBuilder = GetRequest.newBuilder();
        TarimKVProto.KeyValue.Builder kvBuilder = TarimKVProto.KeyValue.newBuilder();
        getReqBuilder.setTableID(100);
        getReqBuilder.setChunkID(1);
        getReqBuilder.addKeys("key-111");
        getReqBuilder.addKeys("key-123");
        getReqBuilder.addKeys("key-225");

        try{
            kvClient.init();
            List<byte[]> results = kvClient.get(getReqBuilder.build());
            logger.info("results: " + Common.BytesListToString(results));
        }catch(TarimKVException e){
            System.out.println("TarimKVException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(NullPointerException e){
            System.out.println("NullPointerException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(Exception e){
            System.out.println("Exception: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }
    }

    @Test
    void testDelete()
    {
        init();

        DeleteRequest.Builder delReqBuilder = DeleteRequest.newBuilder();
        delReqBuilder.setTableID(100);
        delReqBuilder.setChunkID(1);
        delReqBuilder.setKey("key-111");

        try{
            kvClient.init();
            kvClient.delete(delReqBuilder.build());
        }catch(TarimKVException e){
            System.out.println("TarimKVException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(NullPointerException e){
            System.out.println("NullPointerException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(Exception e){
            System.out.println("Exception: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }
    }

    @Test
    void testPrefixSeek()
    {
        init();

        PrefixSeekRequest.Builder prefixReqBuilder = PrefixSeekRequest.newBuilder();
        prefixReqBuilder.setTableID(100);
        prefixReqBuilder.setChunkID(1);
        prefixReqBuilder.setPrefix("key-11");

        try{
            kvClient.init();
            List<TarimKVProto.KeyValueByte> results = kvClient.prefixSeek(prefixReqBuilder.build());
            logger.info("results: " + results.toString());
        }catch(TarimKVException e){
            System.out.println("TarimKVException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(NullPointerException e){
            System.out.println("NullPointerException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(Exception e){
            System.out.println("Exception: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }
    }

    @Test
    void testDeltaScan()
    {
        init();

        int tableID = 100;
        long[] chunks = {1,2,3};
        KVSchema.PrepareScanInfo scanInfo = new KVSchema.PrepareScanInfo();

        /*--- prepare ---*/
        try{
            kvClient.init();
            scanInfo = kvClient.prepareChunkScan(tableID, chunks);
            logger.info("1 scanInfo: " + scanInfo.toString());
        }catch(TarimKVException e){
            System.out.println("TarimKVException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(NullPointerException e){
            System.out.println("NullPointerException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(Exception e){
            System.out.println("Exception: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }
    
        logger.info("2 scanInfo: " + scanInfo.toString());
        KVSchema.DeltaScanParam param = new KVSchema.DeltaScanParam();
        param.scope = 1;
        param.tableID = 100;
        for(KVSchema.ChunkDetail cd : scanInfo.chunkDetails)
        {
            logger.info("--------------- chunk scan: " + cd.chunkID + "--------------------");
            param.chunkID = cd.chunkID;
            param.scanHandler = cd.scanHandler;
            chunkScan(kvClient, param);
        }
    }

    void chunkScan(TarimKVClient kvClient, KVSchema.DeltaScanParam param)
    {
        try{
            /*--- scan ---*/
            boolean ifComplete = false;
            TarimKVProto.RangeData results = kvClient.deltaChunkScan(param, ifComplete);
            logger.info("results: " + results.toString());

            /*--- close ---*/
            if(ifComplete == true)
            {
                kvClient.closeChunkScan(param.tableID, param.chunkID, param.scanHandler);
            }
        }catch(TarimKVException e){
            System.out.println("TarimKVException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(NullPointerException e){
            System.out.println("NullPointerException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(Exception e){
            System.out.println("Exception: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }

    }

    @Test
    void testTimeAndMap()
    {
        logger.info("currentTimeMillis: " + System.currentTimeMillis());
        Map<Long, String> mapMax = new HashMap<>();
        mapMax.put(new Long(1), new String("hello"));
        mapMax.put(new Long(2), new String("hello2"));
        mapMax.put(new Long(5), new String("hello5"));
        mapMax.put(new Long(25), new String("hello25"));
        mapMax.put(new Long(251), new String("hello251"));
        Long maxKey = Collections.max(mapMax.keySet());
        Long minKey = Collections.min(mapMax.keySet());
        logger.info("map max: " + maxKey + ", min: " + minKey);
    }
}

