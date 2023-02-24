package com.deepexi.tarimkv;

import java.util.ArrayList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
//import org.apache.logging.log4j.Level;
//import org.apache.logging.log4j.core.Logger;
//import org.apache.logging.log4j.spi.LoggerFactory;
//import org.apache.logging.log4j.core.LoggerContext;
//import org.apache.logging.log4j.core.config.Configuration;
//import org.apache.logging.log4j.core.config.LoggerConfig;

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

@SpringBootTest(classes = TarimServer.class)
class TarimKVClientTest {

    private final static Logger logger = LogManager.getLogger(TarimKVClientTest.class);

    private TarimKVMetaClient metaClient;
    private KVLocalMetadata lMetadata;

    public TarimKVClientTest(){
        metaClient = mock(TarimKVMetaClient.class);
        initLocalMetadata();
    }

    private void initLocalMetadata(){
        lMetadata = mock(KVLocalMetadata.class);

        lMetadata.id = "dn-1"; lMetadata.address = "127.0.0.1";
        lMetadata.port = 1302;

        //lMetadata.mnodes = new ArrayList();

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
    }

    @Test
    void contextLoads() {
    }

    @Test
    void testPut(){

        doNothing().doThrow(new RuntimeException()).when(metaClient).refreshDistribution();
        when(metaClient.getDistribution()).thenReturn(TarimKVProto.DistributionInfo.newBuilder().build());
        when(metaClient.getMasterReplicaSlot(1)).thenReturn("sl-1");
        
        when(metaClient.getReplicaNode("sl-1")).thenReturn(new KVLocalMetadata.Node("127.0.0.1", 1302));
        TarimKVProto.Node.Builder nodeBuiler = TarimKVProto.Node.newBuilder();
        nodeBuiler.setHost("127.0.0.1");
        nodeBuiler.setPort(1302);
        when(lMetadata.getMasterMNode()).thenReturn(nodeBuiler.build());

        TarimKVClient kvClient = new TarimKVClient(metaClient, lMetadata);

        PutRequest.Builder putReqBuilder = PutRequest.newBuilder();
        TarimKVProto.KeyValue.Builder kvBuilder = TarimKVProto.KeyValue.newBuilder();
        putReqBuilder.setTableID(100);
        putReqBuilder.setChunkID(1);
        for(int i = 0; i < 10; i++){
            kvBuilder.setKey("key-" + Integer.toString(i));
            kvBuilder.setValue("value-" + Integer.toString(i));
            kvBuilder.setEncodeVersion(i%3);
            putReqBuilder.addValues(kvBuilder);
        }

        try{
            kvClient.init();
            kvClient.put(putReqBuilder.build());
        }catch(TarimKVException e){
            System.out.println("TarimKVException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }catch(NullPointerException e){
            System.out.println("NullPointerException: " + e);
            e.printStackTrace();
            Assertions.assertFalse(true);
        }
    }

    @Test
    void testGet(){
            
    }
}

