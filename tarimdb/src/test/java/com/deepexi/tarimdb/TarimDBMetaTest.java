package com.deepexi.tarimdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.springframework.boot.test.context.SpringBootTest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import com.deepexi.rpc.TarimMetaGrpc;
import com.deepexi.rpc.TarimProto;
import tarimmeta.TarimDBMeta;

@SpringBootTest
class TarimDBMetaTest {

    public final static Logger logger = LogManager.getLogger(TarimDBMetaTest.class);

    private String metaHost = "127.0.0.1";
    private int metaPort = 1301;
    private Server server;

    @Test
    void contextLoads() {
    }

    @BeforeEach
    void startServer() 
    {
        logger.info("start tarimdb meta server for unit testing, host: " + metaHost + ", port: " + metaPort);
        try{
            ServerBuilder builder = ServerBuilder.forPort(metaPort);
            builder.addService(new TarimDBMeta());
            server = builder.build().start();
            logger.info("TarimDB meta service start...");
        } catch(IOException e){
            logger.error("TarimDBMetaTest start error(IOException)");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void stopServer() 
    {
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    void testGetTable() 
    {
        logger.info("-- into testGetTable --" );
        ManagedChannel channel;
        TarimMetaGrpc.TarimMetaBlockingStub blockStub;
        channel = ManagedChannelBuilder.forAddress(metaHost, metaPort).usePlaintext().build();
        blockStub = TarimMetaGrpc.newBlockingStub(channel);

        TarimProto.GetTableRequest.Builder reqBuilder = TarimProto.GetTableRequest.newBuilder();
        reqBuilder.setCatName("default");
        reqBuilder.setDbName("db001");
        reqBuilder.setTblName("t001");
        TarimProto.GetTableRequest request = reqBuilder.build();
        TarimProto.GetTableResponse response = blockStub.getTable(request);

        logger.info("getTable request: " + request.toString());
        logger.info("getTable response: " + response.toString());
    }
}

