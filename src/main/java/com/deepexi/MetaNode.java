package com.deepexi;

import io.grpc.Server;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.lang.InterruptedException;

import com.deepexi.util.TLog;
import com.deepexi.util.BasicConfig;
import com.deepexi.util.Status;
import com.deepexi.tarimkv.TarimKVMeta;
import com.deepexi.tarimkv.KVMetadata;
import com.deepexi.tarimkv.YamlLoader;

/**
 * MetaNode
 *
 */
public class MetaNode extends AbstractNode {

    private int port = 1301;
    private Server server;
    private KVMetadata metadata;

    public MetaNode(BasicConfig conf){
        super(conf);
    }

    @Override
    public Status init(){ 
        TLog.info("metanode init");
        metadata = new KVMetadata();
        YamlLoader.loadMetaConfig(conf_.configFile, metadata);
        return Status.OK;
    }

    @Override
    public Status start() {

        try{
            server = ServerBuilder.forPort(port).addService(new TarimKVMeta(metadata))
                .build().start();
            TLog.info("service start...");

            //添加停机逻辑
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    TLog.info("*** shutting down TarimKVMeta server since JVM is shutting down");
                    MetaNode.this.stop();
                    TLog.info("*** TarimKVMeta server shut down");
                }
            });

        } catch(IOException e){
            TLog.error("MetaNode start error(IOException)");
            return Status.SERVER_START_FAILED;
        //} catch(InterruptedException e){
        //    TLog.error("MetaNode start error(InterruptedException)");
        //    return Status.SERVER_START_FAILED;
        }

        return Status.OK;
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
