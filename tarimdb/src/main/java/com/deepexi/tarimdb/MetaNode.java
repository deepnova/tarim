package com.deepexi.tarimdb;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.tarimkv.TarimKVMeta;
import com.deepexi.tarimdb.tarimkv.KVMetadata;
import com.deepexi.tarimdb.tarimkv.YamlLoader;
import com.deepexi.tarimdb.datamodels.TarimDBMeta;

import org.rocksdb.RocksDBException;

/**
 * MetaNode
 *
 */
public class MetaNode extends AbstractNode {
    public final static Logger logger = LogManager.getLogger(MetaNode.class);

    private int port = 1301;
    private Server server;
    private KVMetadata metadata;

    public MetaNode(BasicConfig conf){
        super(conf);
    }

    @Override
    public Status init()
    { 
        logger.info("metanode init");
        metadata = new KVMetadata();
        YamlLoader.loadMetaConfig(conf_.configFile, metadata);
        return Status.OK;
    }

    @Override
    public Status start() 
    {
        try{
            ServerBuilder builder = ServerBuilder.forPort(port);
            builder.addService(new TarimKVMeta(metadata));
            builder.addService(new TarimDBMeta(metadata));
            server = builder.build().start();
            logger.info("service start...");

            //添加停机逻辑
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    logger.info("*** shutting down TarimKVMeta server since JVM is shutting down");
                    MetaNode.this.stop();
                    logger.info("*** TarimKVMeta server shut down");
                }
            });

        } catch(RocksDBException e){
            logger.error("MetaNode start error(RocksDBException)");
            return Status.SERVER_START_FAILED;
        } catch(IOException e){
            logger.error("MetaNode start error(IOException)");
            return Status.SERVER_START_FAILED;
        } catch(Exception e){
            logger.error("MetaNode start error(Exception)");
            return Status.SERVER_START_FAILED;
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
