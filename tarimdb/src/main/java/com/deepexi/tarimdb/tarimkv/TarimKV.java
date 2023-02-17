package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.BasicConfig;
import com.deepexi.tarimdb.util.Status;
import com.deepexi.rpc.TarimKVMetaSvc;
import com.deepexi.rpc.TarimKVMetaSvc.DistributionInfo;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * TarimKV
 *   TarimKV data server
 *
 */
public class TarimKV extends AbstractKV {

    //private final static Logger logger = LoggerFactory.getLogger(this.getClass());
    public final static Logger logger = LogManager.getLogger(TarimKV.class);

    private KVLocalMetadata lMetadata;
    private TarimKVMetaClient metaClient;
    private DistributionInfo dataDist;

    // An instance per slot, a slot per disk(directory)
    private Map<String, RocksDB> dbInstances;

    static {
      logger.debug("Load RocksDB library.");
      RocksDB.loadLibrary();
    }

    public TarimKV(){
        dbInstances = new HashMap();
    }

    public int init(KVLocalMetadata lMetadata) {
        this.lMetadata = lMetadata;
        getKVMetadata();
        openRocksDBInstances();
        return 0;
    }

    public void openRocksDBInstances() {

        for(TarimKVMetaSvc.Slot slot : lMetadata.slots){

            if(slot.getDataPath() == null){
                logger.error("slot id=" + slot.getId() + ", it's dataPath is null.");
                //TODO: should set slot status
                continue;
            }

            try{
                Options options = new Options();
                options.setCreateIfMissing(true);
                //TODO: custom options

                RocksDB db = RocksDB.open(options, slot.getDataPath());

            } catch (RocksDBException e) {
              logger.error("slot id=%s caught the expected exception -- %s\n", slot.getId(), e);
            } catch (IllegalArgumentException e) {
            }
        }
    }

    public void getKVMetadata() {
        if(metaClient == null){
            TarimKVMetaSvc.Node node = lMetadata.getMasterMNode();
            if(node == null) throw new NullPointerException("Not found master meta node"); 
            logger.info("get kv metadata from host: " + node.getHost() + ", port: " + node.getPort());
            metaClient = new TarimKVMetaClient(node.getHost(), node.getPort());
        }
        dataDist = metaClient.getDistribution();
    }
}

