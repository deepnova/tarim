package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.lang.IllegalArgumentException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVProto;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * SlotManager
 *
 */
public class Slot {
    private TarimKVProto.Slot slotConfig;
    private RocksDB db;

    public final static Logger logger = LogManager.getLogger(TarimKV.class);

    public Slot(TarimKVProto.Slot slot){
        slotConfig = slot;
    }

    public void open() /*throws Exception*/ { // why can't throws, must catch 'Exception' 'RocksDBException' ...
        if(slotConfig.getDataPath() == null){
            logger.error("slot id=" + slotConfig.getId() + ", it's dataPath is null.");
            //TODO: should set slot status
            //continue;
            throw new IllegalArgumentException("slot dataPath is null");
        }

        try {
            Options options = new Options();
            options.setCreateIfMissing(true);
            //TODO: custom options

            db = RocksDB.open(options, slotConfig.getDataPath());

        } catch (RocksDBException e) {
            logger.error("slot id=%s caught the expected exception -- %s\n", slotConfig.getId(), e);
            //throw new RocksDBException("RocksDB open failed.");
        } catch (IllegalArgumentException e) {
            logger.error("slot id=%s caught the expected exception -- %s\n", slotConfig.getId(), e);
            throw new IllegalArgumentException("RocksDB open failed.");
        } catch (Exception e) {
            logger.error("slot id=%s caught the expected exception -- %s\n", slotConfig.getId(), e);
            //throw new Exception("RocksDB open failed.");
        }
    }
}

