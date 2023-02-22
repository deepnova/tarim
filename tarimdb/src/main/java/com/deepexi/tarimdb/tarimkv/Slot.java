package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.lang.IllegalArgumentException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVProto;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import com.deepexi.tarimdb.util.Status;

/**
 * SlotManager
 *
 */
public class Slot {
    private TarimKVProto.Slot slotConfig;
    private RocksDB db;
    private Set<byte[]> columnFamilies;

    public final static Logger logger = LogManager.getLogger(Slot.class);

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

            columnFamilies = db.listColumnFamilies(options, slotConfig.getDataPath());

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

    public RocksDB getDB(){
        return db;
    }

    public String getSlotID(){
        return slotConfig.getId();
    }

    public Status createColumnFamilyIfNotExist(String cfName){
        if(columnFamilies.contains(cfName)){
            logger.debug("column family: " + cfName + " exist.");
            return Status.OK;
        }
        try(final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
                new ColumnFamilyDescriptor(cfName.getBytes(),
                new ColumnFamilyOptions()))) {
            logger.debug("column family: " + cfName + " not exist, create it now.");
            assert (columnFamilyHandle != null);
            columnFamilies.add(cfName.getBytes());
        }
        return Status.OK;
    }

}

