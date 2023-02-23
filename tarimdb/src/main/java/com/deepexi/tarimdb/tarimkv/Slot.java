package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
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
    public final static Logger logger = LogManager.getLogger(Slot.class);

    private TarimKVProto.Slot slotConfig;
    private RocksDB db;
    //private Set<byte[]> columnFamilies;
    private Map<byte[], ColumnFamilyHandle> mapColumnFamilyHandles;

    public Slot(TarimKVProto.Slot slot){
        slotConfig = slot;
    }

    public void open() throws Exception, RocksDBException, IllegalArgumentException {
        if(slotConfig.getDataPath() == null){
            logger.error("slot id=" + slotConfig.getId() + ", it's dataPath is null.");
            throw new IllegalArgumentException("slot dataPath is null");
        }
            
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        List<byte[]> cfList = db.listColumnFamilies(new Options(), slotConfig.getDataPath());
        //TODO: what will happen while before DB create ?
        if(cfList.isEmpty()) cfList.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        for(byte[] cfName : cfList){
            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
            cfDescriptors.add(new ColumnFamilyDescriptor(cfName, cfOptions));
        }

        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);
        dbOptions.setCreateMissingColumnFamilies(true);
        db = RocksDB.open(dbOptions, slotConfig.getDataPath(), cfDescriptors, cfHandles);

        mapColumnFamilyHandles = new HashMap<>();
        for(ColumnFamilyHandle cfh : cfHandles){
             mapColumnFamilyHandles.put(cfh.getName(), cfh);
        }
    }

    public RocksDB getDB(){
        return db;
    }

    public String getSlotID(){
        return slotConfig.getId();
    }

    public void createColumnFamilyIfNotExist(String cfName) throws RocksDBException {
        if(mapColumnFamilyHandles.containsKey(cfName)){
            logger.debug("column family: " + cfName + " exist.");
            return;
        }

        ColumnFamilyHandle cfHandle = db.createColumnFamily( 
                new ColumnFamilyDescriptor(cfName.getBytes(),
                new ColumnFamilyOptions()));
        logger.debug("column family: " + cfName + " not exist, create it now.");

        mapColumnFamilyHandles.put(cfName.getBytes(), cfHandle);
    }

    public void batchWrite(final WriteOptions writeOpts, final WriteBatch updates) throws RocksDBException {
        // TODO: need lock ?
        // cf?
        db.write(writeOpts, updates);
    }

}

