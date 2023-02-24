package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.lang.StringBuilder;
import java.lang.IllegalArgumentException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVProto;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.util.TarimKVException;
import com.deepexi.tarimdb.util.Common;

/**
 * SlotManager
 *
 */
public class Slot {
    public final static Logger logger = LogManager.getLogger(Slot.class);

    private TarimKVProto.Slot slotConfig;
    private RocksDB db;
    //private Map<byte[], ColumnFamilyHandle> mapColumnFamilyHandles;
    private Map<String, ColumnFamilyHandle> mapColumnFamilyHandles;

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

        List<byte[]> cfList = RocksDB.listColumnFamilies(new Options(), slotConfig.getDataPath());
        //TODO: what will happen while before DB create ?
        if(cfList == null){
            cfList.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }else if(cfList.isEmpty()){
            cfList = new ArrayList<>(); // RocksDB.listColumnFamilies() returns ArrayList.asList(), can't add.
            cfList.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }
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
             mapColumnFamilyHandles.put(new String(cfh.getName()), cfh);
        }
    }

    public RocksDB getDB(){
        return db;
    }

    public String getSlotID(){
        return slotConfig.getId();
    }

    public void createColumnFamilyIfNotExist(String cfName) throws RocksDBException 
    {
        if(mapColumnFamilyHandles.containsKey(cfName))
        {
            logger.debug("column family: " + cfName + " exist.");
            return;
        }

        ColumnFamilyHandle cfHandle = db.createColumnFamily( 
                new ColumnFamilyDescriptor(cfName.getBytes(),
                new ColumnFamilyOptions()));
        logger.info("column family: " + cfName + " not exist, create it now."
                   + " handle name: " + new String(cfHandle.getName()) + ", handle id: " + cfHandle.getID());

        mapColumnFamilyHandles.put(cfName, cfHandle);
    }

    private String mapColumnFamilyHandlestoString(String key) throws RocksDBException 
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ColumnFamilyHandle> entry : mapColumnFamilyHandles.entrySet()) 
        {
            ColumnFamilyHandle cfh = entry.getValue();
            sb.append("{key=");             sb.append(entry.getKey());
            sb.append(",{value=[name:");    sb.append(new String(cfh.getName()));
            sb.append(",id:");              sb.append(cfh.getID());
            sb.append("]}},");
            logger.info("map key compare: " + (key == entry.getKey()));
        }
        return sb.toString();
    }

    public ColumnFamilyHandle getColumnFamilyHandle(final String cfName) throws RocksDBException, TarimKVException 
    {
        // TODO: WriteBatch and Iterator need lock ?
        ColumnFamilyHandle cfHandle = mapColumnFamilyHandles.get(cfName);
        logger.info("CF handles: " + mapColumnFamilyHandlestoString(cfName));
        if(cfHandle == null)
        {
            logger.error("not found ColumnFamilyHandle of column family: " + cfName);
            throw new TarimKVException(Status.NULL_POINTER);
        }
        return cfHandle;
    }

    public void batchWrite(final WriteOptions writeOpts, final WriteBatch updates) throws RocksDBException
    {
        db.write(writeOpts, updates);
    }

    public List<byte[]> multiGet(final ReadOptions readOpts, ColumnFamilyHandle cfHandle, List<String> keys) throws RocksDBException
    {
        List<ColumnFamilyHandle> cfhList = new ArrayList<>();
        List<byte[]> keyList = Common.stringListToBytesList(keys);
        for(int i = 0; i < keys.size(); i++){
            cfhList.add(cfHandle); // ColumnFamilyHandle for every key
        }

        List<byte[]> values = db.multiGetAsList(readOpts, cfhList, keyList);
        logger.info("multiGet(), ColumnFamily name: " + new String(cfHandle.getName())
                  + ", slot id: " + getSlotID()
                  + ", key size: " + keyList.size()
                  + ", value size: " + values.size());
        return values;
    }

    public void delete(final WriteOptions writeOpts, ColumnFamilyHandle cfHandle, String key) throws RocksDBException
    {
        logger.info("delete(), ColumnFamily name: " + new String(cfHandle.getName())
                  + ", slot id: " + getSlotID() + ", key: " + key);
        db.delete(cfHandle, writeOpts, key.getBytes());
    }
}

