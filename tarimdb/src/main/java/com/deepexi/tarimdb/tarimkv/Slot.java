package com.deepexi.tarimdb.tarimkv;

import java.util.*;
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
import com.deepexi.tarimdb.util.HandlerMap;

/**
 * SlotManager
 *
 */
public class Slot 
{
    public final static Logger logger = LogManager.getLogger(Slot.class);
    private TarimKVProto.Slot slotConfig;
    private RocksDB db;
    private Map<String, ColumnFamilyHandle> mapColumnFamilyHandles;

    private HandlerMap<RocksIterator> mapScanHandlers; //TODO: clear handlers which not be closed.
    private Map<String, RocksIterator> mapToIter;

    public Slot(TarimKVProto.Slot slot){
        slotConfig = slot;
    }

    public void put(String key, String value){
        logger.info("put operator, key = {}, value = {}", key ,value);

        try {
            db.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
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

        mapScanHandlers = new HandlerMap<>();
        mapToIter = new HashMap<>();
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
            logger.info("map key compare: " + (key.equals(entry.getKey())));
        }
        return sb.toString();
    }

    public ColumnFamilyHandle getColumnFamilyHandle(final String cfName) throws RocksDBException, TarimKVException 
    {
        // TODO: WriteBatch and Iterator need lock ?
        ColumnFamilyHandle cfHandle = mapColumnFamilyHandles.get(cfName);
        return cfHandle;
    }

    public void batchWrite(WriteOptions writeOpts, final WriteBatch updates) throws RocksDBException
    {
        db.write(writeOpts, updates);
    }

    public List<byte[]> multiGet(ReadOptions readOpts, ColumnFamilyHandle cfHandle, List<String> keys) throws RocksDBException
    {
        List<ColumnFamilyHandle> cfhList = new ArrayList<>();
        List<byte[]> keyList = Common.stringListToBytesList(keys);
        for(int i = 0; i < keys.size(); i++){
            cfhList.add(cfHandle); // ColumnFamilyHandle for every key
        }

        readOpts.setAutoPrefixMode(true);
        List<byte[]> values = db.multiGetAsList(readOpts, cfhList, keyList);
        logger.info("multiGet(), ColumnFamily name: " + new String(cfHandle.getName())
                  + ", slot id: " + getSlotID()
                  + ", key size: " + keyList.size()
                  + ", value size: " + values.size());
        logger.info("multiGet()" + Common.BytesListToString(keyList));
        for (byte[] value: values){
            if (value != null){
                logger.info("value:" + Arrays.toString(value));
            }
        }
        return values;
    }

    public void delete(WriteOptions writeOpts, ColumnFamilyHandle cfHandle, String key) throws RocksDBException
    {
        logger.info("delete(), ColumnFamily name: " + new String(cfHandle.getName())
                  + ", slot id: " + getSlotID() + ", key: " + key);
        db.delete(cfHandle, writeOpts, key.getBytes());
    }

    public List<TarimKVProto.KeyValueByte>  prefixSeek(ReadOptions readOpts, ColumnFamilyHandle cfHandle, String keyPrefix) throws RocksDBException, TarimKVException
    {
        readOpts.setAutoPrefixMode(true);
        RocksIterator iter = db.newIterator(cfHandle, readOpts);
        iter.seek(keyPrefix.getBytes());

        List<TarimKVProto.KeyValueByte> results = new ArrayList();

        for (iter.seek(keyPrefix.getBytes()); 
             iter.isValid() && Common.startWith(iter.key(), keyPrefix.getBytes()); 
             iter.next()) 
        {
            iter.status();
            if(iter.key() == null || iter.value() == null)
            {
                logger.error("prefixSeek(), iterator seek error, key or value is null, key: " + iter.key()
                           + ", value: " + iter.value()
                           + ", key prefix: " + keyPrefix);
                continue; // TODO: throw exception if necessary in futrue.
            }
            String key = new String(iter.key());
            byte[] value = iter.value();
            KeyValueCodec kvc = KeyValueCodec.KeyDecode(key, value);
            if(kvc == null){
                logger.warn("prefixSeek() key not matched and ignore, result internal key: " + key 
                            + ", value: " + value
                            + ", cfName: " + cfHandle.getName()
                            + ", key prefix: " + keyPrefix);
                continue;
            } 
            logger.info("prefixSeek(), result internal key: " + key
                        + ", value: " + value
                        + ", chunkID: " + kvc.chunkID
                        + ", key: " + kvc.value.getKey()
                        + ", value: " + kvc.value.getValue()
                        + ", cfName: " + cfHandle.getName()
                        + ", key prefix: " + keyPrefix);
            results.add(kvc.value);
        }
        return results;
    }

    public List<TarimKVProto.KeyValue>  prefixSeekForSchema(ReadOptions readOpts, ColumnFamilyHandle cfHandle, String keyPrefix) throws RocksDBException, TarimKVException
    {
        readOpts.setAutoPrefixMode(true);
        RocksIterator iter = db.newIterator(cfHandle, readOpts);
        iter.seek(keyPrefix.getBytes());

        List<TarimKVProto.KeyValue> results = new ArrayList();

        for (iter.seek(keyPrefix.getBytes());
             iter.isValid() && Common.startWith(iter.key(), keyPrefix.getBytes());
             iter.next())
        {
            iter.status();
            if(iter.key() == null || iter.value() == null)
            {
                logger.error("prefixSeek(), iterator seek error, key or value is null, key: " + iter.key()
                        + ", value: " + iter.value()
                        + ", key prefix: " + keyPrefix);
                continue; // TODO: throw exception if necessary in futrue.
            }
            String key = new String(iter.key());
            byte[] value = iter.value();
            KeyValueCodec kvc = KeyValueCodec.schemaKeyDecode(key, new String(value));
            if(kvc == null){
                logger.warn("prefixSeek() key not matched and ignore, result internal key: " + key
                        + ", value: " + value
                        + ", cfName: " + cfHandle.getName()
                        + ", key prefix: " + keyPrefix);
                continue;
            }
            logger.info("prefixSeek(), result internal key: " + key
                    + ", value: " + value
                    + ", chunkID: " + kvc.tableID
                    + ", key: " + kvc.valueKV.getKey()
                    + ", value: " + kvc.valueKV.getValue()
                    + ", cfName: " + cfHandle.getName()
                    + ", key prefix: " + keyPrefix);
            results.add(kvc.valueKV);
        }
        return results;
    }

    public long prepareScan(ColumnFamilyHandle cfHandle, String keyPrefix) 
    {
        ReadOptions scanOpts = new ReadOptions();
        scanOpts.setAutoPrefixMode(true);
        RocksIterator it = db.newIterator(cfHandle, scanOpts);
        return mapScanHandlers.put(it);
    }

    boolean getIteratorFilter(int upperBoundType, String upperBound, byte[] iteratorKey, String starKey){

        switch (upperBoundType) {
            case 0:
                return Common.startWith(iteratorKey, starKey.getBytes());
            case 1:
                return (new String(iteratorKey)).compareTo(upperBound) < 0;
            case 2:
                return (new String(iteratorKey)).compareTo(upperBound) <= 0;
            default:
                throw new RuntimeException("un support type!");
        }
    }

    public TarimKVProto.RangeData deltaScan(ReadOptions readOpts,
                                                   ColumnFamilyHandle cfHandle, 
                                                   long scanHandler,
                                                   String startKey,
                                                   int scanSize,
                                                   String planID,
                                                   String lowerBound,
                                                   String upperBound,
                                                   int lowerBoundType,
                                                   int upperBoundType) throws RocksDBException, TarimKVException
    {
        readOpts.setAutoPrefixMode(true);
        readOpts.setFillCache(false);
        readOpts.setPrefixSameAsStart(true);
        //readOpts.setIgnoreRangeDeletions(true); //TODO: may useful
        int totalSize = 0;
        List<TarimKVProto.KeyValueOp> results = new ArrayList();
        KeyValueCodec kvc;
        Boolean endFlag = false;


        if (!mapToIter.containsKey(planID)) {
            RocksIterator it = db.newIterator(cfHandle, readOpts);

            mapToIter.put(planID, it);
            //the first range in the scan
            if (lowerBoundType == 0){
                //NEGATIVE_INFINITY
                for (it.seekToFirst();
                     it.isValid()  && totalSize < scanSize;
                     it.next()){
                    it.status();
                    if(it.key() == null || it.value() == null)
                    {
                        throw new RocksDBException("deltaScan the key is null!");
                    }

                    if (!getIteratorFilter(upperBoundType, upperBound, it.key(), startKey)){
                        continue;
                    }

                    totalSize++;

                    kvc = KeyValueCodec.OpKeyDecode(new String(it.key()), it.value());
                    if(kvc == null){
                        throw new RocksDBException("deltaScan the kvc is null!");
                    }
                    logger.info("deltaScan(), result internal key: " + it.key()
                            + ", value: " + it.value()
                            + ", chunkID: " + kvc.chunkID
                            + ", op: " + kvc.valueOp.getOp()
                            + ", key: " + kvc.valueOp.getKey()
                            + ", value: " + kvc.valueOp.getValue()
                            + ", cfName: " + cfHandle.getName()
                            + ", start key: " + startKey);
                    results.add(kvc.valueOp);
                }
            }else{
                //NEGATIVE_INFINITY

                for (it.seek(lowerBound.getBytes());
                     it.isValid() && getIteratorFilter(upperBoundType, upperBound, it.key(), startKey) && totalSize < scanSize;
                     it.next(), totalSize++){
                    it.status();
                    if(it.key() == null || it.value() == null)
                    {
                        throw new RocksDBException("deltaScan the key is null!");
                    }

                    if (lowerBound.equals(new String(it.key()))){
                        //skip the first, because the range is (a,b]
                        logger.info("the first key is skip" + it.key());
                        continue;
                    }

                    kvc = KeyValueCodec.OpKeyDecode(new String(it.key()), it.value());
                    if(kvc == null){
                        throw new RocksDBException("deltaScan the kvc is null!");
                    }
                    logger.info("deltaScan(), result internal key: " + it.key()
                            + ", value: " + it.value()
                            + ", chunkID: " + kvc.chunkID
                            + ", op: " + kvc.valueOp.getOp()
                            + ", key: " + kvc.valueOp.getKey()
                            + ", value: " + kvc.valueOp.getValue()
                            + ", cfName: " + cfHandle.getName()
                            + ", start key: " + startKey);
                    results.add(kvc.valueOp);
                }
            }

            if (!it.isValid() || !getIteratorFilter(upperBoundType, upperBound, it.key(), startKey)){
                endFlag = true;
                mapToIter.remove(planID);
                it.close();
            }

        }else{
            RocksIterator iter = mapToIter.get(planID);

            if(iter == null) throw new TarimKVException(Status.NULL_POINTER);
            for (; iter.isValid() && getIteratorFilter(upperBoundType, upperBound, iter.key(), startKey) && totalSize < scanSize;
                 iter.next(), totalSize++){
                iter.status();
                if(iter.key() == null || iter.value() == null)
                {
                    throw new RocksDBException("deltaScan the key is null!");
                }

                kvc = KeyValueCodec.OpKeyDecode(new String(iter.key()), iter.value());
                if(kvc == null){
                    throw new RocksDBException("deltaScan the kvc is null!");
                }
                logger.info("deltaScan(), result internal key: " + iter.key()
                        + ", value: " + iter.value()
                        + ", chunkID: " + kvc.chunkID
                        + ", op: " + kvc.valueOp.getOp()
                        + ", key: " + kvc.valueOp.getKey()
                        + ", value: " + kvc.valueOp.getValue()
                        + ", cfName: " + cfHandle.getName()
                        + ", start key: " + startKey);
                results.add(kvc.valueOp);
            }
            if (!iter.isValid() || !getIteratorFilter(upperBoundType, upperBound, iter.key(), startKey)){
                endFlag = true;
                mapToIter.remove(planID);
                iter.close();
            }
        }

        return TarimKVProto.RangeData.newBuilder()
                .setDataEnd(endFlag)
                .addAllValues(results)
                .build();
    }

    public void releaseScanHandler(long scanHandler) 
    {
        //TODO: RocksDB的Iterator在析构中释放的资源，rocksjni未提供相关接口。
        //      由GC触发内存释放可能会比较晚，可能会存在资源释放不及时的问题(如pinned data)。
        /*
        RocksIterator it = mapScanHandlers.get(scanHandler);
        do some release?
        if(it == null) return;
        */
        mapScanHandlers.remove(scanHandler); 
    }
}

