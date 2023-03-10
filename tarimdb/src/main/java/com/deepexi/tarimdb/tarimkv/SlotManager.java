package com.deepexi.tarimdb.tarimkv;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.rpc.TarimKVProto;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

/**
 * SlotManager
 *
 */
public class SlotManager {

    public final static Logger logger = LogManager.getLogger(SlotManager.class);

    // An instance a slot, a slot a disk(directory)
    private Map<String, Slot> slots; // a thread for a slot
    private List<TarimKVProto.Slot> slotsConfig;

    static {
      logger.debug("Load RocksDB library.");
      RocksDB.loadLibrary();
    }

    public SlotManager(){
        slots = new HashMap();
    }

    public int init(List<TarimKVProto.Slot> slotsConfig) throws Exception, RocksDBException {
        this.slotsConfig = slotsConfig;
        openSlots();
        return 0;
    }

    private void openSlots() throws Exception, IllegalArgumentException, RocksDBException {
        for(TarimKVProto.Slot conf : slotsConfig){
            Slot slot = new Slot(conf);
            slot.open();
            slots.put(conf.getId(), slot);
        }
    }

    public Slot getSlot(String slotID){
        Slot slot = slots.get(slotID);
        if(slot == null){
            logger.error("slot:" + slotID + " not found on this node.");
            return null;
        }
        return slot;
    }
}
