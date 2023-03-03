package com.deepexi.tarimdb.tarimkv;

import java.util.List;

import com.deepexi.rpc.TarimKVProto.*;

public abstract class AbstractKV {

    public int init(KVLocalMetadata lMetadata){
        // no-op
        return 0;
    }

    /*------ write ------*/

    public void put(PutRequest request) {
        // no-op
    }

    /*------ point read ------*/

    public List<KeyValue> get(GetRequest request) {
        // no-op
        return null;
    }

    /*------ chunk scan (only local) ------*/
    
    public KVSchema.PrepareScanInfo prepareChunkScan(String tableID, long[] chunks){
        // no-op
        // Note: need keeping the snapshot before complete scan (snapshot counter?).
        return null;
    }

    // ifComplete is output parameter, scan not complete until ifComplete == true.
    public List<KeyValue> deltaChunkScan(KVSchema.DeltaScanParam param, boolean ifComplete){ 
        // no-op
        return null;
    }

    // stop scan even ifComplete == false
    public void closeChunkScan(int snapshotID){
        // no-op
    }

    /*------ prefix scan ------*/
    // only support prefix scan in chunk of table
    // for internal
/*
    // return snapshotID 
    public int preparePrefixChunkScan(String tableID, long[] chunks){
        // no-op
        // Note: need keeping the snapshot before complete scan (snapshot counter?).
        return 0;
    }
*/
    // Temp: get all one-time
    public List<KeyValue> prefixSeek(PrefixSeekRequest request) {
        return null;
    }

    /*------ delete ------*/

    public void delete(DeleteRequest request) {
    }

    // Temp: local only
    public void deleteChunk(KVSchema.KVChunk chunk) {
        // no-op
    }
}

