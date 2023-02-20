package com.deepexi.tarimdb.tarimkv;

import java.util.List;

public abstract class AbstractKV {
    public int init(KVLocalMetadata lMetadata){
        // no-op
        return 0;
    }

    /*------ write ------*/

    /*public void put(KVSchema.KeyValue value) {
        // no-op
    }*/
    public void put(KVSchema.KVChunk chunk, KVSchema.KeyValue value) {
        // no-op
    }
    public void put(KVSchema.KVChunk chunk, List<KVSchema.KeyValue> values) {
        // no-op
    }

    /*------ point read ------*/

    public KVSchema.KeyValue get(KVSchema.KVChunk chunk) {
        // no-op
        return null;
    }

    /*------ chunk scan ------*/
    // only local
    public KVSchema.PrepareScanInfo prepareChunkScan(String tableID, long[] chunks){
        // no-op
        // Note: need keeping the snapshot before complete scan (snapshot counter?).
        return null;
    }

    // ifComplete is output parameter, scan not complete until ifComplete == true.
    public List<KVSchema.KeyValue> deltaChunkScan(KVSchema.DeltaScanParam param, boolean ifComplete){ 
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
    public List<KVSchema.KeyValue> prefixChunkScan(KVSchema.PrefixScanParam param, boolean ifComplete){ 
        // no-op
        return null;
    }
    // stop scan same as if necessary: public void closeChunkScan(int snapshotID);

    /*------ delete ------*/
    public void delete(KVSchema.KVChunk chunk, String key) {
        // no-op
    }

    public void deleteChunk(KVSchema.KVChunk chunk) {
        // no-op
    }
}

