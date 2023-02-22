package com.deepexi.tarimdb.tarimkv;

import java.util.List;

// define in tarimkv.proto
public class KVSchema {

    static class KVTable {
        public String id;
        public String columnFamily;
        // ...
    }

    static class KVChunk {
        public String tableID;
        public int id; // 统一转换int
    }
/*
    static class KeyValue {
        // 1: new
        // 2: delete
        public int op;
        public String key;
        public String value;
        public int encodeVersion;
    }
*/
    static class InternalKeyValue {
        public String key;
        public String value;
        public int encodeVersion;
    }

    static class PrepareScanInfo {
        public int snapshotID;
        public List<ChunkDetail> chunkDetails;
        public MainAccount mainAccount;
    }

    static class ChunkDetail {
        public long chunkID;
        // 0：none_merge
        // 1：client_merge
        // 2：server_merge
        public int mergePolicy; // only implement 'client_merge' policy first
        public String mainPath;
    }
    
    static class MainAccount {
        // 1：S3
        // 2：HDFS
        public int accountType;
        public String username;
        public String token;
    }

    static class DeltaScanParam {
        // 1：delta-only
        // 2: full
        public int scope; 
        public String tableID;
        public int snapshotID;
        public long chunkID;
        public String lastKey;
        public int scanSize;
    }

    static class PrefixScanParam {
        public String tableID;
        //public int snapshotID;
        public long chunkID;
        public String lastKey;
        public int scanSize;
    }
}

