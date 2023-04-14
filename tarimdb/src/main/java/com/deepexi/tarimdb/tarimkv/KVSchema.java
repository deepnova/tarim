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

    public static class PrepareScanInfo {
        public List<ChunkDetail> chunkDetails;
        public MainAccount mainAccount;

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{[");
            if(chunkDetails != null)
            {
                for(ChunkDetail cd : chunkDetails)
                {
                     sb.append(cd.toString());
                     sb.append(",");
                }
            }
            sb.append("],");
            if(mainAccount != null) sb.append(mainAccount.toString());
            sb.append("}");
            return sb.toString();
        }
    }

    public static class ChunkDetail {
        public long chunkID;
        public long scanHandler; // iterator handle
        // 0：none_merge
        // 1：client_merge
        // 2：server_merge
        public int mergePolicy; // only implement 'client_merge' policy first
        public List<String> mainPaths;

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{chunkID=");         sb.append(chunkID);
            sb.append(",scanHandler=");     sb.append(scanHandler);
            sb.append(",mergePolicy=");     sb.append(mergePolicy);
            sb.append(",mainPaths=");       sb.append(mainPaths.toString());
            sb.append("}");
            return sb.toString();
        }
    }

    public static class ChunkScanHandler{
        public long chunkID;
        public long scanHandler; // iterator handle
    }
    
    public static class MainAccount {
        // 1：S3
        // 2：HDFS
        public int accountType;
        public String username;
        public String token;

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{accountType=");     sb.append(accountType);
            sb.append(",username=");        sb.append(username);
            sb.append(",token=");           sb.append(token);
            sb.append("}");
            return sb.toString();
        }
    }

    public static class DeltaScanParam {
        // 1：delta-only
        // 2: full
        public int scope; 
        public int tableID;
        public long scanHandler;
        public long chunkID;
        public String lastKey; //TODO: for full in future
        public int scanSize;
        public String planID;
        public String lowerBound;
        public String upperBound;
        public int lowerBoundType;
        public int upperBoundType;
    }

    public static class PrefixScanParam {
        public String tableID;
        //public int scanHandler;
        public long chunkID;
        public String lastKey;
        public int scanSize;
    }
}

