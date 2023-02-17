package com.deepexi.tarimdb.tarimkv;

public class KVSchema {

    public class KVTable {
        public int id;
        public String columnFamily;
        // ...
    }

    public class KVChunk {
        public int id; // It's hash value
        public int tableID;
        // ...
    }

    public class KeyValue {
        public String key;
        public String value;
        public int encodeVersion;
    }

    public class InternalKeyValue {
        public String key;
        public String value;
        public int encodeVersion;
    }
}

