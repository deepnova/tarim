package com.deepexi;

import com.deepexi.tarimdb.*;
import com.deepexi.tarimkv.*;
/**
 * TarimServer
 *
 */
public class TarimServer
{
    public static void main( String[] args ) {

        System.out.println( "TarimServer: Hello World!" );

        TarimKV kv = new TarimKV();
        TarimKVMeta kvMeta = new TarimKVMeta();
        TarimDB db = new TarimDB(kv, kvMeta);
    }

    public static void testRocksdb() {
        TestRocksDB test = new TestRocksDB();
        test.rocksdbSample();
    }
}
