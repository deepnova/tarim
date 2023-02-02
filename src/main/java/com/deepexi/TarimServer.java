package com.deepexi;

import com.deepexi.tarimdb.TarimDB;
import com.deepexi.tarimkv.TarimKV;
/**
 * TarimServer
 *
 */
public class TarimServer
{
    public static void main( String[] args )
    {
        System.out.println( "TarimServer: Hello World!" );

        TarimDB db = new TarimDB();
        db.info();

        TarimKV kv = new TarimKV();
        kv.info();
        kv.rocksdbSample();
    }

}
