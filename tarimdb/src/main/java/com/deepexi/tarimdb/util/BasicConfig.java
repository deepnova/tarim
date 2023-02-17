package com.deepexi.tarimdb.util;

/**
 * BasicConfig
 *
 */
public class BasicConfig {

    public static final String DATANODE = "dnode";
    public static final String METANODE = "mnode";

    public String mode; // DATANODE, METANODE
    public String configFile; 

    public BasicConfig(){
        mode = DATANODE;
        configFile = "";
    }
}
