package com.deepexi;

import com.deepexi.rpc.TarimKVProto;

import java.io.Serializable;

public class KvNode implements Serializable {
    public String host;
    public int port;
    public KvNode(String host, int port) {
        this.host = host;
        this.port = port;
    }
    public KvNode(KvNode node) {
        this.host = node.host;
        this.port = node.port;
    }
    public String toString(TarimKVProto.Node node) {
        StringBuilder sb = new StringBuilder();
        sb.append("{host=");     sb.append(host);
        sb.append(",port=");     sb.append(port);
        sb.append("}");
        return sb.toString();
    }
}
