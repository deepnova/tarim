package com.deepexi;

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
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(host);
        sb.append(":");
        sb.append(port);
        return sb.toString();
    }
}
