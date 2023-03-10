package com.deepexi.tarimdb.tarimkv;

import java.util.List;
import java.lang.StringBuilder;
import com.deepexi.rpc.TarimKVProto;

public class KVLocalMetadata {

    public String id;
    public List<TarimKVProto.Node> mnodes;
    public List<TarimKVProto.Slot> slots;

    // for data model (以后需要独立出来)
    public String address;
    public int port;

    /*--- get from meta server in future ----*/
    public KVSchema.MainAccount mainAccount;
    public String mainPath;

    public TarimKVProto.Node getMasterMNode() {
        if(mnodes == null || mnodes.isEmpty()) return null;
        for(TarimKVProto.Node node : this.mnodes){
            return node; // TODO: 暂未实现主节点查找逻辑
        }
        return null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{id=");       sb.append(this.id);
        sb.append(",address=");  sb.append(this.address);
        sb.append(",port=");     sb.append(this.port);

        if(mnodes != null && !mnodes.isEmpty())
        {
            sb.append(",mnodes=[");
            for(TarimKVProto.Node node : this.mnodes){
                sb.append("{id=");     sb.append(node.getId());
                sb.append(",host=");   sb.append(node.getHost());
                sb.append(",port=");   sb.append(node.getPort());
                //sb.append(",role=");   sb.append(node.getRole());
                sb.append(",status="); sb.append(node.getStatus());
                sb.append("}");
            }
            sb.append("]");
        }

        if(slots != null && !slots.isEmpty())
        {
            sb.append(",slots=[");
            for(TarimKVProto.Slot slot : this.slots){
                sb.append("{id=");          sb.append(slot.getId());
                sb.append(",dataPath=");    sb.append(slot.getDataPath());
                sb.append(",role=");        sb.append(slot.getRole());
                sb.append(",status=");      sb.append(slot.getStatus());
                sb.append("}");
            }
            sb.append("]");
        }

        if(mainAccount != null)
        {
            sb.append(",mainAccount={");
            sb.append("type=");         sb.append(this.mainAccount.accountType);
            sb.append(",username=");    sb.append(this.mainAccount.username);
            sb.append(",token=");       sb.append(this.mainAccount.token);
            sb.append("}");
        }

        sb.append(",mainPath=");    sb.append(this.mainPath);
        sb.append("}");

        return sb.toString();
    }

    public static String ObjToString(TarimKVProto.Node node) {
        StringBuilder sb = new StringBuilder();
        sb.append("{id=");     sb.append(node.getId());
        sb.append(",host=");   sb.append(node.getHost());
        sb.append(",port=");   sb.append(node.getPort());
        sb.append(",slots=[");
            for(TarimKVProto.Slot slot: node.getSlotsList()){
                sb.append("{id=");          sb.append(slot.getId());
                sb.append(",dataPath=");    sb.append(slot.getDataPath());
                sb.append(",role=");        sb.append(slot.getRole());
                sb.append(",status=");      sb.append(slot.getStatus());
                sb.append("}");
            }
        sb.append("]");
        sb.append(",status=");    sb.append(node.getStatus());
        sb.append("}");
        return sb.toString();
    }

    public static class Node {
        public String host;
        public int port;
        public Node(String host, int port) {
            this.host = host;
            this.port = port;
        }
        public Node(Node node) {
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
}
