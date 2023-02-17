package com.deepexi.tarimkv;

import java.util.List;
import java.lang.StringBuilder;
import com.deepexi.rpc.TarimKVMetaSvc;

public class KVLocalMetadata {

    public String id;
    public List<TarimKVMetaSvc.Node> mnodes;
    public List<TarimKVMetaSvc.Slot> slots;

    // for data model (以后需要独立出来)
    public String address;
    public int port;

    public TarimKVMetaSvc.Node getMasterMNode() {
        for(TarimKVMetaSvc.Node node : this.mnodes){
            return node; // TODO: 暂未实现主节点查找逻辑
        }
        return null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{id=");       sb.append(this.id);
        sb.append(",address=");  sb.append(this.address);
        sb.append(",port=");     sb.append(this.port);

        sb.append(",mnodes=[");
        for(TarimKVMetaSvc.Node node : this.mnodes){
            sb.append("{id=");     sb.append(node.getId());
            sb.append(",host=");   sb.append(node.getHost());
            sb.append(",port=");   sb.append(node.getPort());
            //sb.append(",role=");   sb.append(node.getRole());
            sb.append(",status="); sb.append(node.getStatus());
            sb.append("}");
        }
        sb.append("]");

        sb.append(",slots=[");
        for(TarimKVMetaSvc.Slot slot : this.slots){
            sb.append("{id=");          sb.append(slot.getId());
            sb.append(",dataPath=");    sb.append(slot.getDataPath());
            sb.append(",role=");        sb.append(slot.getRole());
            sb.append(",status=");      sb.append(slot.getStatus());
            sb.append("}");
        }
        sb.append("]");

        sb.append("}");
        return sb.toString();
    }

    public static String ObjToString(TarimKVMetaSvc.Node node) {
        StringBuilder sb = new StringBuilder();
        sb.append("{id=");     sb.append(node.getId());
        sb.append(",host=");   sb.append(node.getHost());
        sb.append(",port=");   sb.append(node.getPort());
        sb.append(",slots=[");
            for(TarimKVMetaSvc.Slot slot: node.getSlotsList()){
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
}
