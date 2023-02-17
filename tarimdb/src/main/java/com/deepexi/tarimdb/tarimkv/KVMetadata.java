package com.deepexi.tarimdb.tarimkv;

import java.util.List;
import java.lang.StringBuilder;
import com.deepexi.rpc.TarimKVMetaSvc;

public class KVMetadata {

    public String id;
    public String metaMode;
    public String address;
    public int port;
    public String role;
    public List<TarimKVMetaSvc.Node> mnodes;
    public List<TarimKVMetaSvc.RGroupItem> rgroups;
    public List<TarimKVMetaSvc.Node> dnodes;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{id=");       sb.append(this.id);
        sb.append(",metaMode="); sb.append(this.metaMode);
        sb.append(",address=");  sb.append(this.address);
        sb.append(",port=");     sb.append(this.port);
        sb.append(",role=");     sb.append(this.role);

        sb.append(",mnodes=[");
        for(TarimKVMetaSvc.Node node : this.mnodes){
            sb.append("{id=");     sb.append(node.getId());
            sb.append(",host=");   sb.append(node.getHost());
            sb.append(",port=");   sb.append(node.getPort());
            sb.append(",status="); sb.append(node.getStatus());
            sb.append("}");
        }
        sb.append("]");

        sb.append(",rgroups=[");
        for(TarimKVMetaSvc.RGroupItem group : this.rgroups){
            sb.append("{id=");          sb.append(group.getId());
            sb.append(",hashValue=");   sb.append(group.getHashValue());
            sb.append(",slots=[");
                for(TarimKVMetaSvc.Slot slot : group.getSlotsList()){
                    sb.append("{id=");          sb.append(slot.getId());
                    sb.append(",role=");        sb.append(slot.getRole());
                    sb.append("}");
                }
            sb.append("]}");
        }
        sb.append("]");

        sb.append(",dnodes=[");
        for(TarimKVMetaSvc.Node node : this.dnodes){
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
        }
        sb.append("]");

        sb.append("}");
        return sb.toString();
    }

    public static String ObjToString(TarimKVMetaSvc.RGroupItem rgItem) {
        StringBuilder sb = new StringBuilder();
        sb.append("{id=");   sb.append(rgItem.getId());
        sb.append(",hashValue=");       sb.append(rgItem.getHashValue());
        sb.append(",slots=[");
            for(TarimKVMetaSvc.Slot slot : rgItem.getSlotsList()){
                sb.append("{id=");      sb.append(slot.getId());
                sb.append(",role=");    sb.append(slot.getRole());
                sb.append("}");
            }
        sb.append("]}");
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

    /*public void setId(String id){
        this.id = id;
    }
    public String getId(){
        return this.id;
    }*/
/*
    // replace by tarimkvmeta.proto below ?
    public enum NodeStatus {
        sInit((byte) 0),
        sRunning((byte) 1),
        sOffline((byte)2),
        sFailure((byte)3);

        private NodeStatus(final byte value){
            value_ = value;
        }

        public byte getValue() {
            return value_;
        }

        private final byte value_;
    }

    public enum SlotRole {
        rNone((byte) 0),
        rMaster((byte) 1),
        rSecondary((byte)2);

        private SlotRole(final byte value){
            value_ = value;
        }

        public byte getValue() {
            return value_;
        }

        private final byte value_;
    }

    public enum SlotStatus {
        sIdle((byte) 0),
        sAllocated((byte) 1),
        sUsing((byte)2),
        sOffline((byte)3),
        sFailure((byte)4);

        private SlotStatus(final byte value){
            value_ = value;
        }

        public byte getValue() {
            return value_;
        }

        private final byte value_;
    }

    public class Node {
        public String id;
        public String host;
        public int port;
        public Slot[] slots;
        public NodeStatus status;
    }

    public class Slot {
        public String id;
        public String dataPath;
        public SlotRole role;
        public SlotStatus status;
    }

    public class RGroupItem {
        public String id;
        public int hashValue;
        public Slot[] slots;
    }
    // Distribution is RGroupItem[]

    public RGroupItem[] dataDist;
    public Node[] dataNodes;
    public Node[] metaNodes;
*/
}
