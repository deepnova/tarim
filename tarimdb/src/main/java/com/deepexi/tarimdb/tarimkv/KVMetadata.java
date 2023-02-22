package com.deepexi.tarimdb.tarimkv;

import java.util.List;
import java.util.Iterator;
import java.lang.StringBuilder;
import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimKVProto.DataDistributionRequest;
import com.deepexi.rpc.TarimKVProto.DataDistributionResponse;
import com.deepexi.rpc.TarimKVProto.DistributionInfo;
import com.deepexi.rpc.TarimKVProto.StatusResponse;

public class KVMetadata {

    public String id;
    public String metaMode;
    public String address;
    public int port;
    public String role;
    public List<TarimKVProto.Node> mnodes;
    public List<TarimKVProto.RGroupItem> rgroups;
    public List<TarimKVProto.Node> dnodes;

    private DistributionInfo dataDist;

    public static<T> Iterable<T> iteratorToIterable(Iterator<T> iterator)
    {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return iterator;
            }
        };
    }

    public DistributionInfo toDistributionInfo(boolean rebuild){
        if(dataDist == null || rebuild == true){
            DistributionInfo.Builder distBuilder = DistributionInfo.newBuilder();
            distBuilder.addAllRgroups(iteratorToIterable(rgroups.iterator()));
            distBuilder.addAllDnodes(iteratorToIterable(dnodes.iterator()));
            dataDist = distBuilder.build();
        }
        return dataDist;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{id=");       sb.append(this.id);
        sb.append(",metaMode="); sb.append(this.metaMode);
        sb.append(",address=");  sb.append(this.address);
        sb.append(",port=");     sb.append(this.port);
        sb.append(",role=");     sb.append(this.role);

        sb.append(",mnodes=[");
        for(TarimKVProto.Node node : this.mnodes){
            sb.append("{id=");     sb.append(node.getId());
            sb.append(",host=");   sb.append(node.getHost());
            sb.append(",port=");   sb.append(node.getPort());
            sb.append(",status="); sb.append(node.getStatus());
            sb.append("}");
        }
        sb.append("]");

        sb.append(",rgroups=[");
        for(TarimKVProto.RGroupItem group : this.rgroups){
            sb.append("{id=");          sb.append(group.getId());
            sb.append(",hashValue=");   sb.append(group.getHashValue());
            sb.append(",slots=[");
                for(TarimKVProto.Slot slot : group.getSlotsList()){
                    sb.append("{id=");          sb.append(slot.getId());
                    sb.append(",role=");        sb.append(slot.getRole());
                    sb.append("}");
                }
            sb.append("]}");
        }
        sb.append("]");

        sb.append(",dnodes=[");
        for(TarimKVProto.Node node : this.dnodes){
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
        }
        sb.append("]");

        sb.append("}");
        return sb.toString();
    }

    public static String ObjToString(TarimKVProto.RGroupItem rgItem) {
        StringBuilder sb = new StringBuilder();
        sb.append("{id=");   sb.append(rgItem.getId());
        sb.append(",hashValue=");       sb.append(rgItem.getHashValue());
        sb.append(",slots=[");
            for(TarimKVProto.Slot slot : rgItem.getSlotsList()){
                sb.append("{id=");      sb.append(slot.getId());
                sb.append(",role=");    sb.append(slot.getRole());
                sb.append("}");
            }
        sb.append("]}");
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
}
