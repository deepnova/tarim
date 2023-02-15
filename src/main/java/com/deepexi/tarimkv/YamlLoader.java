package com.deepexi.tarimkv;

import com.deepexi.util.TLog;
import com.deepexi.util.Status;
import com.deepexi.rpc.TarimKVMetaSvc;

import java.io.InputStream;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
//import com.google.common.primitives.UnsignedInteger;

/**
 * YamlLoader
 *  
 */
public class YamlLoader {

    //@SuppressWarnings(value="unchecked")
    public static Status load(String filename, KVMetadata metaConf) {
/*
        Yaml yaml = new Yaml(new Constructor(KVMetadata.class));
        InputStream inputStream = YamlLoader.class
                                      .getClassLoader()
                                      .getResourceAsStream(filename);

        KVMetadata meta = yaml.load(inputStream);
        TLog.debug("id: " + meta.id);
*/

        Yaml yaml = new Yaml();

        InputStream inputStream = YamlLoader.class
                                      .getClassLoader()
                                      .getResourceAsStream(filename);

        Map<String, Object> obj = yaml.load(inputStream);
        TLog.debug(obj.toString());

        if(metaConf == null){
            //metaConf = new KVMetadata(); // not working
            TLog.error("yaml file load faild: metaConf is null. filename: " + filename);
            return Status.NULL_POINTER;
        }
        metaConf.id = obj.get("id").toString();
        metaConf.metaMode = obj.get("metaMode").toString();
        metaConf.address = obj.get("address").toString();
        metaConf.port = Integer.valueOf(obj.get("port").toString());
        metaConf.role = obj.get("role").toString();
        metaConf.mnodes = new ArrayList();
        metaConf.rgroups = new ArrayList();
        metaConf.dnodes = new ArrayList();

        TarimKVMetaSvc.Node.Builder nodeBuiler = TarimKVMetaSvc.Node.newBuilder();
        TarimKVMetaSvc.Slot.Builder slotBuiler = TarimKVMetaSvc.Slot.newBuilder();
        TarimKVMetaSvc.RGroupItem.Builder rgBuiler = TarimKVMetaSvc.RGroupItem.newBuilder();
        List<Map<String,Object>> objs = (List<Map<String,Object>>) obj.get("dnodes");
        for(Map<String, Object> node: objs) {
            nodeBuiler.setId(node.get("id").toString());
            nodeBuiler.setHost(node.get("host").toString());
            nodeBuiler.setPort(Integer.valueOf(node.get("port").toString()));
            nodeBuiler.setStatus(TarimKVMetaSvc.NodeStatus.forNumber(Integer.valueOf(node.get("status").toString())));

            List<Map<String,Object>> slots = (List<Map<String,Object>>) node.get("slots");
            for(int i = 0; i < slots.size(); i++) {
                Map<String, Object> slot = slots.get(i);
                slotBuiler.setId(slot.get("id").toString());
                slotBuiler.setDataPath(slot.get("dataPath").toString());

                /*TLog.debug("dnode id: " + node.get("id").toString() 
                         + ", slot id: " + slot.get("id").toString()
                         + ", dataPath: " + slot.get("dataPath").toString()
                         + ", role: " + slot.get("role").toString());*/

                slotBuiler.setRole(TarimKVMetaSvc.SlotRole.forNumber(Integer.valueOf(slot.get("role").toString())));
                slotBuiler.setStatus(TarimKVMetaSvc.SlotStatus.forNumber(Integer.valueOf(slot.get("status").toString())));
                nodeBuiler.addSlots(i, slotBuiler.build());
            }
            metaConf.dnodes.add(nodeBuiler.build());
        }

        objs = (List<Map<String,Object>>) obj.get("rgroups");
        for(Map<String, Object> group: objs) {
            rgBuiler.setId(group.get("id").toString());
            rgBuiler.setHashValue(Long.valueOf(group.get("hashValue").toString()));
            List<Map<String,Object>> slots = (List<Map<String,Object>>) group.get("slots");
            slotBuiler.setDataPath("");
            slotBuiler.setStatus(TarimKVMetaSvc.SlotStatus.SS_IDLE);
            for(int i = 0; i < slots.size(); i++) {
                Map<String, Object> slot = slots.get(i);
                slotBuiler.setId(slot.get("id").toString());
                slotBuiler.setRole(TarimKVMetaSvc.SlotRole.forNumber(Integer.valueOf(slot.get("role").toString())));
                rgBuiler.addSlots(i, slotBuiler.build());
            }
            metaConf.rgroups.add(rgBuiler.build());
        }

        TLog.debug("MetaConfig: " + metaConf.toString());

        return Status.OK;
    }
}

