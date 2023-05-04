package com.deepexi.tarimdb.tarimkv;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.rpc.TarimKVProto;

import java.io.InputStream;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.yaml.snakeyaml.Yaml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import tarimmeta.KVMetadata;

/**
 * YamlLoader
 *  
 */
public class YamlLoader {

    public final static Logger logger = LogManager.getLogger(YamlLoader.class);

    //@SuppressWarnings(value="unchecked")
    public static Status loadMetaConfig(String filename, KVMetadata metadata) {

        Yaml yaml = new Yaml();
        InputStream inputStream = YamlLoader.class
                                      .getClassLoader()
                                      .getResourceAsStream(filename);

        Map<String, Object> obj = yaml.load(inputStream);
        logger.debug(obj.toString());

        if(metadata == null){
            //metadata = new KVMetadata(); // not working
            logger.error("yaml file load faild: metadata is null. filename: " + filename);
            return Status.NULL_POINTER;
        }
        metadata.id = obj.get("id").toString();
        metadata.metaMode = obj.get("metaMode").toString();
        metadata.address = obj.get("address").toString();
        metadata.port = Integer.valueOf(obj.get("port").toString());
        metadata.role = obj.get("role").toString();
        metadata.mnodes = new ArrayList();
        metadata.rgroups = new ArrayList();
        metadata.dnodes = new ArrayList();

        TarimKVProto.Node.Builder nodeBuiler = TarimKVProto.Node.newBuilder();
        TarimKVProto.Slot.Builder slotBuiler = TarimKVProto.Slot.newBuilder();
        TarimKVProto.RGroupItem.Builder rgBuiler = TarimKVProto.RGroupItem.newBuilder();
        List<Map<String,Object>> objs = (List<Map<String,Object>>) obj.get("dnodes");
        for(Map<String, Object> node: objs) {
            nodeBuiler.setId(node.get("id").toString());
            nodeBuiler.setHost(node.get("host").toString());
            nodeBuiler.setPort(Integer.valueOf(node.get("port").toString()));
            nodeBuiler.setStatus(TarimKVProto.NodeStatus.forNumber(Integer.valueOf(node.get("status").toString())));

            List<Map<String,Object>> slots = (List<Map<String,Object>>) node.get("slots");
            for(int i = 0; i < slots.size(); i++) {
                Map<String, Object> slot = slots.get(i);
                slotBuiler.setId(slot.get("id").toString());
                slotBuiler.setDataPath(slot.get("dataPath").toString());

                /*logger.debug("dnode id: " + node.get("id").toString() 
                         + ", slot id: " + slot.get("id").toString()
                         + ", dataPath: " + slot.get("dataPath").toString()
                         + ", role: " + slot.get("role").toString());*/

                slotBuiler.setRole(TarimKVProto.SlotRole.forNumber(Integer.valueOf(slot.get("role").toString())));
                slotBuiler.setStatus(TarimKVProto.SlotStatus.forNumber(Integer.valueOf(slot.get("status").toString())));
                nodeBuiler.addSlots(i, slotBuiler.build());
            }
            metadata.dnodes.add(nodeBuiler.build());
            nodeBuiler.clear();
        }

        objs = (List<Map<String,Object>>) obj.get("rgroups");
        for(Map<String, Object> group: objs) {
            rgBuiler.setId(group.get("id").toString());
            rgBuiler.setHashValue(Long.valueOf(group.get("hashValue").toString()));
            List<Map<String,Object>> slots = (List<Map<String,Object>>) group.get("slots");
            slotBuiler.setDataPath("");
            slotBuiler.setStatus(TarimKVProto.SlotStatus.SS_IDLE);
            for(int i = 0; i < slots.size(); i++) {
                Map<String, Object> slot = slots.get(i);
                slotBuiler.setId(slot.get("id").toString());
                slotBuiler.setRole(TarimKVProto.SlotRole.forNumber(Integer.valueOf(slot.get("role").toString())));
                rgBuiler.addSlots(i, slotBuiler.build());
            }
            metadata.rgroups.add(rgBuiler.build());
        }

        slotBuiler.clear();
        slotBuiler.setId(obj.get("metaSlotID").toString());
        slotBuiler.setDataPath(obj.get("metaSlotDataPath").toString());
        metadata.metaSlotConf = slotBuiler.build();

        logger.debug("MetaConfig: " + metadata.toString());

        return Status.OK;
    }

    public static Status loadDNodeConfig(String filename, KVLocalMetadata metadata) {

        Yaml yaml = new Yaml();
        InputStream inputStream = YamlLoader.class
                                      .getClassLoader()
                                      .getResourceAsStream(filename);

        Map<String, Object> obj = yaml.load(inputStream);
        logger.debug(obj.toString());

        if(metadata == null){
            logger.error("yaml file load faild: localMetaConf is null. filename: " + filename);
            return Status.NULL_POINTER;
        }

        metadata.id = obj.get("id").toString();
        metadata.address = obj.get("address").toString();
        metadata.port = Integer.valueOf(obj.get("port").toString());
        metadata.mnodes = new ArrayList();
        metadata.slots = new ArrayList();

        //init ,or else it will be nullptr
        metadata.mainAccount = new KVSchema.MainAccount();

        TarimKVProto.Node.Builder nodeBuiler = TarimKVProto.Node.newBuilder();
        TarimKVProto.Slot.Builder slotBuiler = TarimKVProto.Slot.newBuilder();

        List<Map<String,Object>> objs = (List<Map<String,Object>>) obj.get("mnodes");
        for(Map<String, Object> node: objs) {
            nodeBuiler.setId(node.get("id").toString());
            nodeBuiler.setHost(node.get("host").toString());
            nodeBuiler.setPort(Integer.valueOf(node.get("port").toString()));
            //nodeBuiler.role = node.get("role").toString();
            nodeBuiler.setStatus(TarimKVProto.NodeStatus.forNumber(Integer.valueOf(node.get("status").toString())));
            metadata.mnodes.add(nodeBuiler.build());
        }

        List<Map<String,Object>> slots = (List<Map<String,Object>>) obj.get("slots");
        for(int i = 0; i < slots.size(); i++) {
            Map<String, Object> slot = slots.get(i);
            slotBuiler.setId(slot.get("id").toString());
            slotBuiler.setDataPath(slot.get("dataPath").toString());
            slotBuiler.setRole(TarimKVProto.SlotRole.forNumber(Integer.valueOf(slot.get("role").toString())));
            slotBuiler.setStatus(TarimKVProto.SlotStatus.forNumber(Integer.valueOf(slot.get("status").toString())));
            metadata.slots.add(slotBuiler.build());
        }

        Map<String,Object> account = (Map<String,Object>) obj.get("mainAccount");

        metadata.mainAccount.accountType  = Integer.valueOf(account.get("accountType").toString());
        metadata.mainAccount.username = account.get("username").toString();
        metadata.mainAccount.token = account.get("token").toString();

        metadata.mainPath = obj.get("mainPath").toString();

        logger.debug("local metadata: " + metadata.toString());

        return Status.OK;
    }
}

