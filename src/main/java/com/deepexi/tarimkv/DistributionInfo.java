package com.deepexi.tarimkv;

public class DistributionInfo {

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
}
