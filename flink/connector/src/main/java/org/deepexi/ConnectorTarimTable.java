package org.deepexi;

import org.apache.iceberg.*;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ConnectorTarimTable implements Table, Serializable {
    String name;
    ConnectorTarimTable(String name){
        this.name = name;
    }

    @Override
    public void refresh() {

    }

    @Override
    public TableScan newScan() {
        return null;
    }

    @Override
    public Schema schema() {
        return null;
    }

    @Override
    public Map<Integer, Schema> schemas() {
        return null;
    }

    @Override
    public PartitionSpec spec() {
        return null;
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        return null;
    }

    @Override
    public SortOrder sortOrder() {
        return null;
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        return null;
    }

    @Override
    public Map<String, String> properties() {
        return null;
    }

    @Override
    public String location() {
        return null;
    }

    @Override
    public Snapshot currentSnapshot() {
        return null;
    }

    @Override
    public Snapshot snapshot(long l) {
        return null;
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        return null;
    }

    @Override
    public List<HistoryEntry> history() {
        return null;
    }

    @Override
    public UpdateSchema updateSchema() {
        return null;
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
        return null;
    }

    @Override
    public UpdateProperties updateProperties() {
        return null;
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
        return null;
    }

    @Override
    public UpdateLocation updateLocation() {
        return null;
    }

    @Override
    public AppendFiles newAppend() {
        return null;
    }

    @Override
    public RewriteFiles newRewrite() {
        return null;
    }

    @Override
    public RewriteManifests rewriteManifests() {
        return null;
    }

    @Override
    public OverwriteFiles newOverwrite() {
        return null;
    }

    @Override
    public RowDelta newRowDelta() {
        return null;
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
        return null;
    }

    @Override
    public DeleteFiles newDelete() {
        return null;
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
        return null;
    }

    @Override
    public Rollback rollback() {
        return null;
    }

    @Override
    public ManageSnapshots manageSnapshots() {
        return null;
    }

    @Override
    public Transaction newTransaction() {
        return null;
    }

    @Override
    public FileIO io() {
        return null;
    }

    @Override
    public EncryptionManager encryption() {
        return null;
    }

    @Override
    public LocationProvider locationProvider() {
        return null;
    }
}
