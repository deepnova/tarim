package org.deepexi;

import com.deepexi.TarimMetaClient;
import org.apache.flink.table.api.TableSchema;
import org.apache.iceberg.*;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.deepexi.source.ScanPartition;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ConnectorTarimTable implements Table, Serializable {
    private String name;
    private int tableId;
    private transient TableSchema flinkSchema;

    private List<String> partitionKey;

    private PartitionSpec partitionSpec;
    private Schema schema;
    private String schemaJson;
    private TarimPrimaryKey primaryKey;

    public TarimMetaClient metaClient;

    private List<ScanPartition> scanList;

    public ConnectorTarimTable(String name){
        this.name = name;
    }

    public ConnectorTarimTable(String name, int tableId, TableSchema flinkSchema, List<String> partitionKey, PartitionSpec partitionSpec, Schema schema, String schemaJson, TarimPrimaryKey tarimPrimaryKey){
        this.name = name;
        this.tableId = tableId;
        this.flinkSchema = flinkSchema;
        this.partitionKey = partitionKey;
        this.partitionSpec = partitionSpec;
        this.schema = schema;
        this.schemaJson = schemaJson;
        this.primaryKey = tarimPrimaryKey;
    }

    public void setMetaClient(TarimMetaClient metaClient){
        this.metaClient = metaClient;
    }

    public TarimMetaClient getMetaClient() {
        return this.metaClient;
    }
    public String getName(){return this.name;}

    public int getTableId(){return tableId;}

    public String getSchemaJson(){return schemaJson;}

    public TableSchema getFlinkSchema(){
        return flinkSchema;
    }


    public List<String> getPartitionKey(){
        return partitionKey;
    }

    public TarimPrimaryKey getPrimaryKey(){
        return primaryKey;
    }

    public void setScanList(List<ScanPartition> list){
        this.scanList = list;
    }

    public List<ScanPartition> getScanList(){
        return this.scanList;
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
        return this.schema;
    }

    @Override
    public Map<Integer, Schema> schemas() {
        return null;
    }

    @Override
    public PartitionSpec spec() {
        return this.partitionSpec;
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
