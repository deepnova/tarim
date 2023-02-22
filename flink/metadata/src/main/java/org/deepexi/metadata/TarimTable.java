package org.deepexi.metadata;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;

public class TarimTable extends BaseTable {

    public TableOperations ops;
    public TarimTable(TableOperations ops, String name){
        super(ops, name);
        this.ops = ops;
    }

    public FileIO io() {
        return this.ops().io();
    }
    TableOperations ops() {return ops;}
}