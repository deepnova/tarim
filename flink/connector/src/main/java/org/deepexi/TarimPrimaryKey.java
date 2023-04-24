package org.deepexi;

import org.apache.iceberg.StructLike;

import java.io.Serializable;
import org.apache.iceberg.*;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

import java.lang.reflect.Array;
import java.util.List;

public class TarimPrimaryKey implements StructLike, Serializable {
    private final int size;
    private final Object[] primaryKeyTuple;
    private final Accessor<StructLike>[] accessors;
    private List<String> primaryKeys;
    private List<Type.TypeID> types;

    public TarimPrimaryKey(List<String> primaryKeys, List<Type.TypeID> types, List<Integer> ids, Schema schema){
        this.size = primaryKeys.size();
        this.primaryKeyTuple = new Object[this.size];
        this.accessors = (Accessor[]) Array.newInstance(Accessor.class, this.size);
        this.primaryKeys = primaryKeys;
        this.types = types;
        for(int i = 0; i < this.size; ++i) {
            Accessor<StructLike> accessor = schema.accessorForField(ids.get(i));
            this.accessors[i] = accessor;
        }

    }

    public Object[] primaryData(StructLike row){

        Object object[] = new Object[this.size];
        for (int i = 0; i < this.size; i++){
            object[i] = this.accessors[i].get(row);
        }
        return object;

    }
    public List<String> getPrimaryKeys(){
        return this.primaryKeys;
    }

    public List<Type.TypeID> getTypes(){
        return this.types;
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public <T> T get(int i, Class<T> aClass) {
        return aClass.cast(this.primaryKeyTuple[i]);
    }

    @Override
    public <T> void set(int i, T value) {
        this.primaryKeyTuple[i] = value;
    }
}
