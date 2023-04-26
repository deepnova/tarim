package org.deepexi;

import org.apache.iceberg.StructLike;

import java.io.Serializable;
import org.apache.iceberg.*;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
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

    public String codecPrimaryValue(List<String> primaryKeyValues){
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < primaryKeyValues.size();i++){
            Type.TypeID id = types.get(i);
            switch (id){
                case INTEGER:
                    buf.append(keyIntBase16Codec(Integer.parseInt(primaryKeyValues.get(i))));
                    //buf.append(keyLongBase16Codec(Long.parseLong(primaryKeyValues.get(i))));
                    break;
                case LONG:
                    buf.append(keyLongBase16Codec(Long.parseLong(primaryKeyValues.get(i))));
                    break;
                case STRING:
                    buf.append(primaryKeyValues.get(i));
                    break;
                default:
                    throw new RuntimeException("un support primary type!");

            }
        }
        return buf.toString();
    }

    public String keyLongBase16Codec(long value) {
        value = value ^ 0x8000000000000000L;

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(value);
        byte[] bytes = buffer.array();
        char[] chars = new char[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            chars[i] = (char) bytes[i];
        }

        return new String(chars);
    }

    public String keyIntBase16Codec(int value) {
        value = value ^ 0x80000000;

        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        byte[] bytes = buffer.array();

        char[] chars = new char[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            chars[i] = (char) bytes[i];
        }

        return new String(chars);
    }
}
