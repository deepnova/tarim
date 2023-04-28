package org.deepexi;

import java.util.Objects;

public class FlinkSqlPrimaryKey<T> {
    private String primaryKey;
    private T value;
    public FlinkSqlPrimaryKey(String primaryKey, T value){
        this.primaryKey = primaryKey;
        this.value = value;
    }

    public String getPrimaryKey(){
        return this.primaryKey;
    }

    public T getValue(){
        return this.value;
    }

    public boolean equals(Object o){
        if (this == o){
            return true;
        }

        if (o == null || getClass() != o.getClass()){
            return false;
        }

        FlinkSqlPrimaryKey key = (FlinkSqlPrimaryKey)o;
        return key.primaryKey == this.primaryKey && key.value.equals(this.value);


    }

    public int hashCode(){
        return Objects.hash(primaryKey);
    }
}
