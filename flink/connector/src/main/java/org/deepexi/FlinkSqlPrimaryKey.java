package org.deepexi;

import java.util.Objects;

public class FlinkSqlPrimaryKey {
    private String primaryKey;
    private String value;
    public FlinkSqlPrimaryKey(String primaryKey, String value){
        this.primaryKey = primaryKey;
        this.value = value;
    }

    public String getPrimaryKey(){
        return this.primaryKey;
    }

    public String getValue(){
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
        return key.primaryKey == this.primaryKey && key.value == key.value;
    }

    public int hashCode(){
        return Objects.hash(primaryKey + value);
    }
}
