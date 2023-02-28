# TarimDB

simple try:
```
mvn compile

// run meta node
mvn exec:java -Dexec.mainClass="com.deepexi.TarimServer" -Dexec.args='-m mnode -f meta.yaml'
// run data node
mvn exec:java -Dexec.mainClass="com.deepexi.TarimServer" -Dexec.args='-m dnode -f kv.yaml'
```

### ldb

```
ldb --db=target/slot1 list_column_families
ldb --db=target/slot1 get '1_key-1_1' --column_family=100

```
