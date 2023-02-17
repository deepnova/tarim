# TarimDB

simple try:
```
mvn compile

// run meta node
mvn exec:java -Dexec.mainClass="com.deepexi.TarimServer" -Dexec.args='-m mnode -f meta.yaml'
// run data node
mvn exec:java -Dexec.mainClass="com.deepexi.TarimServer" -Dexec.args='-m dnode -f kv.yaml'
```

