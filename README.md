# Tarim

simple try:
```
mvn compile

// run meta node
mvn exec:java -Dexec.mainClass="com.deepexi.TarimServer" -Dexec.args='-m dnode -f meta.yaml'
// run data node
mvn exec:java -Dexec.mainClass="com.deepexi.TarimServer" -Dexec.args='-m dnode -f meta.yaml'
```
