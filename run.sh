#!/bin/sh
 
run_v1() 
{
    classpath="${classpath}target/classes/"
    classpath="${classpath}:/home/deepliu/lib/rocksdbjni-7.6.0-linux64.jar"
    classpath="${classpath}:/home/deepliu/.m2/repository/commons-cli/commons-cli/1.5.0/commons-cli-1.5.0.jar"
    classpath="${classpath}:/home/deepliu/.m2/repository/org/apache/logging/log4j/log4j-core/2.19.0/log4j-core-2.19.0.jar"
    classpath="${classpath}:/home/deepliu/.m2/repository/org/apache/logging/log4j/log4j-api/2.19.0/log4j-api-2.19.0.jar"
    classpath="${classpath}:/home/deepliu/.m2/repository/io/grpc/grpc-api/1.52.1/grpc-api-1.52.1.jar"
    classpath="${classpath}:/home/deepliu/.m2/repository/io/grpc/grpc-netty-shaded/1.52.1/grpc-netty-shaded-1.52.1.jar"
    
    params=$@ # e.j: -m mnode -f /path/to/file
    cmd="java -cp ${classpath} com.deepexi.TarimServer $@"
    
    ${cmd}
}

run_v2() 
{
    params=$@ # e.j: -m mnode -f /path/to/file
    cmd="mvn exec:java -Dexec.mainClass='com.deepexi.TarimServer' -Dexec.args='$@'"
    
    echo "${cmd}"
    ${cmd}
}

run_v2 $@
