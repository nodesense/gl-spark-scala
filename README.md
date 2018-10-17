# SBT

> sbt

> assembly



# Hadoop

50070 is default UI port of Namenode for http. http://nodesense.local:50070/

YARN should be here http://nodesense.local:8088/cluster

Yarn port number 8032 http://nodesense.local:8042/node



```
hdfs dfs -mkdir  /data
hdfs dfs -put data /

hadoop fs -copyFromLocal /path/in/linux /hdfs/path

hdfs dfs -ls /
hdfs dfs -ls /data
hdfs dfs -ls /data/minimal

hdfs dfs -ls /data/minimal

```


hdfs dfsadmin -safemode leave


# Spark Master setup

start-master.sh

MasterURL: `spark://nodesense.local:7077`

MasterWebUI: `http://nodesense.local:8080`


start-slave.sh spark://nodesense.local:7077




spark-submit \
  --class com.gl.OrderProcessing \
  --master local[8] \
  target/scala-2.11/GlobalLogic-Spark-assembly-0.1.jar \
  100



# Run on a Spark standalone cluster in client deploy mode
```
spark-submit \
  --class com.gl.OrderProcessing \
  --master spark://nodesense.local:7077 \
  --executor-memory 8G \
  --total-executor-cores 100 \
  target/scala-2.11/GlobalLogic-Spark-assembly-0.1.jar \
  1000
```

```
 spark-submit \
  --class com.gl.OrderProcessing \
  --master yarn \
  --deploy-mode client \
  --executor-memory 4G \
  --num-executors 2 \
    target/scala-2.11/GlobalLogic-Spark-assembly-0.1.jar \
    1000
```

```
 spark-submit \
  --class  com.gl.OrderProcessing \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 2G \
  --num-executors 2 \
    target/scala-2.11/GlobalLogic-Spark-assembly-0.1.jar \
  1000
```