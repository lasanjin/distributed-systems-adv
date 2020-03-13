## Data Streaming 
For this lab, we developed a streaming query that analyses data from the [Linear Road Benchmark](http://www.vldb.org/conf/2004/RS12P1.PDF) and deploy that query in a Flink cluster.

### [Local setup tutorial](https://ci.apache.org/projects/flink/flink-docs-release-1.10/getting-started/tutorials/local_setup.html#start-a-local-flink-cluster) with [project template for Java](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/projectsetup/java_api_quickstart.html)
1. [Download](https://flink.apache.org/downloads.html) and unpack Flink
```
$ wget https://www.nic.funet.fi/pub/mirrors/apache.org/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz | tar xzf
```

2. Create a project
```
$ curl https://flink.apache.org/q/quickstart.sh | bash -s 1.10.0
```

3. Build project
```
$ cd quickstart/
$ mvn clean package
```

4. Start Flink
   - In case of `OutOfMemoryError` in Flink TaskManager when processing input file
     - Edit conf file `FLINK_DIR/conf/flink-conf.yaml` 
       - Set `taskmanager.memory.flink.size: 2560m`
```
$ ./flink-1.10.0/bin/start-cluster.sh 
```

5. Use netcat to start local server
```
$ nc -l 9000
```

6. Submit the Flink program:
```
$ ./flink-1.10.0/bin/flink run quickstart/target/quickstart-0.1.jar --port 9000
```

7. Stop Flink
```
$ ./flink-1.10.0/bin/stop-cluster.sh
```

### [Flink DataStream API Programming Guide](https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/datastream_api.html)
