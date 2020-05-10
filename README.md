Task 1:

1.	Create Kafka topic for IoT data events
2.	Write a IoT data simulator or MQTT i.e. Kafka producer application which generate random data points (please refer to the SendTemperatureData.scala class in attachment)

MQTT rest api example:
 curl -s -X POST -H 'Content-Type: application/json' http://localhost:8083/connectors -d '{
    "name" : "mqtt-source",
"config" : {
    "connector.class" : "io.confluent.connect.mqtt.MqttSourceConnector",
    "tasks.max" : "1",
    "mqtt.server.uri" : "tcp://127.0.0.1:1883",
    "mqtt.topics" : "temperature",
    "kafka.topic" : "mqtt.temperature",
    "confluent.topic.bootstrap.servers": "localhost:9092",
    "confluent.topic.replication.factor": "1",
    "confluent.license":""
    }
}'

 Task 2: 
1.	Create a Hbase table with two column families as below
 

2.	Write a spark streaming application which reads events from kafka topic created above and change the time column to human readable format and store it in Hbase table created above. . (please refer to the TempEventsToHbase.scala class in attachment)
 
3.	Verify that data is produced to Hbase table 

4.	Create a hive table on top of hbase table created above and do the column mapping as below

5.	Open Impala shell and create a database named hbase

6.	Run the below command so that impala get to know about the table created in hive shell above.
 
7.	List few records to check whether impala can read data from Hbase table.

Task 3:
1.	The maximum temperatures measured for every device.
 
[quickstart.cloudera:21000] > select deviceid, max(temperature) from htable_temparature_events group by deviceid;
Query: select deviceid, max(temperature) from htable_temparature_events group by deviceid
Query submitted at: 2019-06-12 16:45:36 (Coordinator: http://quickstart.cloudera:25000)
Query progress can be monitored at: http://quickstart.cloudera:25000/query_plan?query_id=834561eaeed1027e:73289b6f00000000
+--------------------------------------+------------------+
| deviceid                             | max(temperature) |
+--------------------------------------+------------------+
| f4556146-2f99-4188-8e44-48bd9267d31b | 39               |
| 1bfb24c8-e8e3-477e-85d9-d4c6d41b51a4 | 39               |
| 3cb1f533-a6ed-4634-bf44-5617660f7291 | 39               |
+--------------------------------------+------------------+

2.	The amount of data points aggregated for every device.
 
[quickstart.cloudera:21000] > select deviceid, count(*) from htable_temparature_events group by deviceid;
Query: select deviceid, count(*) from htable_temparature_events group by deviceid
Query submitted at: 2019-06-12 16:48:17 (Coordinator: http://quickstart.cloudera:25000)
Query progress can be monitored at: http://quickstart.cloudera:25000/query_plan?query_id=e24a0c30c7d1d93a:9e1981800000000
+--------------------------------------+----------+
| deviceid                             | count(*) |
+--------------------------------------+----------+
| f4556146-2f99-4188-8e44-48bd9267d31b | 133      |
| 1bfb24c8-e8e3-477e-85d9-d4c6d41b51a4 | 133      |
| 3cb1f533-a6ed-4634-bf44-5617660f7291 | 133      |
+--------------------------------------+----------+
Fetched 3 row(s) in 0.32s

3.	The highest temperature measured on a given day for every device.
 
[quickstart.cloudera:21000] > select deviceid,substr(time, 1, 10) as day, max(temperature) as temp_max from htable_temparature_events group by day, deviceid;
Query: select deviceid,substr(time, 1, 10) as day, max(temperature) as temp_max from htable_temparature_events group by day, deviceid
Query submitted at: 2019-06-12 16:51:15 (Coordinator: http://quickstart.cloudera:25000)
Query progress can be monitored at: http://quickstart.cloudera:25000/query_plan?query_id=2b4d9237f97a17d1:33bc806100000000
+--------------------------------------+------------+----------+
| deviceid                             | day        | temp_max |
+--------------------------------------+------------+----------+
| 3cb1f533-a6ed-4634-bf44-5617660f7291 | 2019-06-13 | 39       |
| 1bfb24c8-e8e3-477e-85d9-d4c6d41b51a4 | 2019-06-13 | 39       |
| f4556146-2f99-4188-8e44-48bd9267d31b | 2019-06-13 | 39       |
+--------------------------------------+------------+----------+
Fetched 3 row(s) in 0.33s
