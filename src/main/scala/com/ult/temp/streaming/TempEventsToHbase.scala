package com.ult.temp.streaming

/** Describes the flow of temprature events
  * reading from kafka topic and writing to Hbase table
  *
  * ==Overview==
  * contains multiple methods for kafkaconsumer properties, Habse connection configuration
  *
  * If you include [[com.typesafe.config.ConfigFactory]], you can
  * read variable values from application.conf file by default
  */

import java.text.SimpleDateFormat
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import scala.util.parsing.json._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory

object TempEventsToHbase extends LazyLogging {

  /** Provides the required parameters for Kafka Consumer
    *
    * all the kafka consumer configuration
    * can be added here
    */

  def getConsumerProperties: Map[String, Object] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigFactory.load().getString("application.kafka.brokers"),
      "key.deserializer" -> ConfigFactory.load().getString("application.key.deserializer"),
      "value.deserializer" -> ConfigFactory.load().getString("application.value.deserializer"),
      "group.id" -> ConfigFactory.load().getString("application.group.id"),
      "auto.offset.reset" -> ConfigFactory.load().getString("application.auto.offset.reset"),
      "enable.auto.commit" -> ConfigFactory.load().getString("application.enable.auto.commit"))
    return kafkaParams
  }

  /** Provides the required parameters for establishing Hbase connection
    *
    * all the Hbase connection configuration
    * can be added here
    */

  def getHbaseConfiguration: Configuration = {

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.master", ConfigFactory.load().getString("application.hbase.master"))
    hbaseConf.setInt("timeout", ConfigFactory.load().getInt("application.timeout"))
    hbaseConf.set("hbase.zookeeper.quorum", ConfigFactory.load().getString("application.hbase.zookeeper.quorum"))
    hbaseConf.set("hbase.security.authentication", ConfigFactory.load().getString("application.hbase.security.authentication"))
    return hbaseConf
  }

  def main(args: Array[String]): Unit = {

    val logger = Logger(LoggerFactory.getLogger(this.getClass))
    val conf = new SparkConf().setMaster("local[*]").setAppName("Temperature events to Hbase")
    val ssc = new StreamingContext(conf, Seconds(1)) // Streaming context

    val topics = Array(ConfigFactory.load().getString("application.topic.name"))
    val tableName = ConfigFactory.load().getString("application.hbase.table.name")

    try {
      // Creating kafka stream
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, getConsumerProperties)
      )

      stream.foreachRDD(rdd => {
        rdd.foreachPartition(iter => {

          val conn = ConnectionFactory.createConnection(getHbaseConfiguration) // need to establish connection to each worker node
          val hTable: Table = conn.getTable(TableName.valueOf(tableName))

          iter.foreach(record => {

            val value = record.value().toString

            JSON.parseFull(value).foreach {

              case json: Map[String, Map[String, Map[String, Double]]] =>
                println(value)

                // converting time to the required format
                val time = json("data").get("time").getOrElse()
                val pattern = ConfigFactory.load().getString("application.date.format")
                val dateObject = new SimpleDateFormat(pattern)
                val desiredTimeFormat = dateObject.format(time)
                println(s"desiredTimeFormat: $desiredTimeFormat")

                // mapping the data to the fields
                val deviceId = json("data").get("deviceId").getOrElse().toString
                val temperature = json("data").get("temperature").getOrElse().toString
                val latitude = json("data").get("location").get("latitude").toString
                val longitude = json("data").get("location").get("longitude").toString

                // assigning consumer record hashcode as the row key for Hbase table
                val rowKey = record.hashCode().toString
                //println(rowKey)

                val thePut = new Put(Bytes.toBytes(rowKey))
                thePut.addColumn(Bytes.toBytes("temperatureCF"), Bytes.toBytes("deviceId"), Bytes.toBytes(deviceId))
                thePut.addColumn(Bytes.toBytes("temperatureCF"), Bytes.toBytes("temperature"), Bytes.toBytes(temperature))
                thePut.addColumn(Bytes.toBytes("locationCF"), Bytes.toBytes("latitude"), Bytes.toBytes(latitude))
                thePut.addColumn(Bytes.toBytes("locationCF"), Bytes.toBytes("longitude"), Bytes.toBytes(longitude))
                thePut.addColumn(Bytes.toBytes("temperatureCF"), Bytes.toBytes("time"), Bytes.toBytes(desiredTimeFormat))
                thePut.addColumn(Bytes.toBytes("entityCF"), Bytes.toBytes("datapoint"), Bytes.toBytes(value))

                // writing to Hbase table
                hTable.put(thePut)
                System.out.println(s"record is inserted into table: ${hTable.getName}")

            }

          })

        })

      })
    }
    catch {
      case e: Exception => logger.info(e.getMessage)
    }

    ssc.start() // Start the computation
    ssc.awaitTermination()

  }
}
