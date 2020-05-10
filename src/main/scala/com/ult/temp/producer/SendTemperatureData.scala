package com.ult.temp.producer

/** Provides object for generating the random temperature records
  * and to produce to kafka topic
  *
  * ==Overview==
  * for handling Json data , we are importing modules from play framework [[play.api.libs.json. {Json, OWrites}]]
  * implicit writes are needed for the json conversion as below
  * refer to the link https://www.playframework.com/documentation/2.7.x/ScalaJson
  * {{{
  *  scala> implicit val writesDataPoint: OWrites[DataPoint] = Json.writes[DataPoint]
  *  scala> val value = Json.toJson(event).toString()
  *
  * }}}
  *
  * If you include [[com.typesafe.config.ConfigFactory]], you can
  * read variable values from application.conf file by default
  */

import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.{JsValue, Json, OWrites, Writes}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory

case class DataPoint(data: TemperatureDevice)

object SendTemperatureData extends LazyLogging {

  /** Provides the required parameters for Kafka producer
    *
    * all the kafka producer configuration
    * can be added here
    */

  def getProducerProperties: Properties = {

    val props = new Properties()
    props.put("bootstrap.servers", ConfigFactory.load().getString("application.kafka.brokers"))
    props.put("key.serializer", ConfigFactory.load().getString("application.key.serializer"))
    props.put("value.serializer", ConfigFactory.load().getString("application.value.serializer"))
    props.put("linger.ms", ConfigFactory.load().getString("application.linger.ms"))
    return props

  }

  /** Used to create the random deviced ids
    *
    * numDevices can be fond in the
    * application.conf file in resources folder
    */

  def createDevices(numDevices: Int): util.ArrayList[TemperatureDevice] = {

    var eventTempDevice: util.ArrayList[TemperatureDevice] = new util.ArrayList()
    (1 to numDevices).foreach { n =>
      eventTempDevice.add(new TemperatureDevice().createDevice())

    }
    return eventTempDevice

  }

  def main(args: Array[String]): Unit = {

    val logger = Logger(LoggerFactory.getLogger(this.getClass))
    val producer = new KafkaProducer[String, String](getProducerProperties) // Kafka Producer
    val topic = ConfigFactory.load().getString("application.topic.name")
    var devices = createDevices(ConfigFactory.load().getInt("application.num.devices"))

    implicit val iotDataWrites: Writes[TemperatureDevice] = new Writes[TemperatureDevice] {  //implicit writes for Json conversion
      def writes(iot: TemperatureDevice): JsValue = Json.obj(
        "deviceId" -> iot.deviceId,
        "temperature" -> iot.temperature,
        "location" -> iot.location,
        "time" -> iot.time
      )
    }

    implicit val writesDataPoint: OWrites[DataPoint] = Json.writes[DataPoint]

    try {

      while (true) {

        (0 until devices.size()).foreach { element =>
          val event: DataPoint = DataPoint(devices.get(element).genarateRecord()) // generating 3 data points for 3 devices

          val record = new ProducerRecord(topic, "TempratureDevice", Json.toJson(event).toString()) // producing to kafka topic
          val Metadata = producer.send(record).get()

          logger.info("record produced to kafka topic with offset: " + Metadata.offset() + " value : " + record.value())

        }

      }
    }
    catch {
      case e: Exception => logger.info(e.getMessage)
    }
    finally {
      producer.close()
    }

  }

}
