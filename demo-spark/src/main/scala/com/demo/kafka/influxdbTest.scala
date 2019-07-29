package com.demo.kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.izettle.metrics.influxdb.{InfluxDbHttpSender, InfluxDbReporter, InfluxDbSender}

import scala.collection.JavaConversions

/**
  * Created by pengxingxiong@ruijie.com.cn on 2019/7/26 11:34
  */
object influxdbTest {
  val property = new Properties()
  val registry = new MetricRegistry()
  var reporter: InfluxDbReporter = _

  val INFLUX_DEFAULT_TIMEOUT = 1000 // milliseconds
  val INFLUX_DEFAULT_PERIOD = 10
  val INFLUX_DEFAULT_UNIT = TimeUnit.SECONDS
  val INFLUX_DEFAULT_PROTOCOL = "http"
  val INFLUX_DEFAULT_PREFIX = ""
  val INFLUX_DEFAULT_TAGS = ""

  val INFLUX_KEY_PROTOCOL = "protocol"
  val INFLUX_KEY_HOST = "host"
  val INFLUX_KEY_PORT = "port"
  val INFLUX_KEY_PERIOD = "period"
  val INFLUX_KEY_UNIT = "unit"
  val INFLUX_KEY_DATABASE = "database"
  val INFLUX_KEY_AUTH = "auth"
  val INFLUX_KEY_PREFIX = "prefix"
  val INFLUX_KEY_TAGS = "tags"
  var pollPeriod: Int = _
  var pollUnit: TimeUnit = _

  def propertyToOption(prop: String): Option[String] = Option(property.getProperty(prop))


  def main(args: Array[String]): Unit = {
    property.setProperty(INFLUX_KEY_HOST,"172.18.135.11")
    property.setProperty(INFLUX_KEY_PORT,"30200")
    property.setProperty(INFLUX_KEY_DATABASE,"spark")
    init()
    start()
    report()
  }

  def init(): Unit = {
    if (propertyToOption(INFLUX_KEY_HOST).isEmpty) {
      throw new Exception("InfluxDb sink requires 'host' property.")
    }
    if (propertyToOption(INFLUX_KEY_PORT).isEmpty) {
      throw new Exception("InfluxDb sink requires 'port' property.")
    }
    if (propertyToOption(INFLUX_KEY_DATABASE).isEmpty) {
      throw new Exception("InfluxDb sink requires 'database' property.")
    }

    pollPeriod = propertyToOption(INFLUX_KEY_PERIOD)
      .map(_.toInt)
      .getOrElse(INFLUX_DEFAULT_PERIOD)

    pollUnit = propertyToOption(INFLUX_KEY_UNIT)
      .map(s => TimeUnit.valueOf(s.toUpperCase))
      .getOrElse(INFLUX_DEFAULT_UNIT)

    val protocol = propertyToOption(INFLUX_KEY_PROTOCOL).getOrElse(INFLUX_DEFAULT_PROTOCOL)
    val host = propertyToOption(INFLUX_KEY_HOST).get
    val port = propertyToOption(INFLUX_KEY_PORT).get.toInt
    val database = propertyToOption(INFLUX_KEY_DATABASE).get
    val auth = property.getProperty(INFLUX_KEY_AUTH)
    val prefix = propertyToOption(INFLUX_KEY_PREFIX).getOrElse(INFLUX_DEFAULT_PREFIX)
    val tags = propertyToOption(INFLUX_KEY_TAGS).getOrElse(INFLUX_DEFAULT_TAGS)


    val sender: InfluxDbSender = new InfluxDbHttpSender(protocol, host, port, database, auth,
      TimeUnit.MILLISECONDS, INFLUX_DEFAULT_TIMEOUT, INFLUX_DEFAULT_TIMEOUT, prefix)

    val defaultTags = Seq(
      "host" -> "localhost",
      "appId" -> "pengxxTest")
    val allTags = defaultTags.toMap

    reporter = InfluxDbReporter.forRegistry(registry)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .withTags(JavaConversions.mapAsJavaMap(allTags))
      .groupGauges(true)
      .build(sender)
  }

  def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  def stop() {
    reporter.stop()
  }

  def report() {
    reporter.report()
  }
}
