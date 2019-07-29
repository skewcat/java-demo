package com.demo.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.slf4j.LoggerFactory

object KafkaDemo {
  val LOG = LoggerFactory.getLogger(KafkaDemo.getClass)
  val KAFKA_BOOTSTRAP_SERVERS = "172.18.135.2:9092"
  //  val TOPIC = "dev-sysinfo,dev-terminal,dev-usage,dev-syslog"
  val TOPIC = "dev-syslog"

  def sql_type(): Unit = {
    val conf = new SparkConf()
//    conf.set("spark.driver.host", "localhost")
    val spark = SparkSession.builder().config(conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val groupId = "packet-pull".+(System.currentTimeMillis())
    val df = spark
      .readStream.format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", TOPIC)
      .option("group.id", groupId)
      .option("auto.offset.reset", "earliest")
      .option("enable.auto.commit", false: java.lang.Boolean)
      .option("max.poll.records", "500").load()
    val sqlExprs = "CAST(value AS STRING)"
    val query = df.selectExpr(sqlExprs).writeStream.outputMode("append")
      .format("console")
      .option("truncate",value = false).start()
    query.awaitTermination()
  }

  def rdd_type(): Unit = {
    val conf = new SparkConf()
//    conf.set("spark.driver.host", "localhost")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.sparkContext.setLogLevel("WARN")
    val brokers = KAFKA_BOOTSTRAP_SERVERS
    val topics = TOPIC
    val topicsSet = topics.split(",").toSet
    val groupId = "packet-pull".+(System.currentTimeMillis())
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "max.poll.records" -> "500"
    )
    LOG.info("kafkaParams=" + kafkaParams)
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val lines = messages.map(_.value)
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    rdd_type()
  }

}
