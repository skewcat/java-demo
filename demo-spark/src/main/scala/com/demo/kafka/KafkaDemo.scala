package com.demo.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object KafkaDemo {
  val KAFKA_BOOTSTRAP_SERVERS = "172.18.135.11:9092,172.18.135.12:9092,172.18.135.13:9092"
  val TOPIC = "dev-sysinfo,dev-terminal,dev-usage,dev-syslog"

  def sql_type(): Unit = {
    val spark = SparkSession.builder().appName("kafkaDeme").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", TOPIC)
      .load()
    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()
  }

  def rdd_type(): Unit = {
    val conf = new SparkConf().setAppName("kafkaDeme").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    val brokers = KAFKA_BOOTSTRAP_SERVERS
    val topics = TOPIC
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val lines = messages.map(_.value)
    lines.print()
    lines.count().print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    rdd_type()
  }
}
