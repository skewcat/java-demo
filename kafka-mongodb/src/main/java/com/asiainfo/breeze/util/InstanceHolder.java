package com.asiainfo.breeze.util;

import java.util.*;
import java.util.concurrent.BlockingQueue;

import com.asiainfo.breeze.conf.Configration;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.asiainfo.breeze.consumer.ConsumerThread;
import com.asiainfo.breeze.consumer.ProducerThread;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

public class InstanceHolder {
    private static final Logger log = Logger.getLogger(InstanceHolder.class);
    public static String targetCollection;

    public static KafkaConsumer<String, String> kc;
    public static MongoClient mClient;
    public static MongoDatabase recordMdb;
    public static MongoDatabase configMdb;
    public static BlockingQueue<String> queue;
    public static Set<ProducerThread> pThreads;
    public static Set<ConsumerThread> cThreads;
    public static Timer timer;
    public static Map<String, String> collectionMap = new HashMap<String, String>();

    public static int consumerNameCount;
    public static int producerNameCount;
    private static int kafkaTopicPatitions;

    static {
        getKafkaTopicPatitions();
    }
    /**
     * 获取kafka的指定topic的分区数
     */
    private static void getKafkaTopicPatitions() {
        try {
            log.info("loading kafka.properties...");
            KafkaConsumer<String, String> kc = new KafkaConsumer<>(Configration.KAFKA_PROPS);

            String topic = Configration.CONSUMER_PROPS.getProperty("consumer.kafkaTopic");
            if ("".equals(topic) || topic == null) {
                log.error("consumer.kafkaTopic must not be null or empty!");
            }
            //连接topic
            kc.subscribe(Collections.singletonList(topic));
            kafkaTopicPatitions = kc.partitionsFor(topic).size();
            log.info("partitions = " + kafkaTopicPatitions);
        } catch (Throwable t) {
            log.error("initializing kafka client failed", t);
            throw new RuntimeException(t);
        }

    }
    public static int getConsumerThreadCount(){
        String consumerThreadCount = Configration.CONSUMER_PROPS.getProperty("consumer.threadCount");
        int cCount;
        if ("".equals(consumerThreadCount) || consumerThreadCount == null) {
            log.info("consumer.threadCount is null, it will use the topic partitions of kafka");
            cCount = kafkaTopicPatitions;
        }else {
            cCount = Integer.parseInt(consumerThreadCount);
        }
        log.info("the number of consumer threads is " + cCount);
        return cCount;
    }
    public static int getProducerThreadCount(){
        String producerThreadCount = Configration.CONSUMER_PROPS.getProperty("producer.threadCount");
        int pCount;
        if ("".equals(producerThreadCount) || producerThreadCount == null) {
            log.info("consumer.threadCount is null, it will use the topic partitions of kafka");
            pCount = kafkaTopicPatitions;
        }else {
            pCount = Integer.parseInt(producerThreadCount);
        }
        log.info("the number of producer threads is " + pCount);
        return pCount;
    }
}
