package com.asiainfo.breeze.consumer;

import com.asiainfo.breeze.conf.Configration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import com.asiainfo.breeze.util.InstanceHolder;

import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

/**
 * 此线程会持续从Kafka集群拉取消息，置入阻塞队列中
 *
 * @author kelgon
 */
public class ProducerThread extends Thread {
    private static final Logger log = Logger.getLogger(ProducerThread.class);
    private boolean run = true;
    private KafkaConsumer<String, String> kc;
    public void run() {
        try {
            log.info("loading kafka.properties...");
            InputStream kafkaPropertiesFile = ConsumerRunner.class.getClassLoader().getResourceAsStream("kafka.properties");
            Properties kafkaProperties = new Properties();
            kafkaProperties.load(kafkaPropertiesFile);
            kc = new KafkaConsumer<>(kafkaProperties);
            InputStream consumerPropertiesFile = ConsumerRunner.class.getClassLoader().getResourceAsStream("consumer.properties");
            Properties consumerProperties = new Properties();
            consumerProperties.load(consumerPropertiesFile);

            String topic = consumerProperties.getProperty("consumer.kafkaTopic");
            if ("".equals(topic) || topic == null) {
                log.error("consumer.kafkaTopic must not be null or empty!");
            }

            kc.subscribe(Collections.singletonList(topic));
        } catch (Throwable t) {
            log.error("initializing kafka client failed", t);
        }

        log.info("Producer thread started");
        while (run) {
            try {
                //队列积压超过上限，则设置背压机制，减缓从kafka拉取数据的速度
                int threshold = Integer.parseInt(Configration.CONSUMER_PROPS.getProperty("record.queueSizeAlarmThreshold"));
                int queueSize = InstanceHolder.queue.size();
                if (queueSize >= threshold) {
                    Thread.sleep(1000);
                }
                log.debug("Fetching records from Kafka...");
                //从Kafka集群拉取消息，如果5秒内没有任何消息，则继续下一循环
                ConsumerRecords<String, String> records = kc.poll(5000);
                log.debug(records.count() + " new records fetched");

                //将拉取到的消息写入阻塞队列
                for (ConsumerRecord<String, String> record : records) {
                    InstanceHolder.queue.put(record.value());
                }
                log.info(Thread.currentThread().getName() + " added " + records.count() + " records to the queue, queue length: " + InstanceHolder.queue.size());

                //向Kafka集群提交offset
                if (records.count() > 0)
                    kc.commitSync();
                Thread.sleep(1000);
            } catch (Throwable t) {
                log.error("Error occured in producer thead", t);
            }
        }
        log.warn("Producer thread ended");
    }

    /**
     * 向此线程发送中止信号，线程会在完成当前循环后退出
     */
    void sigStop() {
        this.run = false;
    }
}
