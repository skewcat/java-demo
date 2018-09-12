package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2018/8/18 10:27
 */
public class SimpleKafkaConsumer {
    private static Logger log = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
    //    private static final String[] TOPIC = {"dev-sysinfo","dev-terminal","dev_usage","dev_syslog"};
    private static final String[] TOPIC = {"dev-terminal"};

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.159.11:9092,172.31.159.12:9092,172.31.159.13:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");//自动提交偏移量到ZK的间隔
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);//消费者连接器

        consumer.subscribe(Arrays.asList(TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records.count() != 0) {
                log.info("records.count = {}", records.count());
            }
            for (ConsumerRecord<String, String> record : records)
                log.info("topic = {}, offset = {}, key = {}, value = {}", record.topic(), record.offset(), record.key(), record.value());
        }

    }
}

