package consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2018/8/18 10:27
 */
public class SimpleKafkaConsumer {
    private static Logger log = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
    //        private static final String[] TOPIC = {"dev-sysinfo", "dev-terminal", "dev-usage", "dev-syslog"};
    private static final String[] TOPIC = {"pengxxTest14"};
    //    private static final String[] TOPIC = {"sam-user", "smp-info", "dev-sysinfo", "dev-terminal", "dev-notify", "device-identify"};
    //    private static final String BOOTSTRAP_SERVERS = "172.31.159.11:9092,172.31.159.12:9092,172.31.159.13:9092";
//    private static final String BOOTSTRAP_SERVERS = "192.168.31.90:9092";
//    private static final String BOOTSTRAP_SERVERS = "10.10.10.1:9092,10.10.10.2:9092,10.10.10.3:9092";
    private static final String BOOTSTRAP_SERVERS = "172.18.135.131:9092";

    public static void main(String[] args) {
        String groupId = "test1" + String.format("%5d", System.currentTimeMillis());
//        String groupId = "test1";
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");//自动提交偏移量到ZK的间隔
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);//消费者连接器

        consumer.subscribe(Arrays.asList(TOPIC));
        Set<String> ipSet = new HashSet<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                log.info("topic = {}, offset = {}, key = {}, value = {}", record.topic(), record.offset(), record.key(), record.value());
//                ipSet.add(record.key().split(",")[0]);
            }
//            System.out.println(ipSet.size());
        }

    }
}

