package producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/3/11 16:16
 */
public class SimpleKafkaProducer {
    private static final String TOPIC = "pengxxTest216";
    private static final String BOOTSTRAP_SERVERS = "172.18.135.2:9092";
    public static void main(String[] args) {
        List<JSONObject> result = getFileTopic();
        producerBacth(result);
    }

    private static List<JSONObject> getFileTopic() {
        List<JSONObject> result = new ArrayList<>();
        File file = new File("e:/data/test/ion.packetFeatureA_2019-02-25[192.168.203.95,9100,192.168.23.153,4952].json");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            // 一次读入一行，直到读入null为文件结束
            int i = 1;
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                // 显示行号
                if (!StringUtils.isEmpty(line)) {
                    JSONObject jsonObject = JSONObject.parseObject(line);
                    JSONObject newData = new JSONObject();
                    newData.put("num_id",++i);
                    newData.put("pktSize", jsonObject.getIntValue("pktSize"));
                    newData.put("pktInterval", jsonObject.getIntValue("pktInterval"));
                    newData.put("srcIP", jsonObject.getString("srcIP"));
                    newData.put("srcPort", jsonObject.getIntValue("srcPort"));
                    newData.put("rxPktSize", jsonObject.getIntValue("rxPktSize"));
                    newData.put("timeStamp", Long.parseLong(jsonObject.getString("timeStamp").substring(16, jsonObject.getString("timeStamp").length() - 2)));
                    newData.put("txPktInterval", jsonObject.getIntValue("txPktInterval"));
                    newData.put("protocol", jsonObject.getIntValue("protocol"));
                    newData.put("dstPort", jsonObject.getIntValue("dstPort"));
                    newData.put("txPktSize", jsonObject.getIntValue("txPktSize"));
                    newData.put("dstIP", jsonObject.getString("dstIP"));
                    newData.put("flowStatus", jsonObject.getString("flowStatus"));
                    newData.put("rxPktInterval", jsonObject.getIntValue("rxPktInterval"));
                    newData.put("key", jsonObject.getString("key"));
                    newData.put("direction", jsonObject.getIntValue("direction"));
                    result.add(newData);
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return result;
    }

    private static void producerBacth(List<JSONObject> result) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", 1);
        kafkaProps.put("linger.ms", 100);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
        for (JSONObject ele : result) {
            JSONObject sendJson = new JSONObject();
            sendJson.put("flowFeature", ele);
            String key = "192.168.203.95,9100,192.168.23.153,4952";
            // kafka的ProducerRecord()构造函数有多个方法，如果只发送value，那么是随机到任意分区的；如果带上key，那么key相同的是只会到一个分区
            // 分区数的影响是，当使用多线程的消费者来消费时，单分区只能使用一个线程。
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, sendJson.toJSONString());//Topic Key Value
            try {
                Future future = producer.send(record);
                System.out.println(future.get().toString());
                producer.flush();
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();//连接错误、No Leader错误都可以通过重试解决；消息太大这类错误kafkaProducer不会进行任何重试，直接抛出异常
            }
        }
    }
    private static void produceOneMessage(){
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaProps.put("acks", "all");
        kafkaProps.put("retries", 1);
        kafkaProps.put("linger.ms", 100);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
        JSONObject sendJson = new JSONObject();
        sendJson.put("flowFeature", "test");
        String key = "pengxxTest";
        // kafka的ProducerRecord()构造函数有多个方法，如果只发送value，那么是随机到任意分区的；如果带上key，那么key相同的是只会到一个分区
        // 分区数的影响是，当使用多线程的消费者来消费时，单分区只能使用一个线程。
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, sendJson.toJSONString());//Topic Key Value
        try {
            Future future = producer.send(record);
            System.out.println(future.get().toString());
            producer.flush();
            Thread.sleep(10);
        } catch (Exception e) {
            e.printStackTrace();//连接错误、No Leader错误都可以通过重试解决；消息太大这类错误kafkaProducer不会进行任何重试，直接抛出异常
        }
    }
}
