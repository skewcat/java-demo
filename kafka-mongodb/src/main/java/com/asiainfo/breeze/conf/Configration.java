package com.asiainfo.breeze.conf;

import com.asiainfo.breeze.consumer.ConsumerRunner;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/3/13 9:40
 */
public class Configration {
    public static Properties KAFKA_PROPS;
    public static Properties MONGODB_PROPS;
    public static Properties CONSUMER_PROPS;
    private static final Logger log = Logger.getLogger(Configration.class);

    static {
        //kafka配置
        initKafkaProps();
        //mongodb配置
        initMongodbProps();
        //消费者配置
        initConsumerProps();
    }

    public static void initKafkaProps() {
        try {
            InputStream kafkaPropertiesFile = ConsumerRunner.class.getClassLoader().getResourceAsStream("kafka.properties");
            KAFKA_PROPS = new Properties();
            KAFKA_PROPS.load(kafkaPropertiesFile);
            kafkaPropertiesFile.close();
        } catch (Throwable t) {
            log.error("load properties file failed", t);
            throw new RuntimeException(t);
        }
        String groupId = KAFKA_PROPS.getProperty("group.id") + "_" + String.valueOf(System.currentTimeMillis());

        KAFKA_PROPS.setProperty("group.id", groupId);
    }

    public static void initMongodbProps() {
        try {
            InputStream mongodbPropertiesFile = ConsumerRunner.class.getClassLoader().getResourceAsStream("mongo.properties");
            MONGODB_PROPS = new Properties();
            MONGODB_PROPS.load(mongodbPropertiesFile);
            mongodbPropertiesFile.close();
        } catch (Throwable t) {
            log.error("load properties file failed", t);
            throw new RuntimeException(t);
        }
    }

    public static void initConsumerProps() {
        try {
            InputStream consumerPropertiesFile = ConsumerRunner.class.getClassLoader().getResourceAsStream("consumer.properties");
            CONSUMER_PROPS = new Properties();
            CONSUMER_PROPS.load(consumerPropertiesFile);
            consumerPropertiesFile.close();
        } catch (Throwable t) {
            log.error("load properties file failed", t);
            throw new RuntimeException(t);
        }
    }

}
