package com.asiainfo.breeze.consumer;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import com.asiainfo.breeze.conf.Configration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.asiainfo.breeze.util.InstanceHolder;
import com.mongodb.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

/**
 * breeze-consumer启动线程
 *
 * @author kelgon
 */
public class ConsumerRunner {
    private static final Logger log = Logger.getLogger(ConsumerRunner.class);


    /**
     * 初始化MongoDB client
     */
    private static boolean initMongo() {
        try {
            log.info("loading mongo.properties...");

            //构造MongoDB集群serverList
            String servers = Configration.MONGODB_PROPS.getProperty("mongo.servers");
            if ("".equals(servers) || servers == null) {
                log.error("mongo.servers must not be null or empty!");
                return false;
            }
            List<ServerAddress> serverList = new ArrayList<>();
            for (String s : servers.split(",")) {
                String[] addr = s.split(":");
                ServerAddress sa = new ServerAddress(addr[0], Integer.parseInt(addr[1]));
                serverList.add(sa);
            }

            //构造MongoDB身份认证对象
            String recordDbName = Configration.MONGODB_PROPS.getProperty("breeze.recordDbname");
            if ("".equals(recordDbName) || recordDbName == null) {
                log.error("record.dbname must not be null or empty!");
                return false;
            }
            String recordCredentials = Configration.MONGODB_PROPS.getProperty("breeze.recordCredentials");
            List<MongoCredential> mCreList = new ArrayList<>();
            if (!"".equals(recordCredentials) && recordCredentials != null) {
                String[] cre = recordCredentials.split(":");
                MongoCredential credential = MongoCredential.createScramSha1Credential(cre[0], recordDbName, cre[1].toCharArray());
                mCreList.add(credential);
            }

            //从配置文件加载MongoDB客户端参数
            Builder options = new Builder();
            if (Configration.MONGODB_PROPS.containsKey("mongo.connectionsPerHost"))
                options.connectionsPerHost(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.connectionsPerHost")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.connectTimeout"))
                options.connectTimeout(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.connectTimeout")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.heartbeatConnectTimeout"))
                options.heartbeatConnectTimeout(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.heartbeatConnectTimeout")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.heartbeatFrequency"))
                options.heartbeatFrequency(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.heartbeatFrequency")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.heartbeatSocketTimeout"))
                options.heartbeatSocketTimeout(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.heartbeatSocketTimeout")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.maxConnectionIdleTime"))
                options.connectTimeout(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.maxConnectionIdleTime")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.maxConnectionLifeTime"))
                options.maxConnectionLifeTime(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.maxConnectionLifeTime")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.maxWaitTime"))
                options.maxWaitTime(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.maxWaitTime")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.minConnectionsPerHost"))
                options.minConnectionsPerHost(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.minConnectionsPerHost")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.minHeartbeatFrequency"))
                options.minHeartbeatFrequency(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.minHeartbeatFrequency")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.readConcern")) {
                String readConcern = Configration.MONGODB_PROPS.getProperty("mongo.readConcern");
                if ("default".equalsIgnoreCase(readConcern))
                    options.readConcern(ReadConcern.DEFAULT);
                if ("local".equalsIgnoreCase(readConcern))
                    options.readConcern(ReadConcern.LOCAL);
                if ("majority".equalsIgnoreCase(readConcern))
                    options.readConcern(ReadConcern.MAJORITY);
            }
            if (Configration.MONGODB_PROPS.containsKey("mongo.readPreference")) {
                String readPreference = Configration.MONGODB_PROPS.getProperty("mongo.readPreference");
                if ("primary".equalsIgnoreCase(readPreference))
                    options.readPreference(ReadPreference.primary());
                if ("primaryPreferred".equalsIgnoreCase(readPreference))
                    options.readPreference(ReadPreference.primaryPreferred());
                if ("secondary".equalsIgnoreCase(readPreference))
                    options.readPreference(ReadPreference.secondary());
                if ("secondaryPreferred".equalsIgnoreCase(readPreference))
                    options.readPreference(ReadPreference.secondaryPreferred());
                if ("nearest".equalsIgnoreCase(readPreference))
                    options.readPreference(ReadPreference.nearest());
            }
            if (Configration.MONGODB_PROPS.containsKey("mongo.serverSelectionTimeout"))
                options.serverSelectionTimeout(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.serverSelectionTimeout")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.socketTimeout"))
                options.socketTimeout(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.socketTimeout")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.threadsAllowedToBlockForConnectionMultiplier"))
                options.threadsAllowedToBlockForConnectionMultiplier(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.threadsAllowedToBlockForConnectionMultiplier")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.writeConcern"))
                options.writeConcern(new WriteConcern(Integer.parseInt(Configration.MONGODB_PROPS.getProperty("mongo.writeConcern"))));
            if (Configration.MONGODB_PROPS.containsKey("mongo.socketKeepAlive"))
                options.socketKeepAlive(Boolean.parseBoolean(Configration.MONGODB_PROPS.getProperty("mongo.socketKeepAlive")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.sslEnabled"))
                options.sslEnabled(Boolean.parseBoolean(Configration.MONGODB_PROPS.getProperty("mongo.sslEnabled")));
            if (Configration.MONGODB_PROPS.containsKey("mongo.sslInvalidHostNameAllowed"))
                options.sslInvalidHostNameAllowed(Boolean.parseBoolean(Configration.MONGODB_PROPS.getProperty("mongo.sslInvalidHostNameAllowed")));

            //initialize mongodb client
            log.info("initializing mongodb client...");
            if (mCreList.size() > 0)
                InstanceHolder.mClient = new MongoClient(serverList, mCreList, options.build());
            else
                InstanceHolder.mClient = new MongoClient(serverList);
            InstanceHolder.recordMdb = InstanceHolder.mClient.getDatabase(recordDbName);
            return true;
        } catch (Throwable t) {
            log.error("initializing MongoClient failed", t);
            return false;
        }
    }

    /**
     * 初始化breeze-consumer
     */
    private static boolean initBreezeConsumer() {
        InstanceHolder.timer = new Timer();
        try {
            log.debug("loading consumer.properties...");
            //初始化阻塞队列
            log.info("initializing blocking queue...");
            String queueSize = Configration.CONSUMER_PROPS.getProperty("consumer.queueSize");
            if ("".equals(queueSize) || queueSize == null) {
                log.error("consumer.queueSize must not be null or empty!");
                return false;
            }
            if ("0".equals(queueSize))
                InstanceHolder.queue = new LinkedBlockingQueue<>();
            else
                InstanceHolder.queue = new LinkedBlockingQueue<>(Integer.parseInt(queueSize));
            //初始化producer&consumer线程
            log.info("initializing producer&consumer threads...");


            //启动consumer线程
            initConsumerThread();
            //启动producer线程
            initProducerThread();

            //注册dameon线程，每5分钟执行一次
            InstanceHolder.timer.schedule(new DaemonTask(), 10000, 5 * 60 * 1000);

            return true;
        } catch (Throwable t) {
            log.error("initializing breeze-consumer failed", t);
            //explicitly exit to trigger cleaning mechanism
            System.exit(0);
            return false;
        }
    }

    private static void initProducerThread() {
        //初始化producer线程（从kafka获取数据，作为生产者写入到消息队列）
        int pCount = InstanceHolder.getProducerThreadCount();
        log.info("the number of producer threads is " + pCount);
        InstanceHolder.pThreads = new HashSet<>();
        int i = 0;
        while (InstanceHolder.pThreads.size() < pCount) {
            ProducerThread pt = new ProducerThread();
            pt.setName("Producer-" + i);
            InstanceHolder.pThreads.add(pt);
            i++;
        }
        InstanceHolder.producerNameCount = i;
        //启动producer线程
        log.info("launching producer threads...");
        for (ProducerThread pt : InstanceHolder.pThreads) {
            pt.start();
        }
    }

    private static void initConsumerThread() {
        int cCount = InstanceHolder.getConsumerThreadCount();
        log.info("the number of consumer threads is " + cCount);
        //初始化consumer线程（作为消费者来消费消息队列的数据，写入到mongodb）
        InstanceHolder.cThreads = new HashSet<>();
        int j = 0;
        while (InstanceHolder.cThreads.size() < cCount) {
            ConsumerThread ct = new ConsumerThread();
            ct.setName("Consumer-" + j);
            InstanceHolder.cThreads.add(ct);
            j++;
        }
        InstanceHolder.consumerNameCount = j;
        //启动consumer线程
        log.info("launching consumer threads...");
        for (ConsumerThread ct : InstanceHolder.cThreads) {
            ct.start();
        }
    }


    public static void main(String[] args) {
        PropertyConfigurator.configure(ConsumerRunner.class.getClassLoader().getResource("log4j.properties"));
        //注册进程退出hook，在进程退出时执行清理逻辑
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
//        log.info("initializing mongo client...");
//        if (ConsumerRunner.initMongo()) {
//            log.info("initializing kafka client...");
//            if (ConsumerRunner.initKafkaConsumer()) {
//                log.info("initializing breeze-consumer...");
//                if (ConsumerRunner.initBreezeConsumer()) {
//                    log.info("breeze-consumer started");
//                    return;
//                }
//            }
//        }
        log.info("initializing mongo client...");
        if (ConsumerRunner.initMongo()) {
            log.info("initializing breeze-consumer...");
            if (ConsumerRunner.initBreezeConsumer()) {
                log.info("breeze-consumer started");
                return;
            }

        }
        log.error("failed to start breeze-consumer");
        System.exit(0);
    }
}
