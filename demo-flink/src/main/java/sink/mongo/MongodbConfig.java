package sink.mongo;

import com.mongodb.*;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.util.StringUtils;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/17 16:19
 */
public class MongodbConfig implements Serializable {
    public static String username;
    public static String password;
    public static String hosts = "172.18.130.22,172.18.130.24,172.18.130.26";
    public static String db = "ion";
    public static int asyncPoolSize = 10240;
    //最大链接数
    public static int connectionsPerHost = 3;
    //最小连接数
    public static int minConnectionsPerHost = 3;
    //线程可用的最大阻塞数
    public static int threadsAllowedToBlockForConnectionMultiplier = 50;
    //链接超时的毫秒数,0表示不超时,此参数只用在新建一个新链接时，推荐配置10,000.
    public static int connectTimeout = 10000;
    //一个线程等待链接可用的最大等待毫秒数，0表示不等待，负数表示等待时间不确定，推荐配置120000
    public static int maxWaitTime = 120000;
    //该标志用于控制socket保持活动的功能，通过防火墙保持连接活着
    public static boolean socketKeepAlive = true;
    //此参数表示socket I/O读写超时时间,推荐为不超时，即 0    Socket.setSoTimeout(int)
    public static int socketTimeout = 0;
    //连接的最大闲置时间，时间为0，表示无限制。
    public static int maxConnectionIdleTime = 7000;
    //心跳频率
    public static int heartbeatFrequency = 300000;
    public static int minHeartbeatFrequency = 250000;
    //心跳链接超时时间
    public static int heartbeatConnectTimeout = 10000;
    //心跳读写超时时间
    public static int heartbeatSocketTimeout = 10000;
    public static String readPreference = "secondarypreferred";
    public static String writeConcern = "unacknowledged";
    public static int writeConcernW = 0;
    public static String partitioner = "MongoSamplePartitioner";
    //设置spark mongo分区数据量大小
    public static int partitionerOptionsPartitionSizeMB = 48;

    public MongoTemplate mongoTemplate;
    public SimpleMongoDbFactory mongoDbFactory;
    public MappingMongoConverter mongoConverter;
    public String mongoUri = "mongodb://172.18.130.22:26000,172.18.130.24:26000,172.18.130.26:26000";
    public MongodbConfig(){
        this.mongoDbFactory = getMongoDbFactory();
        this.mongoConverter = getMongoConverter();
        this.mongoTemplate = getMongoTemplate();
    }

    private static String getMongoUri() {
        StringBuilder uri = new StringBuilder("mongodb://");
        if (!StringUtils.isEmpty(username)) {
            uri.append(username);
            if (!StringUtils.isEmpty(password)) {
                uri.append(":").append(password);
            }
            uri.append("@");
        }
        uri.append(hosts).append("/").append(db);
        return uri.toString();
    }

    Map<Long, MongoTemplate> mongoTemplateMap = new HashMap<>();

    private SimpleMongoDbFactory getMongoDbFactory() {
        return new SimpleMongoDbFactory(getMongoClient(), db);
    }

    private MongoClient getMongoClient() {
        List<MongoCredential> credentialsList = new java.util.ArrayList<>();
        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            credentialsList.add(MongoCredential.createCredential(username, db, password.toCharArray()));
        }

        return new MongoClient(getServerAddresses(hosts), credentialsList, getMongoClientOption());
    }

    private MongoClientOptions getMongoClientOption() {
        return new MongoClientOptions.Builder()
                .connectionsPerHost(connectionsPerHost)
                .minConnectionsPerHost(minConnectionsPerHost)
                .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier)
                .connectTimeout(connectTimeout)
                .maxWaitTime(maxWaitTime)
                .socketKeepAlive(socketKeepAlive).socketTimeout(socketTimeout).maxConnectionIdleTime(maxConnectionIdleTime)
                .heartbeatFrequency(heartbeatFrequency).minHeartbeatFrequency(minHeartbeatFrequency).heartbeatConnectTimeout(heartbeatConnectTimeout).heartbeatSocketTimeout(heartbeatSocketTimeout)
                .writeConcern(parseWriteConcern(writeConcern))
                .readPreference(parseReadPreference(readPreference))//.codecRegistry(CustomCodecRegistries.codecRegistry)
                .build();

    }

    private ReadPreference parseReadPreference(String readPreference) {
        switch (readPreference.toLowerCase()) {
            case "primary":
                return com.mongodb.ReadPreference.primary();
            case "secondary":
                return com.mongodb.ReadPreference.secondary();
            case "nearest":
                return com.mongodb.ReadPreference.nearest();
            case "primarypreferred":
                return com.mongodb.ReadPreference.primaryPreferred();
            case "secondarypreferred":
                return com.mongodb.ReadPreference.secondaryPreferred();
            default:
                return com.mongodb.ReadPreference.primaryPreferred();
        }
    }

    /**
     * @return Write concern for database writes
     */
    private WriteConcern parseWriteConcern(String writeConcern) {
        switch (writeConcern.toLowerCase()) {
            case "primary":
                return WriteConcern.ACKNOWLEDGED;
            case "secondary":
                return WriteConcern.UNACKNOWLEDGED;
            case "nearest":
                return WriteConcern.REPLICA_ACKNOWLEDGED;
            case "primarypreferred":
                return WriteConcern.MAJORITY;
            case "secondarypreferred":
                return WriteConcern.JOURNALED;
            default:
                return WriteConcern.ACKNOWLEDGED;
        }
    }

    private List<ServerAddress> getServerAddresses(String addressString) {
        List<ServerAddress> addresses = new java.util.ArrayList<>();
        try {
            String[] connections = addressString.split(",");
            for (int i = 0; i < connections.length; i++) {
                String connection = connections[i];
                int port = 26000;
                String[] hostPort = connection.split(":");
                if (hostPort.length > 1 && hostPort[i] != null) {
                    port = Integer.parseInt(hostPort[1].trim());
                } else {
                    addresses.add(new ServerAddress(hostPort[0], port));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return addresses;
    }

    private MappingMongoConverter getMongoConverter() {
        MappingMongoConverter mappingMongoConverter = new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory), new MongoMappingContext());
        mappingMongoConverter.setTypeMapper(new DefaultMongoTypeMapper(null));
        return mappingMongoConverter;
    }

    private MongoTemplate getMongoTemplate() {
        return new MongoTemplate(mongoDbFactory, mongoConverter);
    }


    private SimpleMongoDbFactory getMongoDbFactory(String dbName) {
        return new SimpleMongoDbFactory(getMongoClient(), dbName);
    }
}
