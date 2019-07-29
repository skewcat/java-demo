package sink;

import com.mongodb.*;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/17 15:13
 */
public class MongoManager {
    private static MongoTemplate mongoTemplate= null;
//    public static String MONGODB_URL = "mongodb://172.18.135.22:26000/ion.appUserInfo";
    static {
        initDBPrompties();
    }
    static MongoTemplate getMongo(){return mongoTemplate;}
    /**
     * 初始化连接池
     */
    private static void initDBPrompties() {
        try {
            mongoTemplate = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient("172.18.130.22,172.18.130.24,172.18.130.26", 26000), "ion"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
