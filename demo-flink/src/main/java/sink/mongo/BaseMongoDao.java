package sink.mongo;
import org.springframework.data.mongodb.core.BulkOperations.BulkMode;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/17 17:06
 */
public class BaseMongoDao implements Serializable {
    public static MongoTemplate mongoTemplate = new MongodbConfig().mongoTemplate;
    public static Class entityClass;
    public static String collectionName;
    public static ThreadPoolExecutor threadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();

//    public BaseMongoDao(String colName,Object clazz) {
//        mongoTemplate = new MongodbConfig().mongoTemplate;
//        collectionName = colName;
//        threadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
//        entityClass = clazz.getClass();
//    }

    public static int insert(Object object, String collectionName) {
        mongoTemplate.insert(object, collectionName);
        return 0;
    }

    public static int bulkInsert(Iterable documents, BulkMode mode, Boolean isSync) {
        if (documents != null) {
            BulkWrapper bulkOperation = createBulkOps(mode, isSync);
            documents.forEach(bulkOperation::insert);
            bulkOperation.execute();
        }
        return 0;
    }

    private static BulkWrapper createBulkOps(BulkMode mode, boolean isSync) {
        return new BulkWrapper(mongoTemplate.bulkOps(mode, entityClass, collectionName), isSync, threadPool);
    }
}
