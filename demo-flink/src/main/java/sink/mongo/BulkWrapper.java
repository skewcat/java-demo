package sink.mongo;

import com.mongodb.BulkWriteResult;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.util.Pair;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/17 17:23
 */
public class BulkWrapper implements BulkOperations {
    private BulkOperations bulkOpt;
    private boolean isSync;
    private ThreadPoolExecutor threadPool;

    public BulkWrapper(BulkOperations bulkOperations, boolean isSyncDefined, ThreadPoolExecutor threadPoolDefined) {
        bulkOpt = bulkOperations;
        isSync = isSyncDefined;
        threadPool = threadPoolDefined;
    }

    @Override
    public BulkOperations insert(Object documents) {
        return bulkOpt.insert(documents);
    }

    @Override
    public BulkOperations insert(List<?> documents) {
        return bulkOpt.insert(documents);
    }

    @Override
    public BulkOperations updateOne(Query query, Update update) {
        return bulkOpt.updateOne(query, update);
    }

    @Override
    public BulkOperations updateOne(List<Pair<Query, Update>> updates) {
        return bulkOpt.updateOne(updates);
    }

    @Override
    public BulkOperations updateMulti(Query query, Update update) {
        return bulkOpt.updateMulti(query, update);
    }

    @Override
    public BulkOperations updateMulti(List<Pair<Query, Update>> updates) {
        return bulkOpt.updateMulti(updates);
    }

    @Override
    public BulkOperations upsert(Query query, Update update) {
        return bulkOpt.upsert(query, update);
    }

    @Override
    public BulkOperations upsert(List<Pair<Query, Update>> updates) {
        return bulkOpt.upsert(updates);
    }

    @Override
    public BulkOperations remove(Query remove) {
        return bulkOpt.remove(remove);
    }

    @Override
    public BulkOperations remove(List<Query> removes) {
        return bulkOpt.remove(removes);
    }

    @Override
    public BulkWriteResult execute() {
        BulkWriteResult bulkWriteResult = null;
        if (isSync) {
            //如果是同步批量直接处理
            bulkWriteResult = bulkOpt.execute();
        } else {
            //如果是异步，使用线程池启动线程操作dao，并且设置线程池大小，线程池处理量超出，则使用原有的同步方式操作
            int threadCount = threadPool.getActiveCount();
            if (threadCount < MongodbConfig.asyncPoolSize) {
                Future<BulkWriteResult> messageFuture = threadPool.submit(new TestCallable(bulkOpt));
                try {
                    bulkWriteResult = messageFuture.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                threadPool.execute(() -> {
//                    try {
//                        bulkOpt.execute();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                });

            } else {
                System.out.println("mongodao thread Pool size is now:{" + threadCount + "}");
                bulkWriteResult = bulkOpt.execute();
            }
        }
        return bulkWriteResult;
    }
}

class TestCallable implements Callable<BulkWriteResult> {

    private BulkOperations bulkOperations;

    public TestCallable(BulkOperations bulkOpt) {
        this.bulkOperations = bulkOpt;
    }

    @Override
    public BulkWriteResult call() {
        BulkWriteResult bulkWriteResult = null;
        try {
            bulkWriteResult = bulkOperations.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bulkWriteResult;
    }
}
