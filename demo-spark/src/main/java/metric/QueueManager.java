package metric;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/7/26 15:09
 */
public class QueueManager {
    private final Queue queue;

    public QueueManager(MetricRegistry metrics, String name) {
        this.queue = new ConcurrentLinkedQueue();
        metrics.register(MetricRegistry.name(QueueManager.class, name, "size"),
                (Gauge<Integer>) queue::size);
    }
}