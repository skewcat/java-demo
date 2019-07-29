package metric;

import com.codahale.metrics.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/7/26 14:42
 */
public class GetStarted {


    static final MetricRegistry metrics = new MetricRegistry();
    public static void main(String args[]) {
        startReport();
        Meter requests = metrics.meter("requests");

        String name = MetricRegistry.name(QueueManager.class, "count");
        Counter counter = metrics.counter(name);
//        requests.mark();
        counter.getCount();
        wait5Seconds();
    }

    private static void startReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    private static void wait5Seconds() {
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
        }
    }
}
