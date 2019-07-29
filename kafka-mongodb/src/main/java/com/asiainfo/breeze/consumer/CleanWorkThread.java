package com.asiainfo.breeze.consumer;

import com.asiainfo.breeze.conf.Configration;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.asiainfo.breeze.util.InstanceHolder;

/**
 * 当breeze-consumer进程退出时执行的清理逻辑，避免出现已读入队列但未完成消费的任务
 *
 * @author kelgon
 */
public class CleanWorkThread extends Thread {
    private static final Logger log = Logger.getLogger(CleanWorkThread.class);

    public void run() {
        try {
            //终止daemon定时任务
            log.info("stopping DaemonTask");
            InstanceHolder.timer.cancel();
            //终止producer线程
            log.info("stopping producer thread...");
            for (ProducerThread pt : InstanceHolder.pThreads) {
                pt.sigStop();
            }
            //检查producer线程状态，确认线程已进入TERMINATED状态后再继续
            int stopedProducerThreads = 0;
            while (stopedProducerThreads == InstanceHolder.producerNameCount) {
                Thread.sleep(100);
                for (ProducerThread pt : InstanceHolder.pThreads) {
                    if (State.TERMINATED.equals(pt.getState())){
                        stopedProducerThreads ++;
                    }
                }
            }
            //检查阻塞队列中的任务，直到连续4次检查阻塞队列均为空后再继续
            log.info("checking remaining tasks...");
            int count = 0;
            while (true) {
                Thread.sleep(500);
                if (InstanceHolder.queue.size() == 0)
                    count++;
                if (count > 4)
                    break;
            }
            int cCount = 0;
            do {
                Thread.sleep(500);
                for (ConsumerThread ct : InstanceHolder.cThreads) {
                    if (ct.documents.size() != 0) {
                        log.info("the remain number of documents in thread [" + ct.getName() + "] is " + ct.documents.size());
                        String timestampKey = StringUtils.defaultString(Configration.CONSUMER_PROPS.getProperty("consumer.timestampKey"), "timestamp");
                        int collectionDocumentLength = ct.documents.size();
                        long documentsLastTime = ct.documents.get(collectionDocumentLength - 1).getLong(timestampKey) / 1000;
                        String collectionName = ct.getCollectionName(documentsLastTime);
                        InstanceHolder.recordMdb.getCollection(collectionName).insertMany(ct.documents);
                        ct.documents.clear();
                    } else {
                        cCount++;
                    }
                }
            } while (cCount <= InstanceHolder.consumerNameCount);
            //终止consumer线程
            log.info("stopping consumer threads...");
            for (ConsumerThread ct : InstanceHolder.cThreads) {
                ct.sigStop();
            }
            //检查consumer线程状态，确认所有consumer线程均已进入TERMINATED状态后再继续
            while (true) {
                Thread.sleep(500);
                boolean all = true;
                for (ConsumerThread ct : InstanceHolder.cThreads) {
                    if (!State.TERMINATED.equals(ct.getState()))
                        all = false;
                }
                if (all)
                    break;
            }
            log.info("shutting down complete");
        } catch (Throwable t) {
            log.error("error occured while shutting down breeze-consumer!", t);
        }
    }
}
