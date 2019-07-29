package com.asiainfo.breeze.consumer;

import java.io.InputStream;
import java.lang.Thread.State;
import java.util.Properties;
import java.util.TimerTask;

import com.asiainfo.breeze.conf.Configration;
import org.apache.log4j.Logger;

import com.asiainfo.breeze.util.AlarmSender;
import com.asiainfo.breeze.util.InstanceHolder;

/**
 * 定时检查producer&consumer线程状态，检查阻塞队列长度并告警，更新consumer线程数量，
 * 更新collection配置
 *
 * @author kelgon
 */
public class DaemonTask extends TimerTask {
    private final static Logger log = Logger.getLogger(DaemonTask.class);

    DaemonTask() {
        super();
    }

    public void run() {
        try {
            log.info("DaemonTask started");
            //检查ProducerThread状态
            updateProducerThread();
            //检查ConsumerThread状态，并更新ConsumerThread线程数配置
            updateConsumerThread();
            //检查队列积压情况
//            log.info("checking untended tasks in blockingQueue...");
//            try {
//                int threshold = Integer.parseInt(Configration.CONSUMER_PROPS.getProperty("record.queueSizeAlarmThreshold"));
//                int queueSize = InstanceHolder.queue.size();
//                if (queueSize >= threshold) {
//                    AlarmSender.sendAlarm("breeze-consumer队列积压任务数超阈值，当前队列长度：" + queueSize + "，阈值：" + threshold);
//                }
//            } catch (Throwable t) {
//                log.error("ConsumerThread queueSize-check failed", t);
//                AlarmSender.sendAlarm("breeze-consumer阻塞队列检查出错：" + t.toString());
//            }
            backPressure();
            //刷新配置
            log.info("reload and renew configurations...");
            Configration.initKafkaProps();
            Configration.initMongodbProps();
            Configration.initConsumerProps();
            log.info("DaemonTask done.");
        } catch (Throwable t) {
            log.error("error occured in DaemonTask", t);
            AlarmSender.sendAlarm("[breeze-consumer] DaemonTask执行出错" + t.toString());
        }
    }

    /**
     * 检查ProducerThread状态，并更新ProducerThread线程数配置
     */
    private void updateProducerThread() {
        try {
            int newCount = InstanceHolder.getProducerThreadCount();
            int deadCount = 0;
            int liveCount = 0;
            for (ProducerThread pt : InstanceHolder.pThreads) {//获取存活和死亡的线程的数量
                if (State.TERMINATED.equals(pt.getState())) {
                    deadCount++;
                } else {
                    liveCount++;
                }
            }
            log.info("living ProducerThreads: " + liveCount);
            log.info("dead ProducerThreads: " + deadCount);
            //补充启动线程
            if (newCount > liveCount) {
                log.info("Starting new ProducerThread to match producer.threadCount [" + newCount + "] ...");
                for (int i = 0; i < newCount - liveCount; i++) {
                    InstanceHolder.producerNameCount++;
                    ProducerThread producerT = new ProducerThread();
                    InstanceHolder.pThreads.add(producerT);
                    producerT.setName("Producer-" + InstanceHolder.consumerNameCount);
                    producerT.start();
                }
            }
            //停止多余线程
            if (newCount < liveCount) {
                log.info("Stoping ProducerThread to match producer.threadCount [" + newCount + "] ...");
                int i = 0;
                for (ProducerThread pt : InstanceHolder.pThreads) {
                    pt.sigStop();
                    i++;
                    if (i == liveCount - newCount)
                        break;
                }
            }
        } catch (Throwable t) {
            log.error("ProducerThread state-check failed", t);
            AlarmSender.sendAlarm("breeze-producer线程状态检查出错：" + t.toString());
        }
    }

    /**
     * 检查ConsumerThread状态，并更新ConsumerThread线程数配置
     */
    private void updateConsumerThread() {
        try {
            int newCount = InstanceHolder.getConsumerThreadCount();
            int deadCount = 0;
            int liveCount = 0;
            for (ConsumerThread ct : InstanceHolder.cThreads) {//获取存活和死亡的线程的数量
                if (State.TERMINATED.equals(ct.getState())) {
                    deadCount++;
                } else {
                    liveCount++;
                }
            }
            log.info("living ConsumerThreads: " + liveCount);
            log.info("dead ConsumerThreads: " + deadCount);
            //补充启动线程
            if (newCount > liveCount) {
                log.info("Starting new ConsumerThread to match consumer.threadCount [" + newCount + "] ...");
                for (int i = 0; i < newCount - liveCount; i++) {
                    InstanceHolder.consumerNameCount++;
                    ConsumerThread consumerT = new ConsumerThread();
                    InstanceHolder.cThreads.add(consumerT);
                    consumerT.setName("Consumer-" + InstanceHolder.consumerNameCount);
                    consumerT.start();
                }
            }
            //停止多余线程
            if (newCount < liveCount) {
                log.info("Stoping ConsumerThread to match consumer.threadCount [" + newCount + "] ...");
                int i = 0;
                for (ConsumerThread ct : InstanceHolder.cThreads) {
                    ct.sigStop();
                    i++;
                    if (i == liveCount - newCount)
                        break;
                }
            }
        } catch (Throwable t) {
            log.error("ConsumerThread state-check failed", t);
            AlarmSender.sendAlarm("breeze-consumer线程状态检查出错：" + t.toString());
        }
    }
    private void backPressure(){
        log.info("checking untended tasks in blockingQueue...");
        try {
            int threshold = Integer.parseInt(Configration.CONSUMER_PROPS.getProperty("record.queueSizeAlarmThreshold"));
            int queueSize = InstanceHolder.queue.size();
            if (queueSize >= threshold) {
                AlarmSender.sendAlarm("breeze-consumer队列积压任务数超阈值，当前队列长度：" + queueSize + "，阈值：" + threshold);
            }
        } catch (Throwable t) {
            log.error("ConsumerThread queueSize-check failed", t);
            AlarmSender.sendAlarm("breeze-consumer阻塞队列检查出错：" + t.toString());
        }
    }

}
