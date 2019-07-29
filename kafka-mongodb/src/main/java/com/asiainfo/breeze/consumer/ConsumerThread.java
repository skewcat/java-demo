package com.asiainfo.breeze.consumer;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import com.alibaba.fastjson.JSONObject;
import com.asiainfo.breeze.conf.Configration;
import com.asiainfo.breeze.util.TimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import com.asiainfo.breeze.util.InstanceHolder;

/**
 * 此线程会持续从阻塞队列中读取记录，并将其持久化至MongoDB中
 *
 * @author kelgon
 */
public class ConsumerThread extends Thread {
    private static final Logger log = Logger.getLogger(ConsumerThread.class);
    private boolean run = true;
    public List<Document> documents;

    public void run() {
        log.info(this.getName() + " started");
//        Map<String, List<Document>> quintupleDocumentsMap = new HashMap<>();
        documents = new ArrayList<>();
        long tempTimestamp = 0;
        while (run) {
            try {
                log.debug("Taking record from queue...");
                //从阻塞队列中读取记录，如果5秒内未取到记录，则继续下一循环
                String record = InstanceHolder.queue.poll(5000, TimeUnit.MILLISECONDS);
                if (record == null) {//没有新数据进入，清空缓存的队列
                    if (this.documents.size() != 0) {
                        log.info("the remain number of documents in thread [" + this.getName() + "] is " + this.documents.size());
                        String timestampKey = StringUtils.defaultString(Configration.CONSUMER_PROPS.getProperty("consumer.timestampKey"), "timestamp");
                        int collectionDocumentLength = this.documents.size();
                        long documentsLastTime = this.documents.get(collectionDocumentLength - 1).getLong(timestampKey);
                        String collectionName = this.getCollectionName(documentsLastTime);
                        InstanceHolder.recordMdb.getCollection(collectionName).insertMany(this.documents);
                        this.documents.clear();
                    }
                    continue;
                }
                log.debug("Record fetched");
                log.debug("Record fetched: " + record);
                log.debug("Record all number = " + InstanceHolder.queue.size());
                //解析kafka的数据为Document对象
                Document doc = parseDocument(record);

                documents.add(doc);
//                log.info("size=" + documents.size());
                //写入逻辑主要是批量写入，当队列满或者长时间无数据进入时，则将队列的所有数据写入到mongodb
                //获取时间戳的key
                String timestampKey = StringUtils.defaultString(Configration.CONSUMER_PROPS.getProperty("consumer.timestampKey"), "timestamp");
                int collectionDocumentLength = documents.size();
                long documentsLastTime = documents.get(collectionDocumentLength - 1).getLong(timestampKey);
                long currentTime = System.currentTimeMillis();
                long collectionDocumentTimeInterval = currentTime - documentsLastTime;
                boolean lengthOverRange = collectionDocumentLength >= 3000;//队列长度越界
//                log.info(collectionDocumentLength);
//                log.info(collectionDocumentTimeInterval);
                boolean TimeIntervalOverRange = tempTimestamp == documentsLastTime && (collectionDocumentTimeInterval >= 10000 && collectionDocumentTimeInterval <= 300000);//等待时间越界
                if (lengthOverRange | TimeIntervalOverRange) {
                    //获取目标的mongodb集合名词
                    String collectionName = getCollectionName(documentsLastTime);
                    //批量写入数据到mongodb
                    log.info("Record inserted to MongoDB collection \"" + collectionName + "\"" + "and record number = " + collectionDocumentLength);
                    log.debug(Thread.currentThread().getName() + " collectionDocumentTimeInterval = " + collectionDocumentTimeInterval);
                    InstanceHolder.recordMdb.getCollection(collectionName).insertMany(documents);
                    documents.clear();
                }
                tempTimestamp = documentsLastTime;//记录当前队列长度的时间戳，用于下次循环时对比，发现队列是否有新数据进入
            } catch (Throwable t) {
                log.error("Error occured in consumer thead", t);
            }
        }
    }

    public Document parseDocument(String record) {
        JSONObject recordJson = JSONObject.parseObject(record);

        //从记录对象中获取目标collection名与记录生成时间
        String dataKey = Configration.CONSUMER_PROPS.getProperty("consumer.DataKey");
        String timestampKey = StringUtils.defaultString(Configration.CONSUMER_PROPS.getProperty("consumer.timestampKey"), "timestamp");
        Document doc = Document.parse(recordJson.getString(dataKey));
        doc.put(timestampKey,doc.getLong(timestampKey) / 1000);
        return doc;
    }

    public String getCollectionName(long timestamp) {
        String targetCollection = Configration.CONSUMER_PROPS.getProperty("target.collections");
        if (targetCollection == null) {
            log.error("target collection is null");
            throw new RuntimeException("target collection is null");
        }

        //TODO 在collection名后追加分片时间戳
//        targetCollection = targetCollection + "_" + TimeUtils.getMonday(timestamp);
        return targetCollection;
    }

    /**
     * 向此线程发送中止信号，线程会在完成当前循环后退出
     */
    public void sigStop() {
        this.run = false;
    }
}
