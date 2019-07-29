package kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import entry.AppConvInfo;
import entry.AppUserInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.springframework.data.mongodb.core.BulkOperations;
import sink.mongo.BaseMongoDao;

import java.util.*;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/15 11:10
 */
public class KafkaMessageStreaming {
    //    public static String MONGODB_URL = "mongodb://172.18.135.22:26000/ion.appUserInfo";
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 非常关键，一定要设置启动检查点！！
        env.enableCheckpointing(5000);
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.18.135.2:9092,172.18.135.3:9092,172.18.135.4:9092");
        props.setProperty("group.id", "flink-group");
        BaseMongoDao.entityClass = AppUserInfo.class;
        BaseMongoDao.collectionName = "appUserInfo";
        //创建流
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>("app-netflow", new SimpleStringSchema(), props));
        stream
                .filter(x -> !StringUtils.isEmpty(x) && x.contains("appuserinfo"))
//                .flatMap(KafkaMessageStreaming::getAppUserInfos)
//                .rebalance()
//        .map(x -> BaseMongoDao.bulkInsert(x, BulkOperations.BulkMode.UNORDERED,false));
                .map(KafkaMessageStreaming::getAppUserInfosForList)
                .map(x -> BaseMongoDao.bulkInsert(x, BulkOperations.BulkMode.UNORDERED, false));
        env.execute();
    }

    private static void getAppConvInfo(AppUserInfo appUser) {
        String prot = ("6".equals(appUser.getProt()) || "6.0".equals(appUser.getProt())) ? "TCP" : "UDP";
        String appKey = appUser.getName() + "andand" + appUser.getSip() + "andand" + appUser.getDip() + "andand" + appUser.getSpt() + "andand" + appUser.getDpt() + "andand" + prot + "andand" + appUser.getSrcMac() + "andand" + appUser.getDstMac();
        Tuple10<Integer, Integer, String, Integer, Integer, Long, Date, Integer, Integer, AppUserInfo> tuple10 = new Tuple10<>(appUser.getDgmLen(), appUser.getPackets(), String.valueOf(appUser.getDelay()), appUser.getPktLoss(), 1, appUser.getFlowDuration(), appUser.getTs(), appUser.getResponderBytes(), appUser.getInitiatorBytes(), appUser);
        Tuple2<String, Tuple10> tuple2 = new Tuple2<>(appKey, tuple10);
        AppConvInfo appConInfo = new AppConvInfo();
        //修订时间为会话最新时间
        //添加shardKey字段

    }

    private static void getAppUserInfos(String appUserInfoStream, Collector<AppUserInfo> objectCollector) {
        JSONObject jsonObject = JSON.parseObject(appUserInfoStream);
        Date ts = new Date();

        if (jsonObject.containsKey("@timestamp")) {
            Long tsLong = jsonObject.getLong("@timestamp");
            ts = new Date(tsLong / 1000);
        }
        if (jsonObject.containsKey("appuserinfo")) {
            JSONArray jsonArray = JSONArray.parseArray(jsonObject.getString("appuserinfo"));
            if (jsonArray == null) {
                objectCollector.collect(new AppUserInfo());
            } else {
                for (Object json : jsonArray) {
                    AppUserInfo ele = new AppUserInfo();
                    JSONObject jsonEle = (JSONObject) json;
                    ele.setTs(ts);
                    ele.setSip(jsonEle.getString("src_ip_address"));
                    ele.setSpt(jsonEle.getIntValue("l4_src_port"));
                    ele.setDip(jsonEle.getString("dst_ip_address"));
                    ele.setDpt(jsonEle.getIntValue("l4_dst_port"));

                    ele.setSrcMac(jsonEle.getString("in_src_mac"));
                    ele.setDstMac(jsonEle.getString("out_dst_mac"));
                    ele.setResponderBytes(jsonEle.getIntValue("responderBytes"));
                    ele.setRxMaxPktLength(jsonEle.getIntValue("rx_max_pkt_length"));
                    ele.setPktLoss(jsonEle.getIntValue("droppedOctetDeltaCount"));

                    ele.setInitiatorPackets(jsonEle.getIntValue("initiatorPackets"));
                    ele.setPackets(jsonEle.getIntValue("in_pkts"));

                    ele.setTxMinPktLength(jsonEle.getIntValue("tx_min_pkt_length"));
                    ele.setProt(jsonEle.getString("protocol"));

                    ele.setResponderPackets(jsonEle.getIntValue("responderPackets"));
                    ele.setMinPktInterval(jsonEle.getIntValue("min_pkt_interval"));

                    ele.setTxMaxPktLength(jsonEle.getIntValue("tx_max_pkt_length"));
                    ele.setMaxPktInterval(jsonEle.getIntValue("max_pkt_interval"));
                    ele.setDelay(jsonEle.getInteger("delay"));
                    ele.setMaxDelay(jsonEle.getIntValue("max_delay"));
                    ele.setMinDelay(jsonEle.getIntValue("min_delay"));

                    ele.setInitiatorBytes(jsonEle.getIntValue("initiatorBytes"));
                    ele.setDgmLen(jsonEle.getIntValue("in_bytes"));
                    ele.setName(jsonEle.getString("appName"));

                    ele.setRxMinPktLength(jsonEle.getIntValue("rx_min_pkt_length"));
                    long flowStartTime = jsonEle.getLongValue("flowStartMicroseconds");
                    long flowEndTime = jsonEle.getLongValue("flowEndMicroseconds");
                    long flowDuration = flowEndTime - flowStartTime;
                    ele.setFlowStartTime(flowStartTime);
                    ele.setFlowEndTime(flowEndTime);
                    ele.setFlowDuration(flowDuration);
                    objectCollector.collect(ele);
                }
            }
        }
    }

    private static List<AppUserInfo> getAppUserInfosForList(String appUserInfoStream) {
        JSONObject jsonObject = JSON.parseObject(appUserInfoStream);
        Date ts = new Date();
        List<AppUserInfo> appUserInfos = new ArrayList<>();
        if (jsonObject.containsKey("@timestamp")) {
            Long tsLong = jsonObject.getLong("@timestamp");
            ts = new Date(tsLong / 1000);
        }
        if (jsonObject.containsKey("appuserinfo")) {
            JSONArray jsonArray = JSONArray.parseArray(jsonObject.getString("appuserinfo"));
            if (jsonArray == null) {
                appUserInfos.add(new AppUserInfo());
            } else {
                for (Object json : jsonArray) {
                    AppUserInfo ele = new AppUserInfo();
                    JSONObject jsonEle = (JSONObject) json;
                    ele.setTs(ts);
                    ele.setSip(jsonEle.getString("src_ip_address"));
                    ele.setSpt(jsonEle.getIntValue("l4_src_port"));
                    ele.setDip(jsonEle.getString("dst_ip_address"));
                    ele.setDpt(jsonEle.getIntValue("l4_dst_port"));

                    ele.setSrcMac(jsonEle.getString("in_src_mac"));
                    ele.setDstMac(jsonEle.getString("out_dst_mac"));
                    ele.setResponderBytes(jsonEle.getIntValue("responderBytes"));
                    ele.setRxMaxPktLength(jsonEle.getIntValue("rx_max_pkt_length"));
                    ele.setPktLoss(jsonEle.getIntValue("droppedOctetDeltaCount"));

                    ele.setInitiatorPackets(jsonEle.getIntValue("initiatorPackets"));
                    ele.setPackets(jsonEle.getIntValue("in_pkts"));

                    ele.setTxMinPktLength(jsonEle.getIntValue("tx_min_pkt_length"));
                    ele.setProt(jsonEle.getString("protocol"));

                    ele.setResponderPackets(jsonEle.getIntValue("responderPackets"));
                    ele.setMinPktInterval(jsonEle.getIntValue("min_pkt_interval"));

                    ele.setTxMaxPktLength(jsonEle.getIntValue("tx_max_pkt_length"));
                    ele.setMaxPktInterval(jsonEle.getIntValue("max_pkt_interval"));
                    ele.setDelay(jsonEle.getInteger("delay"));
                    ele.setMaxDelay(jsonEle.getIntValue("max_delay"));
                    ele.setMinDelay(jsonEle.getIntValue("min_delay"));

                    ele.setInitiatorBytes(jsonEle.getIntValue("initiatorBytes"));
                    ele.setDgmLen(jsonEle.getIntValue("in_bytes"));
                    ele.setName(jsonEle.getString("appName"));

                    ele.setRxMinPktLength(jsonEle.getIntValue("rx_min_pkt_length"));
                    long flowStartTime = jsonEle.getLongValue("flowStartMicroseconds");
                    long flowEndTime = jsonEle.getLongValue("flowEndMicroseconds");
                    long flowDuration = flowEndTime - flowStartTime;
                    ele.setFlowStartTime(flowStartTime);
                    ele.setFlowEndTime(flowEndTime);
                    ele.setFlowDuration(flowDuration);
                    appUserInfos.add(ele);
                }
            }
        }
        System.out.println(appUserInfos.size());
        return appUserInfos;
    }
}
