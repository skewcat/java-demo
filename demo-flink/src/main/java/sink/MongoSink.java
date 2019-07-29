package sink;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.net.UnknownHostException;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/17 15:11
 */
public class MongoSink extends RichSinkFunction<String> {
//    public static  String CollectionName = "collection-a";
//    private MongoService mongoService;
//    @Override
//    public void invoke(String t) {
//        try {
//            this.mongoService.saveJson(JSON.parseObject(t),CollectionName);
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//    }
//    @Override
//    public void open(Configuration config) {
//        mongoService = new MongoService();
//        try {
//            super.open(config);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
