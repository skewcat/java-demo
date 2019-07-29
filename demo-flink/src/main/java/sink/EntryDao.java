package sink;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoTimeoutException;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import java.util.UUID;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/17 15:14
 */
public class EntryDao {

    /**
     *
     * @param CollectionName 集合名
     */
    public static JSONObject save(String CollectionName, Object object) {
        JSONObject resp = new JSONObject();
        try {
            MongoTemplate mongoTemplate = MongoManager.getMongo();

            mongoTemplate.insert(object,CollectionName);
        } catch (MongoTimeoutException e1) {
            e1.printStackTrace();
            resp.put("ReasonMessage",e1.getClass() + ":" + e1.getMessage());
            return resp;
        } catch (Exception e) {
            e.printStackTrace();
            resp.put("ReasonMessage",e.getClass() + ":" + e.getMessage());
        }
        return resp;
    }
}
