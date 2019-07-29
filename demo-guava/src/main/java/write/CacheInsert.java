package write;

import java.util.ArrayList;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/3/18 13:52
 */
public class CacheInsert {
    public static void main(String[] args) throws InterruptedException {
        UserCacheTemp.ExpiredCallback<String, ArrayList<String>> callback = (key, val) -> {
            System.out.println("clean data: key=" + key + ",value=" + val);
        };
        int expirationSecs = 5;
        UserCacheTemp<String, ArrayList<String>> userCacheTemp = new UserCacheTemp<>(expirationSecs, callback);
        if (!userCacheTemp.containsKey("key1")){
            ArrayList<String> value = new ArrayList<>();
            value.add("val1");
            userCacheTemp.put("key1",value);
        }else {
            userCacheTemp.get("key1").add("val2");
        }
        while (userCacheTemp.containsKey("key1")) {
            Thread.sleep(1000);
            System.out.println(userCacheTemp.get("key1"));

        }
    }
}
