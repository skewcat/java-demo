package write;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/3/18 13:46
 * 缓存管理类 主要负责缓存中的管理，构造方法包含两个参数，一个是桶的数量，一个是键的有效期，已经解决并发问题，可以直接使用
 * ---------------------
 * 进行性能测试发现：
 * 未加同步锁机制的RotatingMap存储速度最快，但是存在并发问题；
 * 加了synchronized关键字的TimeCacheMap，但是性能最低；
 * 而改造后的类性能介于两者之间，且解决了并发问题；
 * 性能比大约是10:5:1；
 * 如果应用程序不存在并发问题建议使用RotatingMap，存在并发问题要求实时性的采用TimeCacheMap，追求性能对实时性要求不敏感的采用改造后的代码！
 * ---------------------
 */
public class UserCacheTemp<K, V> {
    // this default ensures things expire at most 50% past the expiration time
    private static final int DEFAULT_NUM_BUCKETS = 3;


    /**
     * 键到期回调接口
     *
     * @param <K>保存键的值
     * @param <V>保存值得类型
     * @author cong
     */
    public static interface ExpiredCallback<K, V> {
        /**
         * @param key 更新的键值
         * @param val 更新的值
         */
        public void expire(K key, V val);
    }


    /**
     * 存放数据的桶
     */
    private ConcurrentLinkedDeque<ConcurrentHashMap<K, V>> _buckets;


    /**
     * 回调方法
     */
    private ExpiredCallback _callback;


    /**
     * 清理线程
     */
    private Thread _cleaner;


    /**
     * 构造方法
     *
     * @param expirationSecs 键的有效期
     * @param numBuckets     存放的桶的数量
     * @param callback       回调函数
     */
    public UserCacheTemp(int expirationSecs, int numBuckets, ExpiredCallback<K, V> callback) {
        if (numBuckets < 2) {
            throw new IllegalArgumentException("numBuckets must be >= 2");
        }
        _buckets = new ConcurrentLinkedDeque<>();
        for (int i = 0; i < numBuckets; i++) {
            _buckets.add(new ConcurrentHashMap<>());
        }
        _callback = callback;
        final long expirationMillis = expirationSecs * 1000L;
        final long sleepTime = expirationMillis / (numBuckets - 1);
        _cleaner = new Thread(() -> {
            try {
                while (true) {
                    ConcurrentHashMap<K, V> dead = null;
                    Thread.sleep(sleepTime);//清理线程，每隔一段时间清理一次
                    rotate();
                }
            } catch (InterruptedException ex) {
            }
        });
        _cleaner.setDaemon(true);
        _cleaner.start();
    }


    public UserCacheTemp(int expirationSecs, ExpiredCallback<K, V> callback) {
        this(expirationSecs, DEFAULT_NUM_BUCKETS, callback);
    }


    public UserCacheTemp(int expirationSecs, int numBuckets) {
        this(expirationSecs, numBuckets, null);
    }


    /**
     * 回调方法
     *
     * @return 返回更新的键的集合
     */
    private ConcurrentHashMap<K, V> rotate() {
        ConcurrentHashMap<K, V> dead = null;
        dead = _buckets.removeLast();//在桶的尾部删除一个旧的集合
        _buckets.addFirst(new ConcurrentHashMap<>());//在桶的头部加入一个新的集合
        if (_callback != null) {
            for (Entry<K, V> entry : dead.entrySet()) {
                _callback.expire(entry.getKey(), entry.getValue());
            }
        }
        return dead;
    }


    /**
     * @param key 查找键的对象
     * @return 返回的键是否存在
     */
    public boolean containsKey(K key) {
        for (ConcurrentHashMap<K, V> bucket : _buckets) {
            if (bucket.containsKey(key)) {
                return true;
            }
        }
        return false;
    }


    /**
     * @param key 获得的键的对象
     * @return 返回的键的值
     */
    public V get(K key) {


        for (ConcurrentHashMap<K, V> bucket : _buckets) {
            if (bucket.containsKey(key)) {
                return bucket.get(key);
            }
        }
        return null;
    }


    /**
     * @param key   写入的键
     * @param value 写入键的值
     */
    public void put(K key, V value) {
        Iterator<ConcurrentHashMap<K, V>> it = _buckets.iterator();
        ConcurrentHashMap<K, V> bucket = it.next();
        bucket.put(key, value);
        while (it.hasNext()) {
            bucket = it.next();
            bucket.remove(key);
        }
    }


    /**
     * @param key 删除的键
     */
    public Object remove(K key) {


        for (ConcurrentHashMap<K, V> bucket : _buckets) {
            if (bucket.containsKey(key)) {
                return bucket.remove(key);
            }
        }
        return null;
    }


    /**
     * 返回键的总数
     */
    public int size() {
        int size = 0;
        for (ConcurrentHashMap<K, V> bucket : _buckets) {
            size += bucket.size();
        }
        return size;
    }
}
