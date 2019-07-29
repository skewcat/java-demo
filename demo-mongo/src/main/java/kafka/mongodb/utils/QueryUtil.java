package kafka.mongodb.utils;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/14 20:38
 */
/**
 * @author R09679
 * @Description: 用于解析、适配MongoDB查询语句
 * @date 2018/9/18 17:02
 */
public class QueryUtil {
    /**
     * 区域id组合键：idPattern
     */
    public static final String ZONE_PATTERN = "idPattern";

    /**
     * 空区域
     */
    public static final String Empty_ZONE = "";
    /**
     * _id
     * */
    public static final String ID = "_id";
    /**
     * 时间键：ts
     */
    public static final String TIMESTAMP = "ts";

    /**
     * 时间键：datetime(异常分析)
     */
    public static final String DATETIME = "datetime";

    /**
     * 设备id：did
     */
    public static final String DEVICE_ID = "did";

    /**
     * 安全分隔符
     */
    public static final String SECURITY_SPLIT = ":";

    /**
     * 分数
     */
    public static final String SCORE = "score";

    /**
     * 时间宽容值（历史趋势中，当web传进的endTime比数据库最后一条数据多15分钟，需要用宽容值插入1条空数据）
     */
    public static final Long endTimeTolerant = 900000L;
}