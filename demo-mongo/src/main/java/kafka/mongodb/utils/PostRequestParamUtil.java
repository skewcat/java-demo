package kafka.mongodb.utils;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/22 13:57
 */
public class PostRequestParamUtil {
    final private static Logger LOGGER = LoggerFactory.getLogger(PostRequestParamUtil.class);
    final public static int MIN_SCORE = 0;
    final public static int MAX_SCORE = 1;
    public static Long endTimeJudge(JSONObject param) {
        return (!param.containsKey("endTime") || param.getString("endTime") == null || ""
                .equals(param.getString("endTime")) || param.getLong("endTime") < 0) ? System.currentTimeMillis() :
                param.getLong("endTime");
    }

    /**
     * startTime格式化，无值或负值时请求改变为时刻请求(startTime = endTime)
     *
     * @param param
     * @return
     */
    public static Long startTimeJudge(JSONObject param) {
        Long endTime = endTimeJudge(param);
        return (!param.containsKey("startTime") || param.getString("startTime") == null || ""
                .equals(param.getString("startTime")) || param.getLong("startTime") < 0) ? (endTime - 900000L) :
                param.getLong("startTime");
    }
    /**
     * startTime格式化，15分钟数据
     *
     * @param param
     * @return
     */
    public static Long singleEndTimeApplication(JSONObject param) {
        Long endTime = endTimeJudge(param);
        return endTime - 900000L;
    }
    /**
     * page初始化
     *
     * @param param
     * @return
     */
    public static Integer pageJudge(JSONObject param) {
        return
                (param.getInteger("page") != null && !"".equals(param.getString("page")) && param.getInteger("page") >= 0) ?
                        param.getInteger("page") : 1;
    }

    /**
     * size初始化
     *
     * @param param
     * @return
     */
    public static Integer sizeJudge(JSONObject param) {
        return
                (param.getInteger("size") != null && !"".equals(param.getString("page")) && param.getInteger("size") >= 0) ?
                        param.getInteger("size") : Integer.MAX_VALUE;
    }
    /**
     * sort初始化
     *
     * @param param
     * @return
     */
    public static List<String> sortJudge(JSONObject param) {
        List<String> sorts = new ArrayList<>();
        if (param.getString("sort") == null || param.getString("sort") == "") {
            List<String> resultList = new ArrayList<>();
            resultList.add("ts");
            resultList.add("desc");
            return resultList;
        }
        for (String str : param.getString("sort").split(",")) {
            sorts.add(str);
        }
        return sorts;
    }
    /**
     * 用于解析location信息
     * @param param
     * @param zoneResourceApiClient
     * @return
     */
    public static String parserZoneId(JSONObject param, ZoneResourceApiClient zoneResourceApiClient) {
        Long zoneId = locationJudge(param);
        return parserZoneId(zoneId, zoneResourceApiClient);
    }
}
