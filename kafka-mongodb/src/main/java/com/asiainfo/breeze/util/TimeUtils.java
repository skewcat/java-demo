package com.asiainfo.breeze.util;

import org.joda.time.LocalDateTime;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/3/13 13:44
 */
public class TimeUtils {
    public static String getMonday(long timestamp) {
        LocalDateTime localDateTime = new LocalDateTime(timestamp);
        int dayOfWeek = localDateTime.getDayOfWeek();
        return localDateTime.plusDays(1 - dayOfWeek).toLocalDate().toString();

    }
}
