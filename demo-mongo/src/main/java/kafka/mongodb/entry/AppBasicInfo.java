package kafka.mongodb.entry;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/14 19:14
 */

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * @Author: R09679
 * @Description: 应用列表
 * @Date: 2018/9/16
 */
@Data
@Document(collection = AppBasicInfo.APP_BASIC_COLLECTION)
public class AppBasicInfo {
    public static final String APP_BASIC_COLLECTION = "appBasic";

    private String name;
    private String udpPort;
    private String tcpPort;
    private String protocol;
    private Date ts;
    private Integer score;
    private Boolean beHealth;
    private String appType;
    private Integer sessNum;
    private Long traffic;
    private Integer pktLoss;
    private Double pktLossRate;
    private Double delay;
    private Double jitter;
    private Integer srt;
    private Integer rtt;
    private String idPattern;
}

