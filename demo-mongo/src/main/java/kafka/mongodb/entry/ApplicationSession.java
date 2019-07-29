package kafka.mongodb.entry;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/14 19:13
 */
@Data
public class ApplicationSession implements Serializable {
    public static final String SOURCE_IP = "sip";
    public static final String DESTINATION_IP = "dip";
    @Field("_id") private String _id;
    @Field("ts") private Date ts;
    private Long timestamp;
    @Field("name") private String name;
    @Field("sip") private String sourceIp;
    @Field("spt") private String sourcePort;
    @Field("dip") private String destinationIp;
    @Field("dpt") private String destinationPort;
    @Field("prot") private String protocol;
    @Field("dgmlen") private Integer traffic;
    @Field("delay") private Integer lag;
    //@Field("pktLoss") private Integer drop;
    @Field("pktLossRate") private Integer dropRate;
    @Field("jitter") private Integer jitter;
    @Field("rtt") private Integer rtt;
    @Field("srt") private Integer srt;
    @Field("details") private String details;
    @Field("score") private Integer score;
    @Field("idPattern") private String idPattern;

}
