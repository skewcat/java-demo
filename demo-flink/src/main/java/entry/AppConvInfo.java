package entry;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/17 11:57
 */
@Data
public class AppConvInfo implements Serializable {
    Date ts;//采集时间
    String name; //应用名称
    String sip; //源IP
    String dip; //目的IP
    Integer spt; //源端口
    Integer dpt; //目的端口
    String srcMac; //源mac
    String dstMac; //目的mac
    String prot; //协议
    Long dgmlen; //字节数
    Integer packets; //报文个数
    Double delay; //延时
    Integer pktLoss; //丢包
    Double pktLossRate; //丢包率
    Double jitter; //抖动
    Integer sessionNum; //会话数
    String firstArea; //区域1
    String secondArea; //区域2
    String area; //区域
    String building; //楼
    String floor; //层
    Integer score; //得分
    String details; //详情
    String appClass; //应用类型（10几大类）
    String idPattern; //区域id字符串
    String areaPattern; //区域名字符串
    Integer responderBytes; //下行字节数
    Integer initiatorBytes; //上行字节数

    @Override
    public String toString() {
        return "AppConvInfo{" +
                "ts=" + ts +
                ", name='" + name + '\'' +
                ", sip='" + sip + '\'' +
                ", dip='" + dip + '\'' +
                ", spt=" + spt +
                ", dpt=" + dpt +
                ", srcMac='" + srcMac + '\'' +
                ", dstMac='" + dstMac + '\'' +
                ", prot='" + prot + '\'' +
                ", dgmlen=" + dgmlen +
                ", packets=" + packets +
                ", delay=" + delay +
                ", pktLoss=" + pktLoss +
                ", pktLossRate=" + pktLossRate +
                ", jitter=" + jitter +
                ", sessionNum=" + sessionNum +
                ", firstArea='" + firstArea + '\'' +
                ", secondArea='" + secondArea + '\'' +
                ", area='" + area + '\'' +
                ", building='" + building + '\'' +
                ", floor='" + floor + '\'' +
                ", score=" + score +
                ", details='" + details + '\'' +
                ", appClass='" + appClass + '\'' +
                ", idPattern='" + idPattern + '\'' +
                ", areaPattern='" + areaPattern + '\'' +
                ", responderBytes=" + responderBytes +
                ", initiatorBytes=" + initiatorBytes +
                '}';
    }
}
