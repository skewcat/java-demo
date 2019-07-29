package entry;

import lombok.Data;
import java.io.Serializable;
import java.util.Date;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/15 11:55
 */
@Data
public class AppUserInfo implements Serializable {
    Date ts; //时间
    String name = ""; //应用名称
    String sip; //源IP
    String dip;//目的IP
    int spt; //源端口
    int dpt;//目的端口
    String prot;//协议
    int dgmLen; //字节数
    int packets; //报文个数
    int delay; //延时
    int pktLoss; //丢包
    String srcMac;//源mac
    String dstMac;//目的mac
    long flowStartTime; //流开始时间
    long flowEndTime; //流结束时间
    long flowDuration; //流持续时间
    int responderBytes; //下行字节数
    int initiatorBytes; //上行字节数
    int responderPackets; //下行报文个数
    int initiatorPackets; //下行报文个数

    int rxMinPktLength; //下行报文长度最小值
    int rxMaxPktLength; //下行报文长度最大值
    int txMinPktLength; //上行报文长度最小值
    int txMaxPktLength; //上行报文长度最大值
    int minDelay; //时延最小值
    int maxDelay; //时延最大值
    int minPktInterval; //报文间隔最小值
    int maxPktInterval; //报文间隔最大值


    public String toString() {
        return "AppUserInfo{" +
                "ts=" + ts +
                ", name='" + name + '\'' +
                ", sip='" + sip + '\'' +
                ", dip='" + dip + '\'' +
                ", spt=" + spt +
                ", dpt=" + dpt +
                ", prot='" + prot + '\'' +
                ", dgmLen=" + dgmLen +
                ", packets=" + packets +
                ", delay=" + delay +
                ", pktLoss=" + pktLoss +
                ", srcMac='" + srcMac + '\'' +
                ", dstMac='" + dstMac + '\'' +
                ", flowStartTime=" + flowStartTime +
                ", flowEndTime=" + flowEndTime +
                ", flowDuration=" + flowDuration +
                ", responderBytes=" + responderBytes +
                ", initiatorBytes=" + initiatorBytes +
                ", responderPackets=" + responderPackets +
                ", initiatorPackets=" + initiatorPackets +
                ", rxMinPktLength=" + rxMinPktLength +
                ", rxMaxPktLength=" + rxMaxPktLength +
                ", txMinPktLength=" + txMinPktLength +
                ", txMaxPktLength=" + txMaxPktLength +
                ", minDelay=" + minDelay +
                ", maxDelay=" + maxDelay +
                ", minPktInterval=" + minPktInterval +
                ", maxPktInterval=" + maxPktInterval +
                '}';
    }
}