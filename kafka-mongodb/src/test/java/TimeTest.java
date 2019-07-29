import com.asiainfo.breeze.util.TimeUtils;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/3/19 11:56
 */
public class TimeTest {
    public static void main(String[] args){
        long time = 1551432090663L;
        System.out.println(TimeUtils.getMonday(time));
    }
}
