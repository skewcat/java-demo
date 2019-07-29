package kafka.mongodb.entry;

/**
 * Created by pengxingxiong@ruijie.com.cn on 2019/1/14 19:13
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: R09679
 * @Description: 分析保证返回包装
 * @Date: 2018/9/16
 */
public class AssuranceResponse<T> implements Serializable {
    private List<T> items;
    private Integer count;
    private Long timestamp;
    private String content;

    public List<T> getItems() {
        return items;
    }

    public void setItems(List<T> items) {
        this.items = items;
    }

    public void setItems(T item) {
        List<T> items = new ArrayList<>();
        items.add(item);
        this.items = items;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}

