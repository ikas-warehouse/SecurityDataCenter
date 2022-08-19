package brd.asset.pojo.test;

import lombok.Data;

/**
 * @program SecurityDataCenter
 * @description:
 * @author: 蒋青松
 * @create: 2022/08/18 16:00
 */
@Data
public class P01 {
    public Integer id;
    public Integer score;
    public String time1;

    public P01() {
    }

    public P01(Integer id, Integer score, String time1) {
        this.id = id;
        this.score = score;
        this.time1 = time1;
    }
}
