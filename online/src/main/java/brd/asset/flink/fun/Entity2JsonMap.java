package brd.asset.flink.fun;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @program SecurityDataCenter
 * @description: 对象转json的map函数
 * @author: 蒋青松
 * @create: 2022/08/16 16:48
 */
public class Entity2JsonMap<T> extends RichMapFunction<T, String> {
    @Override
    public String map(T o) throws Exception {

        String json = JSONObject.toJSONString(o);

        return json;
    }
}
