package brd.asset.flink.fun;

import brd.common.StringUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @program DataToDoris
 * @description: 自定义ProcessFunction 按字段过滤kafka数据;
 * @author: 张世钰
 * @create: 2022/10/18 9:47
 */

public class JsonFilterFunction extends ProcessFunction<String, JSONObject> {
    private String strings;

    public JsonFilterFunction(String strings) {
        this.strings = strings;
    }

    @Override
    public void processElement(String jsonStr, Context context, Collector<JSONObject> collector) throws Exception {
        if (StringUtils.isjson(jsonStr)) {
            JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            JSONObject result = new JSONObject();
            String[] fields = strings.split(",");
            if (jsonObject.containsKey(fields[0]) && jsonObject.get(fields[0]) != null) {
                for (String field : fields) {
                    result.put(field, jsonObject.get(field));
                }
                collector.collect(result);
            }
        }
    }
}
