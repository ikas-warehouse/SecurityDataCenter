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
    private String fieldString;
    private String keyString;

    public JsonFilterFunction(String fieldString, String keyString) {
        this.fieldString = fieldString;
        this.keyString = keyString;
    }

    @Override
    public void processElement(String jsonStr, Context context, Collector<JSONObject> collector) throws Exception {
        if (StringUtils.isjson(jsonStr) && jsonStr != null && jsonStr.replaceAll("\\s*", "").length() != 0) {
            JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            JSONObject result = new JSONObject();
            String[] fields = fieldString.split(",");

            boolean flag = true;
            for (String key : keyString.split(",")) {
                if (jsonObject.get(key) == null) {
                    flag = false;
                    break;
                }
            }
            if (flag) {
                for (String field : fields) {
                    if (jsonObject.get(field) != null) {
                        result.put(field, jsonObject.get(field));
                    }
                }
                collector.collect(result);
            }
        }
    }
}
