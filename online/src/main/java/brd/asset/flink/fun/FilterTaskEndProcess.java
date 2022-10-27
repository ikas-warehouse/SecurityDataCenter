package brd.asset.flink.fun;

import brd.asset.constants.ScanCollectConstant;
import brd.common.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.log4j.Logger;

/**
 * @program SecurityDataCenter
 * @description: 过滤任务结束标志数据
 * @author: 蒋青松
 * @create: 2022/09/08 20:14
 */
public class FilterTaskEndProcess extends ProcessFunction<String, JSONObject> {
    private static Logger log = Logger.getLogger(FilterTaskEndProcess.class);
    final OutputTag<JSONObject> taskEndTag = new OutputTag<JSONObject>("task-end") {};
    @Override
    public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        boolean isjson = StringUtils.isjson(value);
        if (isjson) {
            JSONObject originObject = JSON.parseObject(value);
            //resource info
            try {
                JSONObject resourceObj = JSON.parseObject(originObject.get(ScanCollectConstant.RESOURCE_INFO).toString());
                if (resourceObj.size() == 0) {
                    context.output(taskEndTag, originObject);
                } else {
                    collector.collect(originObject);
                }
            } catch (Exception e) {
                log.error("输入内容有误，content： " + value +" ,msg=" + e.getMessage());
            }
        }
    }
}
