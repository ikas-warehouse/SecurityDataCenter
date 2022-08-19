package brd.asset.flink.task;

import brd.common.FlinkUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @program SecurityDataCenter
 * @description: doris sink test
 * @author: 蒋青松
 * @create: 2022/08/08 18:52
 */
public class DorisSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();

        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");

        DataStreamSource<String> source = env.fromElements("");


        SingleOutputStreamOperator<String> data = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                JSONObject obj = new JSONObject();
                obj.put("id", 1002);
                obj.put("time1", "2022-08-12 17:00:19");
                obj.put("score", 99);
                return obj.toJSONString();
            }
        });
        data.addSink(
                DorisSink.sink(
                        DorisReadOptions.builder().build(),
                        DorisExecutionOptions.builder()
                                .setBatchSize(300)
                                .setBatchIntervalMs(1L)
                                .setMaxRetries(3)
                                .setStreamLoadProp(pro).build(),
                        DorisOptions.builder()
                                .setFenodes("192.168.5.91:8031")
                                .setTableIdentifier("test_db.p_01")
                                .setUsername("root")
                                .setPassword("000000").build()
                ));

        env.execute("rowdata sink");
    }

}
