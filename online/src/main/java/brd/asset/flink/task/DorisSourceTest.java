package brd.asset.flink.task;

import brd.common.FlinkUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @program SecurityDataCenter
 * @description: 测试doris数据的实时
 * @author: 蒋青松
 * @create: 2022/08/09 17:13
 */
public class DorisSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        DataStreamSource<String> sourceDS = env.addSource(new CustomSourceForDoris());
        sourceDS.print();
        env.execute("custom doris source");
    }

}
