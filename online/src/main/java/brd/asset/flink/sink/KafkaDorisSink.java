package brd.asset.flink.sink;

import brd.asset.flink.fun.JsonFilterFunction;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @program KafkaDorisSink
 * @description: Kafka数据导入Doris通用Source + Sink
 * @author: 张世钰
 * @create: 2022/10/10
 */

public class KafkaDorisSink<T> {
    private StreamExecutionEnvironment env;
    private String strings;
    private String brokers;
    private String topic;
    private String groupId;
    private String db;
    private String tb;
    private String host;
    private String port;
    private String user;
    private String pw;
    private String labelPrefix;
    private Integer kafkaParallelism;
    private Integer dorisParallelism;


    public KafkaDorisSink(StreamExecutionEnvironment env, Properties properties, Integer kafkaParallelism, Integer dorisParallelism) {
        this.env = env;
        this.strings = properties.getProperty("strings");
        this.brokers = properties.getProperty("brokers");
        this.topic = properties.getProperty("topic");
        this.groupId = properties.getProperty("groupId");
        this.db = properties.getProperty("db");
        this.tb = properties.getProperty("table");
        this.host = properties.getProperty("host");
        this.port = properties.getProperty("port");
        this.user = properties.getProperty("username");
        this.pw = properties.getProperty("password");
        this.labelPrefix = properties.getProperty("labelPrefix");
        this.dorisParallelism = dorisParallelism;
        this.kafkaParallelism = kafkaParallelism;
    }

    public KafkaDorisSink() {
    }

    public void sink() {
        //kafka-source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), tb + "-kafka-source").setParallelism(kafkaParallelism);


        //doris-sink
        DorisSink.Builder<String> builder = DorisSink.builder();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        pro.setProperty("line_delimiter", "\n");
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes(host + ":" + port)
                .setTableIdentifier(db + "." + tb)
                .setUsername(user)
                .setPassword(pw);

        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder
                .setStreamLoadProp(pro)
                .setLabelPrefix(labelPrefix);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        //过滤,转jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(new JsonFilterFunction(strings));

        SingleOutputStreamOperator<String> jsonDS = jsonObjDS.map(JSONAware::toJSONString);

        jsonDS.sinkTo(builder.build()).name(tb + "-doris-sink").setParallelism(dorisParallelism);


    }
}
