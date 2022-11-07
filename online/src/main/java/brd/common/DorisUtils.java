package brd.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.DorisDeserializationSchema;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;
import org.apache.doris.flink.source.DorisSourceBuilder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * @program DorisUtils
 * @description:
 * @author: 张世钰
 * @create: 2022/10/26 14:39
 */
public class DorisUtils {

    /**
     * DataStream方式获取DorisSource, 数据为无字段信息的List数据.
     *
     * @return DorisSource<List < ?>>
     */
    public static DorisSource<List<?>> getListDorisSource(String fe_ip, String tableName, String userName, String passWord) {
        DorisSource<List<?>> dorisSource = DorisSourceBuilder.<List<?>>builder()
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes(fe_ip)
                                .setTableIdentifier(tableName)
                                .setUsername(userName)
                                .setPassword(passWord)
                                .build())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();
        return dorisSource;
    }

    /**
     * DataStream获取DorisSource, 数据为JSONObject, key需要外部传入fields.
     *
     * @return DorisSource<JSONObject>
     */
    public static DorisSource<JSONObject> getJsonDorisSource(String fe_ip, String tableName, String userName, String passWord, String fields) {
        DorisSource<JSONObject> dorisSource = DorisSourceBuilder.<JSONObject>builder()
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes(fe_ip)
                                .setTableIdentifier(tableName)
                                .setUsername(userName)
                                .setPassword(passWord)
                                .build())
                .setDorisReadOptions(DorisReadOptions.builder().setReadFields(fields).build())
                .setDeserializer(new DorisDeserializationSchema<JSONObject>() {
                    @Override
                    public void deserialize(List<?> record, Collector<JSONObject> out) throws Exception {
                        int index = 0;
                        JSONObject result = new JSONObject();
                        for (String key : fields.split(",")) {
                            result.put(key, record.get(index));
                            index++;
                        }
                        out.collect(result);
                    }

                    @Override
                    public TypeInformation<JSONObject> getProducedType() {
                        return TypeInformation.of(new TypeHint<JSONObject>() {
                        });
                    }
                })
                .build();
        return dorisSource;
    }

    /**
     * DataStream获取DorisSink, 用于json格式数据流写入. 适用于kafka无界流数据, 对于doris有界流数据需要改为批处理模式.
     *
     * @return DorisSink<String>
     */
    public static DorisSink<String> getDorisSink(String host, String port, String db, String tb, String user, String pw, String labelPrefix) {
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

        return builder.build();

    }
}
