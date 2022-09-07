package brd.asset.flink.sink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

/**
 * @program SecurityDataCenter
 * @description: doris sink
 * @author: 蒋青松
 * @create: 2022/08/11 16:14
 */
public class DorisSinkBase {
    private String host;
    private String port;
    private String db;
    private String table;
    private String user;
    private String pw;
    private Integer batchSize;
    private Long batchIntervalMs;

    public DorisSinkBase(String host, String port, String db, String table, String user, String pw, Integer batchSize, Long batchIntervalMs) {
        this.host = host;
        this.port = port;
        this.db = db;
        this.table = table;
        this.user = user;
        this.pw = pw;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
    }

    public SinkFunction<Object> getSink() {
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");
        SinkFunction<Object> sink = DorisSink.sink(
                DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder()
                        .setBatchSize(batchSize)
                        .setBatchIntervalMs(batchIntervalMs)
                        .setMaxRetries(3)
                        .setStreamLoadProp(pro).build(),
                DorisOptions.builder()
                        //todo 集群地址改造
                        //.setFenodes(host  + ":"+ port)
                        .setFenodes(host  + ":8031")
                        .setTableIdentifier(db + "." + table)
                        .setUsername(user)
                        .setPassword(pw).build()
        );

        return sink;
    }
}
