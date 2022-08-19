package brd.asset.flink.task;

import brd.asset.pojo.test.P01;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;

/**
 * @program SecurityDataCenter
 * @description: 测试flinksql sink doris
 * @author: 蒋青松
 * @create: 2022/08/18 15:34
 */
public class FlinkTableSinkDoris {
    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // source only supports parallelism of 1
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //doris table
        tEnv.executeSql(
                "CREATE TABLE doris_test_sink (" +
                        "id INT," +
                        "time1 string," +
                        "score int" +
                        ") " +
                        "WITH (\n" +
                        "  'connector' = 'doris',\n" +
                        "  'fenodes' = '192.168.5.91:8031',\n" +
                        "  'table.identifier' = 'test_db.p_01',\n" +
                        "  'sink.batch.size' = '1',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = '000000'\n" +
                        ")");

        P01 p01 = new P01(11, 100, "2022-08-18 12:00:00");
        P01 p02 = new P01(12, 100, "2022-08-18 12:00:00");
        P01 p03 = new P01(13, 100, "2022-08-18 12:00:00");
        ArrayList<P01> list = new ArrayList<>();
        list.add(p01);
        list.add(p02);
        list.add(p03);
        DataStreamSource<P01> source = env.fromCollection(list);
        Table table = tEnv.fromDataStream(source);
        //tEnv.createTemporaryView("test", table);
        table.printSchema();

        Table result = tEnv.sqlQuery("select id,score from " + table);
        result.printSchema();
        tEnv.toDataStream(result).print();

        tEnv.executeSql("insert into doris_test_sink select id, time1, score from " + table);
        /*Table query = tEnv.sqlQuery("select * from doris_test_sink");
        tEnv.toDataStream(query).print("doris rs: ");*/

        env.execute("test table api");

    }
}
