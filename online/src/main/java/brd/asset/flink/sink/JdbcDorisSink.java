package brd.asset.flink.sink;

import brd.common.DruidUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

/**
 * @program JdbcDorisSink
 * @description: 连接jdbc写入doris
 * @author: 张世钰
 * @create: 2022/10/17 1:21
 */
public class JdbcDorisSink<T> extends RichSinkFunction<T> {
    private DruidDataSource druidDataSource;
    private DruidPooledConnection conn;
    private PreparedStatement preparedStatement;
    private String sql;
    private String strings;
    private Properties pro;

    public JdbcDorisSink(String sql, String strings, Properties pro) {
        this.sql = sql;
        this.strings = strings;
        this.pro = pro;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 创建连接池
        DruidUtil druidUtil = new DruidUtil(pro);
        druidDataSource = druidUtil.createDorisDataSource();

    }

    @Override
    public void invoke(T bean, Context context) throws Exception {
        Class<?> clazz = bean.getClass();
        //System.out.println("向 Doris 写入数据的 SQL " + sql); // 测试:打印sql
        try {
            // 获取数据库操作对象（预编译）
            conn = druidDataSource.getConnection();
            preparedStatement = conn.prepareStatement(sql);
        } catch (SQLException sqlException) {
            System.out.println("数据库操作对象获取异常~");
            sqlException.printStackTrace();
        }
        //注入sql
        try {
            Field[] declaredFields = clazz.getDeclaredFields();
            Field declaredField = declaredFields[declaredFields.length - 1];
            declaredField.setAccessible(true);
            HashMap map = (HashMap) declaredField.get(bean);
            String[] fields = strings.split(",");
            int index = 1;
            for (String field : fields) {
                preparedStatement.setObject(index, map.get(field));
                index++;
            }
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //写入
        try {
            preparedStatement.execute();
        } catch (SQLException sqlException) {
            System.out.println("Doris 写入 SQL 执行异常~");
            sqlException.printStackTrace();
        }

        Thread.sleep(100);  //防止连续update导致资源来不及释放报错.
        closeResource(preparedStatement, conn);
        Thread.sleep(100);
    }

    //资源释放方法
    private void closeResource(PreparedStatement preparedStatement, DruidPooledConnection conn) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException sqlException) {
                System.out.println("数据库操作对象释放异常~");
                sqlException.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlException) {
                System.out.println("德鲁伊连接对象释放异常~");
                sqlException.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException sqlException) {
                System.out.println("数据库操作对象释放异常~");
                sqlException.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlException) {
                System.out.println("德鲁伊连接对象释放异常~");
                sqlException.printStackTrace();
            }
        }
        if (druidDataSource != null) {
            try {
                druidDataSource.close();
            } catch (Exception e) {
                System.out.println("德鲁伊连接池释放异常~");
                e.printStackTrace();
            }

        }
    }
}
