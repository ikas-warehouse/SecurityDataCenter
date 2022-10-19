package brd.common;

import com.alibaba.druid.pool.DruidDataSource;

import java.util.Properties;

/**
 * @program DruidUtil
 * @description: 连接池工具类
 * @author: 张世钰
 * @create: 2022/10/17 1:58
 */
public class DruidUtil {
    private DruidDataSource druidDataSource;
    private String jdbc_driver;
    private String db_url_pattern;
    private String host;
    private String port;
    private String db;
    private String user;
    private String passwd;

    public DruidUtil(Properties pro) {
        jdbc_driver = pro.getProperty("jdbc_driver");
        db_url_pattern = pro.getProperty("db_url_pattern");
        host = pro.getProperty("host");
        port = pro.getProperty("port");
        db = pro.getProperty("db");
        user = pro.getProperty("user");
        passwd = pro.getProperty("passwd");
    }

    public DruidDataSource getDataSource(String driverClassName, String url) {
        // 创建连接池
        druidDataSource = new DruidDataSource();
        // 设置驱动全类名
        druidDataSource.setDriverClassName(driverClassName);
        // 设置连接 url
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(user);
        druidDataSource.setPassword(passwd);
        // 设置初始化连接池时池中连接的数量
        druidDataSource.setInitialSize(5);
        // 设置同时活跃的最大连接数
        druidDataSource.setMaxActive(20);
        // 设置空闲时的最小连接数，必须介于 0 和最大连接数之间，默认为 0
        druidDataSource.setMinIdle(1);
        // 设置没有空余连接时的等待时间，超时抛出异常，-1 表示一直等待
        druidDataSource.setMaxWait(-1);
        // 验证连接是否可用使用的 SQL 语句
        druidDataSource.setValidationQuery("select 1");
        // 指明连接是否被空闲连接回收器（如果有）进行检验，如果检测失败，则连接将被从池中去除
        // 注意，默认值为 true，如果没有设置 validationQuery，则报错
        // testWhileIdle is true, validationQuery not set
        druidDataSource.setTestWhileIdle(true);
        // 借出连接时，是否测试，设置为 false，不测试，否则很影响性能
        druidDataSource.setTestOnBorrow(false);
        // 归还连接时，是否测试
        druidDataSource.setTestOnReturn(false);
        // 设置空闲连接回收器每隔 30s 运行一次
        druidDataSource.setTimeBetweenEvictionRunsMillis(30 * 1000L);
        // 设置池中连接空闲 30min 被回收，默认值即为 30 min
        druidDataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000L);

        return druidDataSource;
    }

    public DruidDataSource createDorisDataSource() {
        return getDataSource(jdbc_driver, String.format(db_url_pattern, host, port, db));
    }
}