package brd.asset.flink.fun;

import brd.asset.entity.AssetBase;
import brd.common.TimeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @program SecurityDataCenter
 * @description: 任务结束后，更新assets_task表状态
 * @author: 蒋青松
 * @create: 2022/09/07 13:27
 */
public class AssetbaseUpdate2DorisProcess extends ProcessFunction<AssetBase, String> {
    private static final Logger logger = LoggerFactory.getLogger(AssetbaseUpdate2DorisProcess.class);

    private Connection connection = null;
    private PreparedStatement ps = null;
    private Properties properties;
    private String db;
    private String tb;

    public AssetbaseUpdate2DorisProcess(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection(properties.getProperty("url"),
                properties.getProperty("username"),
                properties.getProperty("password"));
        db = properties.getProperty("db");
        tb = properties.getProperty("table");
    }

    @Override
    public void processElement(AssetBase assetBase, ProcessFunction<AssetBase, String>.Context context, Collector<String> collector) throws Exception {

        String updateTime = TimeUtils.getNow("yyyy-MM-dd HH:mm:ss");
        String sql = "update " + db + "." + tb +
                " set device_name=?, " + //1
                "device_ip_ownership=?, " + //2
                "device_type=?, " + //3
                "data_base_info=?, " + //4
                "kernel_version=?, " + //5
                "message_oriented_middleware=?, " + //6
                "open_service_of_port=?, " + //7
                "os_info=?, " + //8
                "patch_properties=?, " + //9
                "program_info=?, " + //10
                "system_fingerprint_info=?, " + //11
                "label_id=?, " + //12
                "label_type1=?, " + //13
                "label_type2=?, " + //14
                "update_time=? " +//15
                "where device_ip=? " +//16
                "and device_mac=?"; //17
        try {
            ps = connection.prepareStatement(sql);
            ps.setString(1, assetBase.getDevice_name());
            ps.setString(2, assetBase.getDevice_ip_ownership());
            ps.setString(3, assetBase.getDevice_type());
            ps.setString(4, assetBase.getData_base_info());
            ps.setString(5, assetBase.getKernel_version());
            ps.setString(6, assetBase.getMessage_oriented_middleware());
            ps.setString(7, assetBase.getOpen_service_of_port());
            ps.setString(8, assetBase.getOs_info());
            ps.setString(9, assetBase.getPatch_properties());
            ps.setString(10, assetBase.getProgram_info());
            ps.setString(11, assetBase.getSystem_fingerprint_info());
            ps.setString(12, assetBase.getLabel_id());
            ps.setString(13, assetBase.getLabel_type1());
            ps.setString(14, assetBase.getLabel_type2());
            ps.setString(15, updateTime);

            ps.setString(16, assetBase.getDevice_ip());
            ps.setString(17, assetBase.getDevice_mac());
            ps.execute();
            System.out.println(assetBase.getAsset_id() + ": " + "更新完成！");
        } catch (Exception e) {
            logger.error("更新任务结束状态异常，msg=" + e.getMessage());
            System.out.println("更新任务结束状态异常，msg=" + e.getMessage());
        }
    }
}
