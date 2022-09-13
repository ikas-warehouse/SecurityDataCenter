package brd.asset.entity;

import lombok.Data;

/**
 * @program SecurityDataCenter
 * @description: 告警表对象
 * @author: 蒋青松
 * @create: 2022/09/13 10:34
 */
@Data
public class EventAlarm {
    public String event_id;
    public String event_title;
    public String event_type;
    public String event_level;
    public String event_time;
    public String event_count;
    public String event_dev_ip;
    public String event_dev_mac;
    public String event_dev_type;
    public String event_device_factory;
    public String event_device_model;
    public String event_device_name;
    public String event_source_ip;
    public String event_source_port;
    public String event_source_adress;
    public String event_target_ip;
    public String event_target_port;
    public String event_target_adress;
    public String event_affected_dev;
    public String event_affected_dev_belong;
    public String event_description;
    public String trace_log_ids;
    public String protocol;
    public Double traffic_size;
    public String file_name;
    public String handle;
    public String handle_time;

    public EventAlarm() {
    }

    public EventAlarm(String event_id, String event_title, String event_type, String event_level, String event_time, String event_dev_ip, String event_dev_mac) {
        this.event_id = event_id;
        this.event_title = event_title;
        this.event_type = event_type;
        this.event_level = event_level;
        this.event_time = event_time;
        this.event_dev_ip = event_dev_ip;
        this.event_dev_mac = event_dev_mac;
    }
}
