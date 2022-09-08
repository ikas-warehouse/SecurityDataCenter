package brd.common;

import com.alibaba.fastjson.JSONObject;

/**
 * @program stream-process
 * @description: 字符串相关工具
 * @author: 蒋青松
 * @create: 2022/09/08 14:13
 */
public class StringUtils {
    /**
     * 判断字符串是否为json格式
     * @param str
     * @return
     */
    public static boolean isjson(String str){
        try {
            JSONObject.parseObject(str);
            return  true;
        } catch (Exception e) {
            return false;
        }
    }
}

