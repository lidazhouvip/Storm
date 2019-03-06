package com.ldz.bigdata.intergration.kafka;


import org.apache.commons.lang3.time.FastDateFormat;

/**
 * @Author: Dazhou Li
 * @Description:时间解析工具类
 * @CreateDate: 2019/2/18 0018 20:49
 */
public class DateUtils {

    private DateUtils(){}

    private static DateUtils instance;

    public static DateUtils getInstance(){
        if (instance==null){
            instance=new DateUtils();
        }
        return instance;
    }

    FastDateFormat format=FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public long getTime(String time) throws Exception{
        return format.parse(time.substring(1,time.length()-1)).getTime();
    }
}
