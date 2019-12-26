package cn.moretv.doraemon.kafka.sink.util;

import cn.moretv.doraemon.kafka.sink.Constants;

import java.io.*;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/**
 * @author wang.baozhi
 * @since 2018/7/3 上午11:12
 */
public class ConfigUtils {

    public Properties getProperties(String fileName,String evn)  {
        Properties properties=null;
        try {
            String configFileAbsolutePath="";
            if(evn.equalsIgnoreCase(Constants.ENV_PRODUCT)){
                  configFileAbsolutePath=".."+File.separator+"config"+File.separator+fileName;

            }else  if(evn.equalsIgnoreCase(Constants.ENV_TEST)){
                configFileAbsolutePath = "/Users/baozhiwang/local_dir/codes/doraemon/kafka-sink/src/main/resources" + File.separator + fileName;
            }else{
                System.out.println("evn parameter not match");
                System.exit(-1);
            }

            InputStream input = new FileInputStream(configFileAbsolutePath);
            properties = new Properties();
            properties.load(input);
        }catch (IOException e){
            System.out.println("fileName"+fileName+" not found");
            e.printStackTrace();
            System.exit(-1);
        }
        return properties;
    }

    public void printProperties(Properties properties){
        Set<Object> keys = properties.keySet();
        for (Object key : keys) {
            String value=(key.toString().contains("password"))?"xxx":properties.get(key).toString();
            System.out.println(key.toString() + "=" +value);
        }

    }

}
