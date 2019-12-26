package cn.moretv.doraemon.kafka.sink.mock;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;


/**
 * @author wang.baozhi
 * @since 2018/7/2 下午7:25
 */
public class TestReadConfigFile {
    private void getFile(String fileName) throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        /**
         getResource()方法会去classpath下找这个文件，获取到url resource, 得到这个资源后，调用url.getFile获取到 文件 的绝对路径
         */
        URL url = classLoader.getResource(fileName);
        /**
         * url.getFile() 得到这个文件的绝对路径
         */
        System.out.println(url.getFile());
        File file = new File(url.getFile());
        System.out.println(file.exists());
    }


    private void printProperties(String fileName) throws Exception {
        InputStream input = this.getClass().getClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(input);
        System.out.println(properties.getProperty("couchbase_hostname"));
    }

    private void readFromPath(String fileName) throws Exception {
       // String configFileAbsolutePath=".."+File.separator+"config"+File.separator+fileName;
        String configFileAbsolutePath="/Users/baozhiwang/local_dir/codes/doraemon/kafka-sink/src/main/resources"+File.separator+fileName;
        System.out.println("configFileAbsolutePath:"+configFileAbsolutePath);

        InputStream input = new FileInputStream(configFileAbsolutePath);
        Properties properties = new Properties();
        properties.load(input);
        System.out.println(properties.getProperty("couchbase_hostname"));
    }


    public static void main(String[] args) throws Exception {
        String fileName = "kafka-sink.properties";
        TestReadConfigFile testReadConfigFile = new TestReadConfigFile();
//        testReadConfigFile.printProperties2(fileName);
        testReadConfigFile.printProperties(fileName);
    }
}
