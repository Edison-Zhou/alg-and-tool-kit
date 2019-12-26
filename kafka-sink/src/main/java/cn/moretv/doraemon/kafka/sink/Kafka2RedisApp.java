package cn.moretv.doraemon.kafka.sink;

import cn.moretv.doraemon.kafka.sink.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午2:37
 */
public class Kafka2RedisApp {
   public static Logger logger = LoggerFactory.getLogger(Kafka2RedisApp.class);

    private static void parameterCheck(String[] args) {
        if (args.length < 1) {
            System.out.println("need parameter like moretv or helios");
            System.exit(-1);
        }

        String productLine = args[0];
        if (productLine.equalsIgnoreCase(Constants.MORETV)
                || productLine.equalsIgnoreCase(Constants.HELIOS)) {
            System.out.println("productLine is " + productLine);
        } else {
            System.out.println("need parameter like moretv or helios");
            System.exit(-1);
        }
    }


    public void run(String productLine,String mode) {
        System.out.println("productLine:"+productLine+",mode:"+mode);
        ConfigUtils configUtils = new ConfigUtils();
        String fileName = "kafka-sink.properties";
        if(mode.equalsIgnoreCase("test")){
            fileName = "kafka-sink-test.properties";
        }else if(mode.equalsIgnoreCase("dev")){
            fileName = "kafka-sink-dev.properties";
        }
        //Properties properties=configUtils.getProperties(fileName, Constants.ENV_TEST);
        Properties properties = configUtils.getProperties(fileName, Constants.ENV_PRODUCT);
        configUtils.printProperties(properties);

        String topic = properties.getProperty(Constants.REDIS_MORETV_TOPOC);
        String groupName = properties.getProperty(Constants.REDIS_MORETV_GROUP_NAME);
        int threadNum = Integer.parseInt(properties.getProperty(Constants.REDIS_MORETV_THREAD_NUM));

        if (productLine.equalsIgnoreCase(Constants.HELIOS)) {
            topic = properties.getProperty(Constants.REDIS_HELIOS_TOPOC);
            groupName = properties.getProperty(Constants.REDIS_HELIOS_GROUP_NAME);
            threadNum  = Integer.parseInt(properties.getProperty(Constants.REDIS_HELIOS_THREAD_NUM));
        }

        ExecutorService executor= Executors.newFixedThreadPool(threadNum);
        for(int i=0;i<threadNum;i++){
            executor.execute(new KafkaRedisSinkTask(topic,groupName,i,properties));
        }
    }


    public static void main(String[] args) {
        logger.info("main");
        parameterCheck(args);
        String productLine = args[0];
        String mode = args[1];//product or test
        Kafka2RedisApp kafka2RedisApp=new Kafka2RedisApp();
        kafka2RedisApp.run(productLine,mode);
    }
}
