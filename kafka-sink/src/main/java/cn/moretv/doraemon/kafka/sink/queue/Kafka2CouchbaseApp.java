package cn.moretv.doraemon.kafka.sink.queue;

import cn.moretv.doraemon.kafka.sink.Constants;
import cn.moretv.doraemon.kafka.sink.util.ConfigUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午2:37
 * <p>
 * 多线程共用一个BlockingQueue
 */
public class Kafka2CouchbaseApp {
    public static Logger logger = LoggerFactory.getLogger(Kafka2CouchbaseApp.class);

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

        String topic = properties.getProperty(Constants.COUCHBASE_MORETV_TOPOC);
        String groupName = properties.getProperty(Constants.COUCHBASE_MORETV_GROUP_NAME);
        String bucketName = properties.getProperty(Constants.COUCHBASE_MORETV_BUCKET_NAME);
        int threadNumPut = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_MORETV_THREAD_NUM_PUT));
        int threadNumTake = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_MORETV_THREAD_NUM_TAKE));
        int queueSize = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_MORETV_QUEUE_SIZE));

        if (productLine.equalsIgnoreCase(Constants.HELIOS)) {
            topic = properties.getProperty(Constants.COUCHBASE_HELIOS_TOPOC);
            groupName = properties.getProperty(Constants.COUCHBASE_HELIOS_GROUP_NAME);
            bucketName = properties.getProperty(Constants.COUCHBASE_HELIOS_BUCKET_NAME);
            threadNumPut = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_HELIOS_THREAD_NUM_PUT));
            threadNumTake = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_HELIOS_THREAD_NUM_TAKE));
            queueSize = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_HELIOS_QUEUE_SIZE));
        }else if(productLine.equalsIgnoreCase(Constants.MEDUSA)){
            topic = properties.getProperty(Constants.COUCHBASE_MEDUSA_TOPOC);
            groupName = properties.getProperty(Constants.COUCHBASE_MEDUSA_GROUP_NAME);
            bucketName = properties.getProperty(Constants.COUCHBASE_MEDUSA_BUCKET_NAME);
            threadNumPut = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_MEDUSA_THREAD_NUM_PUT));
            threadNumTake = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_MEDUSA_THREAD_NUM_TAKE));
            queueSize = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_MEDUSA_QUEUE_SIZE));
        }else if(productLine.equalsIgnoreCase(Constants.KIDS)){
            topic = properties.getProperty(Constants.COUCHBASE_KIDS_TOPOC);
            groupName = properties.getProperty(Constants.COUCHBASE_KIDS_GROUP_NAME);
            bucketName = properties.getProperty(Constants.COUCHBASE_KIDS_BUCKET_NAME);
            threadNumPut = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_KIDS_THREAD_NUM_PUT));
            threadNumTake = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_KIDS_THREAD_NUM_TAKE));
            queueSize = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_KIDS_QUEUE_SIZE));
        }else if(productLine.equalsIgnoreCase(Constants.USEE)){
            topic = properties.getProperty(Constants.COUCHBASE_USEE_TOPOC);
            groupName = properties.getProperty(Constants.COUCHBASE_USEE_GROUP_NAME);
            bucketName = properties.getProperty(Constants.COUCHBASE_USEE_BUCKET_NAME);
            threadNumPut = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_USEE_THREAD_NUM_PUT));
            threadNumTake = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_USEE_THREAD_NUM_TAKE));
            queueSize = Integer.parseInt(properties.getProperty(Constants.COUCHBASE_USEE_QUEUE_SIZE));
        }


        BlockingQueue<ConsumerRecord> queue = new ArrayBlockingQueue<>(queueSize);
        ExecutorService executorPut = Executors.newFixedThreadPool(threadNumPut);
        ExecutorService executorTake = Executors.newFixedThreadPool(threadNumTake);
        for (int i = 0; i < threadNumPut; i++) {
            executorPut.execute(new KafkaCouchbaseSinkPutToQueueTask(topic, groupName, i, queue, properties));
        }
        for (int i = 0; i < threadNumTake; i++) {
            executorTake.execute(new KafkaCouchbaseSinkTakeFromQueueTask(bucketName, i, queue, properties));
        }
    }

    private static void parameterCheck(String[] args) {
        if (args.length < 1) {
            System.out.println("need parameter like moretv or helios or kids or usee");
            System.exit(-1);
        }

        String productLine = args[0];
        if (productLine.equalsIgnoreCase(Constants.MORETV)
                || productLine.equalsIgnoreCase(Constants.HELIOS)
                || productLine.equalsIgnoreCase(Constants.MEDUSA)
                || productLine.equalsIgnoreCase(Constants.KIDS)
                || productLine.equalsIgnoreCase(Constants.USEE)) {
            System.out.println("productLine is " + productLine);
        }else {
            System.out.println("productLine:need parameter like moretv or helios or medusa or kids or usee");
            System.exit(-1);
        }

        String mode = args[1];
        if(mode.equalsIgnoreCase("product")||mode.equalsIgnoreCase("test")||mode.equalsIgnoreCase("dev")){
            System.out.println("mode is " + mode);
        } else {
            System.out.println("mode:need parameter like product or test or dev");
            System.exit(-1);
        }
    }

    public static void main(String[] args) {
        logger.info("main....");
        parameterCheck(args);
        String productLine = args[0];
        String mode = args[1];//product or test or dev
        Kafka2CouchbaseApp kafka2CouchbaseApp = new Kafka2CouchbaseApp();
        kafka2CouchbaseApp.run(productLine,mode);
    }
}
