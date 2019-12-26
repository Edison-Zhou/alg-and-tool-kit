package cn.moretv.doraemon.kafka.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午2:37
 */
@Deprecated
public class Kafka2CouchbaseApp {
   public static Logger logger = LoggerFactory.getLogger(Kafka2CouchbaseApp.class);

    public static void main(String[] args) {
        logger.info("main");
        String topic="couchbase-moretv-topic-test";
        String groupName="couchbase-moretv-consume-group-test";
        String bucketName="moretv";
        int threadNum=2;
        ExecutorService executor= Executors.newFixedThreadPool(threadNum);
        for(int i=0;i<threadNum;i++){
            executor.execute(new KafkaCouchbaseSinkTask(topic,groupName,bucketName,i));
        }
    }
}
