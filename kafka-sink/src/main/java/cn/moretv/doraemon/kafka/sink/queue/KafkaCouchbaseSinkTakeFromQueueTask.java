package cn.moretv.doraemon.kafka.sink.queue;

import cn.moretv.doraemon.kafka.sink.Constants;
import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午2:50
 */
public class KafkaCouchbaseSinkTakeFromQueueTask implements Runnable {
    Logger logger = LoggerFactory.getLogger(this.getClass());

    private String bucketName = "";
    private int threadIndex = 0;
    private BlockingQueue<ConsumerRecord> queue;
    private final Properties proerties;


    /**
     * @param bucketName
     * @param threadIndex
     * @param queue
     */
    public KafkaCouchbaseSinkTakeFromQueueTask(String bucketName, int threadIndex, BlockingQueue<ConsumerRecord> queue,Properties proerties) {
        this.bucketName = bucketName;
        this.threadIndex = threadIndex;
        this.queue = queue;
        this.proerties = proerties;
    }

    @Override
    public String toString() {
        return "KafkaCouchbaseSinkPutToQueueTask{" +
                ", bucketName='" + bucketName + '\'' +
                ", threadIndex=" + threadIndex +
                '}';
    }


    @Override
    public void run() {
        System.out.println("toString:" + this.toString());
        Bucket bucket = initCouchbaseBucket();
        AsyncBucket asyncBucket = bucket.async();
          while (true) {
             try {
                 ConsumerRecord<String, String> record=  queue.take();
                 //System.out.println("thread take" + threadIndex + ",Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                 handleCouchbaseRecord(record,asyncBucket);
             }catch (Exception e){
                 e.printStackTrace();
             }
        }
    }

    public Bucket initCouchbaseBucket() {
        String hostname=proerties.getProperty(Constants.COUCHBASE_HOSTNAME);
        String userName=proerties.getProperty(Constants.COUCHBASE_USERNAME);
        String password=proerties.getProperty(Constants.COUCHBASE_PASSWORD);

        CouchbaseCluster cluster = CouchbaseCluster.create(hostname);
        cluster.authenticate(userName, password);
        Bucket bucket = cluster.openBucket(bucketName,5, TimeUnit.SECONDS);
        return bucket;
    }

    private void handleCouchbaseRecord(ConsumerRecord<String, String> record, AsyncBucket asyncBucket) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            JsonNode actualObj = objectMapper.readTree(record.value());
            String key = actualObj.get(Constants.KEY).asText();
            JsonNode valueJsonNode = actualObj.get(Constants.VALUE);
            //System.out.println("valueJsonNode.asText():"+valueJsonNode.asText());
            JsonObject json = JsonObject.fromJson(valueJsonNode.asText());
            if(actualObj.has(Constants.TTL)){
                int ttl = actualObj.get(Constants.TTL).asInt();
                asyncBucket.upsert(JsonDocument.create(key, ttl, json)).subscribe(new mySubscriber());
            }else{
                asyncBucket.upsert(JsonDocument.create(key,json)).subscribe(new mySubscriber());
            }
        } catch (BackpressureException e) {

        } catch (Exception e) {
            e.printStackTrace();
            //捕获所有类型异常保证程序能够处理接下来的请求
        }
    }

    class mySubscriber extends Subscriber<Document>{
        @Override
        public void onStart() {
            request(10);
        }
        @Override
        public void onCompleted() {

        }

        @Override
        public void onError(Throwable e) {
        }

        @Override
        public void onNext(Document document) {
           request(10);
        }
    }

}
