package cn.moretv.doraemon.kafka.sink;

import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午2:50
 */
@Deprecated
public class KafkaCouchbaseSinkTask implements Runnable {
    //Logger logger = LoggerFactory.getLogger(KafkaCouchbaseSinkTask.class);

    public String topic = "";
    public String groupName = "";
    public String bucketName = "";
    public int threadIndex = 0;
    public int ttl = 172800;


    public KafkaCouchbaseSinkTask(String topic, String groupName, String bucketName, int threadIndex) {
        this.topic = topic;
        this.groupName = groupName;
        this.bucketName = bucketName;
        this.threadIndex = threadIndex;
    }

    @Override
    public String toString() {
        return "KafkaCouchbaseSinkTask{" +
                "topic='" + topic + '\'' +
                ", groupName='" + groupName + '\'' +
                ", bucketName='" + bucketName + '\'' +
                ", threadIndex=" + threadIndex +
                '}';
    }


    public KafkaConsumer initConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata-appsvr-130-1:9095," +
                "bigdata-appsvr-130-2:9095," +
                "bigdata-appsvr-130-3:9095," +
                "bigdata-appsvr-130-4:9095," +
                "bigdata-appsvr-130-5:9095," +
                "bigdata-appsvr-130-6:9095" +
                "bigdata-appsvr-130-7:9095," +
                "bigdata-appsvr-130-8:9095," +
                "bigdata-appsvr-130-9:9095");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(this.topic));
        return consumer;
    }

    public Bucket initCouchbaseBucket() {
        CouchbaseCluster cluster = CouchbaseCluster.create("bigtest-cmpt-129-204");
        cluster.authenticate("root", "123456");
        Bucket bucket = cluster.openBucket(bucketName);
        return bucket;
    }

    @Override
    public void run() {
        System.out.println("toString:" + this.toString());
        KafkaConsumer consumer = initConsumer();
        Bucket bucket = initCouchbaseBucket();
        AsyncBucket asyncBucket = bucket.async();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("thread" + threadIndex + ",Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                handleMessage(record, asyncBucket);
            }
        }
    }

    private void handleMessage(ConsumerRecord<String, String> record, AsyncBucket asyncBucket) {
      /*  ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode actualObj = objectMapper.readTree(record.value());
            String key = actualObj.get(Constants.KEY).asText();
            JsonNode valueJsonNode = actualObj.get(Constants.VALUE);
            System.out.println("valueJsonNode.asText():"+valueJsonNode.asText());
            JsonObject json = JsonObject.fromJson(valueJsonNode.asText());
            if(actualObj.has(Constants.TTL)){
                int ttl = actualObj.get(Constants.TTL).asInt();
                asyncBucket.upsert(JsonDocument.create(key, ttl, json)).subscribe();
            }else{
                asyncBucket.upsert(JsonDocument.create(key,json)).subscribe();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }
}
