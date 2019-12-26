package cn.moretv.doraemon.kafka.sink.queue;

import cn.moretv.doraemon.kafka.sink.KafkaObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午2:50
 */
public class KafkaCouchbaseSinkPutToQueueTask implements Runnable {
    //Logger logger = LoggerFactory.getLogger(KafkaCouchbaseSinkPutToQueueTask.class);

    private String topic = "";
    private String groupName = "";
    private int threadIndex = 0;
    private BlockingQueue<ConsumerRecord> queue;
    private final Properties properties;

    public KafkaCouchbaseSinkPutToQueueTask(String topic, String groupName, int threadIndex, BlockingQueue<ConsumerRecord> queue,Properties properties) {
        this.topic = topic;
        this.groupName = groupName;
        this.threadIndex = threadIndex;
        this.queue = queue;
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "KafkaCouchbaseSinkPutToQueueTask{" +
                "topic='" + topic + '\'' +
                ", groupName='" + groupName + '\'' +
                ", threadIndex=" + threadIndex +
                '}';
    }


    @Override
    public void run() {
        System.out.println("toString:" + this.toString());
        KafkaObject kafkaObject = new KafkaObject();
        KafkaConsumer consumer = kafkaObject.initConsumer(groupName, topic,properties);
        int timeout = 100;
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        //System.out.println("thread put:" + threadIndex + ",Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                        queue.put(record);
                    }
                    timeout = 100;
                } else {
                    timeout = 1000;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
