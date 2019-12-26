package cn.moretv.doraemon.kafka.sink.mock;


import cn.moretv.doraemon.kafka.sink.Constants;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wang.baozhi
 * @since 2018/7/2 下午12:48
 */
public class KafkaProductMock {
    private String BOOTSTRAP_SERVERS = "bigdata-appsvr-130-1:9095," +
            "bigdata-appsvr-130-2:9095," +
            "bigdata-appsvr-130-3:9095," +
            "bigdata-appsvr-130-4:9095," +
            "bigdata-appsvr-130-5:9095," +
            "bigdata-appsvr-130-6:9095" +
            "bigdata-appsvr-130-7:9095," +
            "bigdata-appsvr-130-8:9095," +
            "bigdata-appsvr-130-9:9095";
    private int loopMax = 2;
    private AtomicInteger atomicInteger = new AtomicInteger();

    public void setLoopMax(int loopMax) {
        this.loopMax = loopMax;
    }

    private Producer<String, String> createProducerInstance() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, this.getClass().getName() + UUID.randomUUID().toString());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(config);
    }

    class CouchBaseThreadTask implements Runnable {

        public void CouchBaseThreadTask(){

        }

        @Override
        public void run() {
            String topic = "couchbase-moretv-topic-test";
            Producer producer = createProducerInstance();

            for (int i = 0; i < loopMax; i++) {
                ObjectMapper objectMapper = new ObjectMapper();
                ObjectNode value=objectMapper.createObjectNode();

                ObjectNode report = objectMapper.createObjectNode();
                report.put("alg", "dl_v2");
                report.put("timestamp", System.currentTimeMillis());
                report.put(Constants.TTL, 0);

                value.put(Constants.KEY, atomicInteger.addAndGet(1));
                value.put(Constants.VALUE,report.toString());
                //System.out.println("value.toString():"+value.toString());

                String key = UUID.randomUUID().toString();
                ProducerRecord<String, String> msg = new ProducerRecord<>(topic, key, value.toString());
                producer.send(msg);
            }
        // String jsonString="{\"key\":\"c:m:uid1\",\"value\":\"{\"id\":[11,12],\"alg\":\"dl_v2\",\"timestamp\":20180629071925}\",\"ttl\":0}\"";
        }
    }


    public void startCouchBaseMock() throws Exception {
        int threadNum = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        List<Future<?>> list = new ArrayList<>();
        Future<?> future=null;
        for (int i = 0; i < threadNum; i++) {
            future =executorService.submit(new CouchBaseThreadTask());
            System.out.println("future.isDone():"+future.isDone());
            list.add(future);
        }

       /* for(Future<?> f : list){
            f.get(5,TimeUnit.SECONDS);
        }

        executorService.shutdownNow();
        executorService.awaitTermination(10,TimeUnit.SECONDS);
*/
//        while(future.isDone()){
//            executorService.shutdownNow();
//            executorService.awaitTermination(10,TimeUnit.SECONDS);
//        }
    }


   /**
    * redis db 0~4  存放KV格式的数据
    * redis db 6~10 存放ZSET格式的数据
    * 穿插数据到kafka队列中，测试jPool
    * */
    class RedisThreadTask implements Runnable {
        @Override
        public void run() {
            String topic = "redis-moretv-topic-test";
            Producer producer = createProducerInstance();

            for (int i = 0; i < loopMax; i++) {
                //通用
                int key=atomicInteger.addAndGet(1);
                ObjectMapper objectMapper = new ObjectMapper();
                ObjectNode kafkaValue=objectMapper.createObjectNode();
                kafkaValue.put(Constants.KEY,  key);
                kafkaValue.put(Constants.TTL,3600);
                kafkaValue.put(Constants.HOST,"bigtest-cmpt-129-204");
                kafkaValue.put(Constants.PORT,6379);

                //for KV
                int dIndex=i%5;
                kafkaValue.put(Constants.DB_INDEX,dIndex);
                kafkaValue.put(Constants.FORMAT_TYPE,Constants.KV);
                kafkaValue.put(Constants.VALUE,1);
                String kafkaKey = UUID.randomUUID().toString();
                System.out.println(kafkaValue.toString());
                ProducerRecord<String, String> msg = new ProducerRecord<>(topic, kafkaKey, kafkaValue.toString());
                producer.send(msg);


                //for ZSET
                dIndex=dIndex+6;
                kafkaValue.put(Constants.DB_INDEX,dIndex);
                ObjectNode redisValue = objectMapper.createObjectNode();
                redisValue.put("member1", 1);
                redisValue.put("member2", 2);
                kafkaValue.set(Constants.VALUE,redisValue);
                kafkaValue.put(Constants.FORMAT_TYPE,Constants.ZSET);
                kafkaKey = UUID.randomUUID().toString();
                System.out.println(kafkaValue.toString());
                msg = new ProducerRecord<>(topic, kafkaKey, kafkaValue.toString());
                producer.send(msg);
            }
        }
    }

    public void startRedisMock() throws Exception {
        int threadNum = 2;
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            Future<?> future =executorService.submit(new RedisThreadTask());
            System.out.println("future.isDone():"+future.isDone());
        }
    }

    public static void main(String[] args) throws Exception {
        KafkaProductMock kafkaProductMock = new KafkaProductMock();
        String type=args[0];
        int loopMax=Integer.parseInt(args[1]);
        kafkaProductMock.setLoopMax(loopMax);

        if (type.equalsIgnoreCase("redis")) {
            System.out.println("start redis mock ....");
            kafkaProductMock.startRedisMock();
        } else {
            System.out.println("start couchbase mock ....");
            kafkaProductMock.startCouchBaseMock();
        }

    }
}
