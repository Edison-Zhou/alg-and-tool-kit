package cn.moretv.doraemon.kafka.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午2:50
 * <p>
 * 每个线程创建一个Map<host_port_dbIndex,JedisPool>，避免不同线程共享对象，简化操作。
 */
public class KafkaRedisSinkTask implements Runnable {

    public String topic = "";
    public String groupName = "";
    public int threadIndex = 0;
    public Map<String, JedisPool> poolMap = new HashMap<>();
    private final Properties properties;

    public KafkaRedisSinkTask(String topic, String groupName, int threadIndex,Properties properties) {
        this.topic = topic;
        this.groupName = groupName;
        this.threadIndex = threadIndex;
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "KafkaRedisSinkTask{" +
                "topic='" + topic + '\'' +
                ", groupName='" + groupName + '\'' +
                ", threadIndex=" + threadIndex;
    }

    public JedisPool getJedisPool(String redisHost,int redisPort,int redisDB) {
        String key=redisHost+redisPort+redisDB;
        if (poolMap.containsKey(key)) {
            return poolMap.get(key);
        } else {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(100);
            config.setMaxWaitMillis(10000);
            JedisPool pool = new JedisPool(config, redisHost, redisPort, 100 * Protocol.DEFAULT_TIMEOUT, null, redisDB);
            poolMap.put(key, pool);
            return pool;
        }
    }


    @Override
    public void run() {
        System.out.println("toString:" + this.toString());
        KafkaObject kafkaObject = new KafkaObject();
        KafkaConsumer consumer = kafkaObject.initConsumer(groupName, topic,properties);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                handleMessage(record);
            }
        }
    }

    private void handleMessage(ConsumerRecord<String, String> record) {
        ObjectMapper objectMapper = new ObjectMapper();
        Jedis jedis = null;
        try {
            JsonNode actualObj = objectMapper.readTree(record.value());
            if(!actualObj.has(Constants.KEY)){
                System.out.println("not contain key--"+actualObj.toString());
            }
            String formatType = actualObj.get(Constants.FORMAT_TYPE).asText();
            String key = actualObj.get(Constants.KEY).asText();
            String host = actualObj.get(Constants.HOST).asText();
            int port = actualObj.get(Constants.PORT).asInt();
            int dbIndex = actualObj.get(Constants.DB_INDEX).asInt();
            int ttl = actualObj.get(Constants.TTL).asInt();
            JedisPool  jedisPool = getJedisPool(host,port,dbIndex);
            jedis = jedisPool.getResource();

            if (formatType.equalsIgnoreCase(Constants.ZSET)) {
                JsonNode jsonNode = actualObj.get(Constants.VALUE);
                Iterator<String> keys = jsonNode.fieldNames();
                Pipeline pipeline = jedis.pipelined();
                pipeline.del(key);
                while (keys.hasNext()) {
                    String member = keys.next();
                    Double score = jsonNode.get(member).asDouble();
                    //System.out.println("key:"+key+",member:"+member+",score:"+score+",formatType:" + formatType);
                    pipeline.zadd(key,score,member);
                    pipeline.expire(key,ttl);
                }
                pipeline.sync();
                pipeline.close();
            } else if (formatType.equalsIgnoreCase(Constants.HASH)) {
                JsonNode jsonNode = actualObj.get(Constants.VALUE);
                Iterator<String> keys = jsonNode.fieldNames();
                Pipeline pipeline = jedis.pipelined();
                pipeline.del(key);

                Map<String,String> hashMap=new HashMap<>();
                while (keys.hasNext()) {
                    String subKey  = keys.next();
                    String subValue=jsonNode.get(subKey).asText();
                    hashMap.put(subKey,subValue);
                }

                pipeline.hmset(key,hashMap);
                pipeline.expire(key,ttl);
                pipeline.sync();
                pipeline.close();
            } else if (formatType.equalsIgnoreCase(Constants.KV)) {
                JsonNode jNode=actualObj.get(Constants.VALUE);
                String value = jNode.asText();
                jedis.set(key,value);
                jedis.expire(key,ttl);
                //System.out.println("key:"+key+",value:"+value+  ",formatType:" + formatType  );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(jedis!=null){
              jedis.close();
            }
        }
    }
}
