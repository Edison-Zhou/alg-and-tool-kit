package cn.moretv.doraemon.kafka.sink;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午8:21
 */
public class KafkaObject {

/*
    public KafkaConsumer initConsumer(String groupName, String topic) {
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
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }*/

    public KafkaConsumer initConsumer(String groupName, String topic,Properties properties) {
        Properties props = new Properties();
        String kafkaHostName=properties.getProperty(Constants.BOOTSTRAP_SERVERS_CONFIG);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaHostName);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }
}
