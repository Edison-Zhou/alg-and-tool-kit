package cn.moretv.doraemon.kafka.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

/**
 * @author wang.baozhi
 * @since 2018/6/28 下午8:06
 */
public class KafkaRedisAppTest {

    /**
     *

     ----生产环境
     使用redis mac客户端
     test_recommend:3>get 45793951
     "{"date":"20180531","similar":{"id":[45784431,45711121,47895541],"alg":"word2vec"},"theme":{"id":[64731085,48074491],"title":"更多精彩","alg":"random"}}"


     使用命令行
     10.10.158.252:6422[3]> get 45793951
     "{\"date\":\"20180531\",\"similar\":{\"id\":[45784431,47895541],\"alg\":\"word2vec\"},\"theme\":{\"id\":[64731085,48074491],\"title\":\"\xe6\x9b\xb4\xe5\xa4\x9a\xe7\xb2\xbe\xe5\xbd\xa9\",\"alg\":\"random\"}}"


     ----打包
     使用redis mac客户端
     mac_redis_test:0>get a
     ""{\"s\":{\"b\":\"b1\",\"bb\":\"bb1\"}}""

     使用命令行
     127.0.0.1:6379> get a
     "\"{\\\"s\\\":{\\\"b\\\":\\\"b1\\\",\\\"bb\\\":\\\"bb1\\\"}}\""

     jNode.asText();后
     使用redis mac客户端
     "{"s":{"b":"b1","bb":"bb1"}}"

     使用命令行
     "{\"s\":{\"b\":\"b1\",\"bb\":\"bb1\"}}"


     * */


  @Test
  public void testJson(){
   String s="{\"date\":\"20180531\",\"similar\":{\"id\":[45784431,47895541],\"alg\":\"word2vec\"},\"theme\":{\"id\":[64731085,48074491],\"alg\":\"random\"}}";
   System.out.println(s);
    String  b= "\"{\\\"s\\\":{\\\"b\\\":\\\"b1\\\",\\\"bb\\\":\\\"bb1\\\"}}\"";
    System.out.println(b);

  }

    @Test
    public void testCouchbaseJson(){
        String s= "{\"k41\":41,\"k42\":42}";
        System.out.println(s);
    }

    @Test
    public void printProperties() throws Exception {
        String fileName = "kafka-sink.properties";
        InputStream input = this.getClass().getClassLoader().getResourceAsStream(fileName);
        Properties properties = new Properties();
        properties.load(input);
        System.out.println(properties.getProperty("couchbase_hostname"));
    }


    /**
     {
     "key":"b",
     "value":{
       "bbb":0.2,
       "bb":0.4,
       "bbbb":0.3
     },
     "host":"bigtest-cmpt-129-204",
     "port":6379,
     "dbIndex":1,
     "ttl":8640000,
     "formatType":"ZSET"
     }
     *
     * */
    @Test
    public void zsetValueMock() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode value=objectMapper.createObjectNode();
        value.put(Constants.KEY, 1);
        value.put(Constants.TTL,3600);
        value.put(Constants.HOST,"bigtest-cmpt-129-204");
        value.put(Constants.PORT,6379);

        ObjectNode report = objectMapper.createObjectNode();
        report.put("member1", 2);
        report.put("member2", 3);
        value.set(Constants.VALUE,report);

        int dIndex=6;
        value.put(Constants.DB_INDEX,dIndex);
        value.put(Constants.FORMAT_TYPE,Constants.ZSET);

        System.out.println(value.toString());
    }


    @Test
    public void testTTL() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode value=objectMapper.createObjectNode();
        value.put(Constants.TTL,172800000);

        System.out.println(value.toString());
        if(value.has(Constants.TTL)){
            int ttl = value.get(Constants.TTL).asInt();
            System.out.println(ttl);
        }

     }


}
