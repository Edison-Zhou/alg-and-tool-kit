package cn.moretv.doraemon.kafka.sink;

/**
 * @author wang.baozhi
 * @since 2018/6/27 下午6:14
 */
public class Constants {
    public static String KEY="key";
    public static String VALUE="value";
    public static String TTL="ttl";
    public static String HOST="host";
    public static String PORT="port";
    public static String DB_INDEX="dbIndex";
    public static String FORMAT_TYPE="formatType";

    //redis formatType
    public static String KV="KV";
    public static String ZSET="ZSET";
    public static String HASH="HASH";

    //运行环境
    public static String ENV_PRODUCT="ENV_PRODUCT";
    public static String ENV_TEST="ENV_TEST";

    //产品线
    public static String MORETV="moretv";
    public static String HELIOS="helios";
    public static String MEDUSA="medusa";
    public static String KIDS="kids";
    public static String USEE = "usee";
    //couchbase moretv info配置
    public static String COUCHBASE_MORETV_TOPOC="couchbase-moretv-topic";
    public static String COUCHBASE_MORETV_GROUP_NAME ="couchbase-moretv-groupName";
    public static String COUCHBASE_MORETV_BUCKET_NAME ="couchbase-moretv-bucketName";
    public static String COUCHBASE_MORETV_THREAD_NUM_PUT ="couchbase-moretv-threadNumPut";
    public static String COUCHBASE_MORETV_THREAD_NUM_TAKE ="couchbase-moretv-threadNumTake";
    public static String COUCHBASE_MORETV_QUEUE_SIZE ="couchbase-moretv-queueSize";

    //couchbase medusa info配置
    public static String COUCHBASE_MEDUSA_TOPOC="couchbase-medusa-topic";
    public static String COUCHBASE_MEDUSA_GROUP_NAME ="couchbase-medusa-groupName";
    public static String COUCHBASE_MEDUSA_BUCKET_NAME ="couchbase-medusa-bucketName";
    public static String COUCHBASE_MEDUSA_THREAD_NUM_PUT ="couchbase-medusa-threadNumPut";
    public static String COUCHBASE_MEDUSA_THREAD_NUM_TAKE ="couchbase-medusa-threadNumTake";
    public static String COUCHBASE_MEDUSA_QUEUE_SIZE ="couchbase-medusa-queueSize";

    //couchbase kids info配置
    public static String COUCHBASE_KIDS_TOPOC="couchbase-kids-topic";
    public static String COUCHBASE_KIDS_GROUP_NAME ="couchbase-kids-groupName";
    public static String COUCHBASE_KIDS_BUCKET_NAME ="couchbase-kids-bucketName";
    public static String COUCHBASE_KIDS_THREAD_NUM_PUT ="couchbase-kids-threadNumPut";
    public static String COUCHBASE_KIDS_THREAD_NUM_TAKE ="couchbase-kids-threadNumTake";
    public static String COUCHBASE_KIDS_QUEUE_SIZE ="couchbase-kids-queueSize";

    //couchbase usee info配置
    public static String COUCHBASE_USEE_TOPOC="couchbase-usee-topic";
    public static String COUCHBASE_USEE_GROUP_NAME ="couchbase-usee-groupName";
    public static String COUCHBASE_USEE_BUCKET_NAME ="couchbase-usee-bucketName";
    public static String COUCHBASE_USEE_THREAD_NUM_PUT ="couchbase-usee-threadNumPut";
    public static String COUCHBASE_USEE_THREAD_NUM_TAKE ="couchbase-usee-threadNumTake";
    public static String COUCHBASE_USEE_QUEUE_SIZE ="couchbase-usee-queueSize";

    //couchbase helios info配置
    public static String COUCHBASE_HELIOS_TOPOC="couchbase-helios-topic";
    public static String COUCHBASE_HELIOS_GROUP_NAME ="couchbase-helios-groupName";
    public static String COUCHBASE_HELIOS_BUCKET_NAME ="couchbase-helios-bucketName";
    public static String COUCHBASE_HELIOS_THREAD_NUM_PUT ="couchbase-helios-threadNumPut";
    public static String COUCHBASE_HELIOS_THREAD_NUM_TAKE ="couchbase-helios-threadNumTake";
    public static String COUCHBASE_HELIOS_QUEUE_SIZE ="couchbase-helios-queueSize";

    //couchbase server配置
    public static String COUCHBASE_HOSTNAME ="couchbase-hostname";
    public static String COUCHBASE_USERNAME ="couchbase-username";
    public static String COUCHBASE_PASSWORD ="couchbase-password";

    //kafka集群配置
    public static String BOOTSTRAP_SERVERS_CONFIG="bootstrap-server-config";

    //redis moretv info配置
    public static String REDIS_MORETV_TOPOC="redis-moretv-topic";
    public static String REDIS_MORETV_GROUP_NAME ="redis-moretv-groupName";
    public static String REDIS_MORETV_THREAD_NUM ="redis-moretv-threadNum";


    //redis helios info配置
    public static String REDIS_HELIOS_TOPOC="redis-helios-topic";
    public static String REDIS_HELIOS_GROUP_NAME ="redis-helios-groupName";
    public static String REDIS_HELIOS_THREAD_NUM ="redis-helios-threadNum";

}
