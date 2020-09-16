package com.xiaohongshu.db.hercules.kafka.schema.manager;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.kafka.KafkaKV;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOptionConf;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaManager {

    private final GenericOptions options;
    private volatile Producer<Object, Object> producer;
    private final String topic;

    private DataType keyType;
    private DataType valueType;

    public KafkaManager(GenericOptions options) {
        this.options = options;
        this.topic = options.getString(KafkaOptionConf.TOPIC, "");
        this.producer = null;
//        checkKafkaConn();
    }

    // 检测kafka连接是否可以连通
//    public void checkKafkaConn() {
//        String bootstrapServers = options.getString(KafkaOptionConf.BOOTSTRAP_SERVERS, "");
//
//        Properties props = new Properties();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        AdminClient adminClient = AdminClient.create(props);
//        Set<String> topicSet;
//        try {
//            topicSet = adminClient.listTopics().names().get();
//        } catch (InterruptedException | ExecutionException e) {
//            throw new RuntimeException("尝试从选定的connId获取topics，获取失败。");
//        } finally {
//            adminClient.close();
//        }
//        if (topicSet.size() == 0){
//            throw new RuntimeException("Kafka connection is not valid, please check kafka connection and topics.");
//        }
//    }

    private Class<? extends Serializer<?>> getSerializer(DataType dataType) {
        // 如果将来有什么特殊类型可以写个isCustom的if逻辑里return
        BaseDataType baseDataType = dataType.getBaseDataType();
        switch (baseDataType) {
            case STRING:
                return StringSerializer.class;
            case BYTES:
                return ByteArraySerializer.class;
            default:
                throw new RuntimeException("Not support the kafka serializer for dataType: " + dataType.getName());
        }
    }

    private Map<String, Object> generateConfig(KafkaKV kv) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getString(KafkaOptionConf.BOOTSTRAP_SERVERS, ""));
        props.put(ProducerConfig.RETRIES_CONFIG, options.getString(KafkaOptionConf.RETRIES_CONFIG, "2"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, options.getString(KafkaOptionConf.BATCH_SIZE_CONFIG, ""));
        props.put(ProducerConfig.LINGER_MS_CONFIG, options.getInteger(KafkaOptionConf.LINGER_MS_CONFIG, 5));
        // props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // 设置类型
        // 由于producer只会被初始化一次，所以这里两个变量也只会被初始化一次
        keyType = kv.getKey().getDataType();
        valueType = kv.getValue().getDataType();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getSerializer(keyType));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getSerializer(valueType));
        return props;
    }

    public void send(KafkaKV kv) {
        // 以懒加载的模式构建producer，毕竟当什么都没传时不知道上游下来什么结构
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    producer = new KafkaProducer<>(generateConfig(kv));
                }
            }
        }
        // 如果kv里类型不同，直接报错，kafka写的时候只允许一种类型的serializer，变来变去像话吗
        if (!kv.getKey().getDataType().equals(keyType) || !kv.getValue().getDataType().equals(valueType)) {
            throw new RuntimeException("Kafka cannot support changing data type, as its serializer is fixed.");
        }
        // 此处get出来的value存在为null的可能，现在没做处理，但是注意一下
        producer.send(new ProducerRecord<>(topic, kv.getKey().getValue(), kv.getValue().getValue()));
    }

    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
