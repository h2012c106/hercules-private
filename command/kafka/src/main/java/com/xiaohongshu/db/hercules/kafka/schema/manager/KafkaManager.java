package com.xiaohongshu.db.hercules.kafka.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.kafka.option.KafkaOptionConf;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaManager {

    private final GenericOptions options;
    private final Producer<String, byte[]> producer;
    private final String topic;

    public KafkaManager(GenericOptions options) {
        this.options = options;
        topic = options.getString(KafkaOptionConf.TOPIC, "");
        producer = new KafkaProducer<>(producerConfigs());
    }

    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getString(KafkaOptionConf.BOOTSTRAP_SERVERS, ""));
        props.put(ProducerConfig.RETRIES_CONFIG, options.getString(KafkaOptionConf.RETRIES_CONFIG, ""));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, options.getString(KafkaOptionConf.BATCH_SIZE_CONFIG, ""));
        props.put(ProducerConfig.LINGER_MS_CONFIG, options.getInteger(KafkaOptionConf.LINGER_MS_CONFIG, 5));
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return props;
    }

    public void send(String key, byte[] value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public void close() {
        producer.close();
    }
}
