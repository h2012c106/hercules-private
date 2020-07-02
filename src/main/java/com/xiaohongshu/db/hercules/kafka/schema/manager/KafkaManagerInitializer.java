package com.xiaohongshu.db.hercules.kafka.schema.manager;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public interface KafkaManagerInitializer {
    KafkaManager initializeManager(GenericOptions options);
}
