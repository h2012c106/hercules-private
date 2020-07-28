package com.xiaohongshu.db.hercules.kafka;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;

public class KafkaDataSource implements DataSource {
    @Override
    public String name() {
        return "Kafka";
    }

    @Override
    public String getFilePositionParam() {
        return null;
    }
}
