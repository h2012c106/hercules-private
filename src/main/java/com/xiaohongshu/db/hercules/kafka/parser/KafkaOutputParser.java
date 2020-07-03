package com.xiaohongshu.db.hercules.kafka.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.converter.CanalOutputOptionConf;

public class KafkaOutputParser extends BaseParser {

    public KafkaOutputParser() {
        super(new CanalOutputOptionConf(), DataSource.Kafka, DataSourceRole.TARGET);
    }
}
