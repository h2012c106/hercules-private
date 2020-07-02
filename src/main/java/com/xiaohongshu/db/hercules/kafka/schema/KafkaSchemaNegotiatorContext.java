package com.xiaohongshu.db.hercules.kafka.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaNegotiatorContext;

public class KafkaSchemaNegotiatorContext extends BaseSchemaNegotiatorContext {
    public KafkaSchemaNegotiatorContext(GenericOptions options) {
        super(options);
    }
}
