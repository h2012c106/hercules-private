package com.xiaohongshu.db.hercules.converter.mongo;

import com.xiaohongshu.db.hercules.converter.KvConverterSupplier;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.json.JSONObject;

public class MongoOplogKvConverterSupplier extends KvConverterSupplier<Integer, Integer, JSONObject> {

    public MongoOplogKvConverterSupplier(GenericOptions options) {
        super(new MongoOplogKvConverter(options), new MongoOplogOutputOptionConf(), new MongoOplogInputOptionConf(), new MongoOplogWrapperSetterFactory(), new MongoOplogWrapperGetterFactory());
    }
}
