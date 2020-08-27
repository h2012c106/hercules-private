package com.xiaohongshu.db.hercules.redis.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;

import java.util.List;
import java.util.Map;

/**
 * Created by jamesqq on 2020/8/15.
 */
public class RedisSchemaFetcher extends BaseSchemaFetcher {

    public RedisSchemaFetcher(GenericOptions options) {
        super(options);
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        return null;
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap() {
        return null;
    }
}
