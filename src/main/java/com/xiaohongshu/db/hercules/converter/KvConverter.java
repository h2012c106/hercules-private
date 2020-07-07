package com.xiaohongshu.db.hercules.converter;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;

public abstract class KvConverter {

    public abstract String convertValue(BaseWrapper wrapper);
    public abstract int getColumnType(DataType type);
    public abstract byte[] generateValue(HerculesWritable value, GenericOptions options);
}
