package com.xiaohongshu.db.hercules.converter.blank;

import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;


public class BlankKvConverter extends KvConverter {

    @Override
    public String convertValue(BaseWrapper wrapper) {
        return null;
    }

    @Override
    public int getColumnType(DataType type) {
        return 0;
    }

    @Override
    public byte[] generateValue(HerculesWritable value, GenericOptions options) {
        return null;
    }
}
