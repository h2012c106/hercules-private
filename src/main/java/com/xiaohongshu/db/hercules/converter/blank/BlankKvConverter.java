package com.xiaohongshu.db.hercules.converter.blank;

import com.xiaohongshu.db.hercules.converter.KvConverter;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;

import java.util.List;
import java.util.Map;


public class BlankKvConverter extends KvConverter {

    public BlankKvConverter() {
        super(null, null, null);
    }

    @Override
    public byte[] generateValue(HerculesWritable value, GenericOptions options, Map columnTypeMap, List columnNameList) {
        return new byte[0];
    }

    @Override
    public HerculesWritable generateHerculesWritable(byte[] data, GenericOptions options) {
        return null;
    }
}
