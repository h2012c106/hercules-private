package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.converter.mongo.MongoOplogWrapperGetterFactory;
import com.xiaohongshu.db.hercules.converter.mongo.MongoOplogWrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.xlog.canal.CanalSerDe;
import com.xiaohongshu.db.xlog.core.codec.Codec;
import com.xiaohongshu.db.xlog.core.exception.SerDeException;

import java.util.Map;

public class TidbCanalEntryKvConverter extends CanalEntryKvConverter{

    public TidbCanalEntryKvConverter(GenericOptions options) {
        super(options);
    }

    @Override
    public byte[] serializeCanalEntry(CanalEntry.Entry entry) {
        try {
            return CanalSerDe.serialize(entry, Codec.CODEC_CANAL_BL02);
        } catch (SerDeException e){
            return null;
        }
    }
}
