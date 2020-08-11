package com.xiaohongshu.db.hercules.converter.tidb;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.converter.mysql.CanalEntryKvSerDer;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.xlog.canal.CanalSerDe;
import com.xiaohongshu.db.xlog.core.codec.Codec;
import com.xiaohongshu.db.xlog.core.exception.SerDeException;

public class TidbCanalEntryKvSerDer extends CanalEntryKvSerDer {

    public TidbCanalEntryKvSerDer(GenericOptions options) {
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
