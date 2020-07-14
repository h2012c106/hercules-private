package com.xiaohongshu.db.hercules.converter.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.xlog.canal.CanalSerDe;
import com.xiaohongshu.db.xlog.core.codec.Codec;
import com.xiaohongshu.db.xlog.core.exception.SerDeException;

public class TidbCanalEntryKvConverter extends CanalEntryKvConverter{
    @Override
    public byte[] serializeCanalEntry(CanalEntry.Entry entry) {
        try {
            return CanalSerDe.serialize(entry, Codec.CODEC_CANAL_BL02);
        } catch (SerDeException e){
            return null;
        }
    }
}
