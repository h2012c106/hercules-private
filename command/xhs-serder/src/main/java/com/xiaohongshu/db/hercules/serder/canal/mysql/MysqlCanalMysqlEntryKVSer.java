package com.xiaohongshu.db.hercules.serder.canal.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.serder.canal.ser.CanalMysqlEntryKVSer;
import com.xiaohongshu.db.xlog.canal.CanalSerDe;
import com.xiaohongshu.db.xlog.core.codec.Codec;
import com.xiaohongshu.db.xlog.core.exception.SerDeException;

public class MysqlCanalMysqlEntryKVSer extends CanalMysqlEntryKVSer {
    @Override
    protected byte[] serializeCanalEntry(CanalEntry.Entry entry) {
        try {
            return CanalSerDe.serialize(entry, Codec.CODEC_CANAL_BL01);
        } catch (SerDeException e) {
            throw new RuntimeException(e);
        }
    }
}
