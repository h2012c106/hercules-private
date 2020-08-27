package com.xiaohongshu.db.hercules.serder.canal.mysql;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xiaohongshu.db.hercules.serder.canal.der.CanalMysqlEntryKVDer;
import com.xiaohongshu.db.xlog.canal.CanalSerDe;
import com.xiaohongshu.db.xlog.core.exception.SerDeException;

import java.util.List;

public class MysqlCanalMysqlEntryKVDer extends CanalMysqlEntryKVDer {
    @Override
    protected List<CanalEntry.Entry> deserializeCanalEntry(byte[] bytes) {
        try {
            return CanalSerDe.deserialize(bytes);
        } catch (SerDeException e) {
            throw new RuntimeException(e);
        }
    }
}
