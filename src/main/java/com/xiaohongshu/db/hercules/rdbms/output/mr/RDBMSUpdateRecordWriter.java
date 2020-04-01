package com.xiaohongshu.db.hercules.rdbms.output.mr.statement;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.rdbms.output.mr.RDBMSRecordWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class RDBMSUpdateRecordWriter extends RDBMSRecordWriter {
    @Override
    protected PreparedStatement getPreparedStatement(List<HerculesWritable> recordList, Connection connection) throws SQLException {
        return null;
    }
}
