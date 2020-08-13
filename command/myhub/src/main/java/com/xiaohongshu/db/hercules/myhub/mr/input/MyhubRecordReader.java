package com.xiaohongshu.db.hercules.myhub.mr.input;

import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSRecordReader;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.SQLException;

public class MyhubRecordReader extends RDBMSRecordReader {

    private static final Log LOG = LogFactory.getLog(RDBMSRecordReader.class);

    @GeneralAssembly
    private RDBMSManager manager;

    public MyhubRecordReader(TaskAttemptContext context) {
        super(context);
    }

    @Override
    protected void start(String querySql, Integer fetchSize) throws IOException {
        try {
            connection = manager.getConnection();
            statement = SqlUtils.makeReadStatement(connection, querySql);
            LOG.info("Executing query: " + querySql);
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            close();
            throw new IOException(e);
        }
    }

}
