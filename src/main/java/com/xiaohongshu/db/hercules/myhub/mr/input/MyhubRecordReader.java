package com.xiaohongshu.db.hercules.myhub.mr.input;

import com.xiaohongshu.db.hercules.rdbms.mr.input.RDBMSRecordReader;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MyhubRecordReader extends RDBMSRecordReader {

    private static final Log LOG = LogFactory.getLog(RDBMSRecordReader.class);


    public MyhubRecordReader(TaskAttemptContext context, RDBMSManager manager) {
        super(context, manager);
    }

    @Override
    protected void start(String querySql, Integer fetchSize) throws IOException {
        try {
            connection = manager.getConnection();
            statement = connection.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            LOG.info("Set client cursor.");
            statement.setFetchSize(Integer.MIN_VALUE);
            LOG.info("Executing query: " + querySql);
            resultSet = statement.executeQuery();
        } catch (SQLException e) {
            close();
            throw new IOException(e);
        }
    }

}
