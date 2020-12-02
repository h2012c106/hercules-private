package com.xiaohongshu.db.hercules.myhub.mr.output;

import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSRichLineRecordWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class MyhubRecordWriter extends RDBMSRichLineRecordWriter {

    private static final Log LOG = LogFactory.getLog(MyhubRecordWriter.class);

    public MyhubRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType) throws Exception {
        super(context, tableName, exportType);
    }

    @Override
    protected RDBMSMultiThreadAsyncWriter generateWriter(int threadNum) {
        return new MyhubMultiThreadAsyncWriter(threadNum);
    }

    private class MyhubMultiThreadAsyncWriter extends RDBMSMultiThreadAsyncWriter {
        public MyhubMultiThreadAsyncWriter(int threadNum) {
            super(threadNum);
        }

        @Override
        protected void doWrite(ThreadContext context, RDBMSWorkerMission mission) throws Exception {
            if (mission.getHerculesWritableList() != null && mission.getHerculesWritableList().size() > 0) {
                context.addRecordNum(mission.getHerculesWritableList().size());
                PreparedStatement preparedStatement = null;
                Statement statement = null;
                try {
                    preparedStatement = getPreparedStatement(mission, context.getConnection());
                    String preparedSql = ((com.mysql.jdbc.PreparedStatement) preparedStatement).asSql();
                    statement = preparedStatement.getConnection().createStatement();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Executing update sql: " + preparedSql);
                    }
                    statement.execute(preparedSql);
                    context.increaseTmpStatementPerCommit();
                    context.increaseExecuteNum();
                } finally {
                    if (preparedStatement != null) {
                        try {
                            preparedStatement.close();
                        } catch (SQLException ignore) {
                        }
                    }
                    if (statement != null) {
                        try {
                            statement.close();
                        } catch (SQLException ignore) {
                        }
                    }
                }
            }

            if (mission.needClose()
                    || (!unlimitedStatementPerCommit && context.getTmpStatementPerCommit() >= statementPerCommit)) {
                transactionManager.commit(context.getConnection());
                context.setTmpStatementPerCommit(0);
                context.increaseCommitNum();
            }
        }
    }
}
