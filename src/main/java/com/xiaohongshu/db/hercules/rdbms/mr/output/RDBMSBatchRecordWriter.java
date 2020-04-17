package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class RDBMSBatchRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSBatchRecordWriter.class);

    public RDBMSBatchRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType, RDBMSSchemaFetcher schemaFetcher)
            throws SQLException, ClassNotFoundException {
        super(context, tableName, exportType, schemaFetcher);
    }

    @Override
    protected String makeSql(String columnMask, Integer rowNum) {
        return statementGetter.getExportSql(tableName, columnNames, columnMask, 1);
    }

    @Override
    protected boolean singleRowPerSql() {
        return true;
    }

    @Override
    protected PreparedStatement getPreparedStatement(WorkerMission mission, Connection connection)
            throws Exception {
        List<HerculesWritable> recordList = mission.getHerculesWritableList();
        String sql = mission.getSql();

        LOG.info("Update sql is: " + sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (HerculesWritable record : recordList) {
            // 排去null的下标
            int meaningfulSeq = 0;
            for (int i = 0; i < columnNames.length; ++i) {
                BaseWrapper columnValue = record.get(columnNames[i]);
                // 如果没有这列值，则meaningfulSeq不加
                if (columnValue == null) {
                    continue;
                }
                WrapperSetter<PreparedStatement> setter = wrapperSetterList.get(i);
                // meaningfulSeq + 1为prepared statement里问号的下标
                setter.set(columnValue, preparedStatement, null, ++meaningfulSeq);
            }
            preparedStatement.addBatch();
        }
        return preparedStatement;
    }
}
