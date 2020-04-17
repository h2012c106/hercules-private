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

/**
 * rich line指一个sql里塞多行数据（也就是好多组?）
 */
public class RDBMSRichLineRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSRichLineRecordWriter.class);

    public RDBMSRichLineRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType, RDBMSSchemaFetcher schemaFetcher)
            throws SQLException, ClassNotFoundException {
        super(context, tableName, exportType, schemaFetcher);
    }

    @Override
    protected String makeSql(String columnMask, Integer rowNum) {
        return statementGetter.getExportSql(tableName, columnNames, columnMask, rowNum);
    }

    @Override
    protected boolean singleRowPerSql() {
        return false;
    }

    @Override
    protected PreparedStatement getPreparedStatement(WorkerMission mission, Connection connection) throws Exception {
        List<HerculesWritable> recordList = mission.getHerculesWritableList();
        String sql = mission.getSql();

        LOG.debug("Batch export sql is: " + sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        // 因为有很多行问号，所以下标值需要在循环间继承
        int meaningfulSeq = 0;
        for (HerculesWritable record : recordList) {
            // 排去null的下标
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
        }
        preparedStatement.addBatch();

        return preparedStatement;
    }
}
