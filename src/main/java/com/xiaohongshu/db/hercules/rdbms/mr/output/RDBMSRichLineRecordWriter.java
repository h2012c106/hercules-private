package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * rich line指一个sql里塞多行数据（也就是好多组?）
 */
public class RDBMSRichLineRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSRichLineRecordWriter.class);

    public RDBMSRichLineRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType,
                                     RDBMSManager manager, RDBMSWrapperSetterFactory wrapperSetterFactory)
            throws Exception {
        super(context, tableName, exportType, manager, wrapperSetterFactory);
    }

    @Override
    protected String makeSql(String columnMask, Integer rowNum) {
        return statementGetter.getExportSql(tableName, columnNameList, columnMask, rowNum);
    }

    @Override
    protected boolean singleRowPerSql() {
        return false;
    }

    @Override
    protected PreparedStatement getPreparedStatement(RDBMSWorkerMission mission, Connection connection) throws Exception {
        List<HerculesWritable> recordList = mission.getHerculesWritableList();
        String sql = mission.getSql();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Batch export sql is: " + sql);
        }
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        // 因为有很多行问号，所以下标值需要在循环间继承
        int meaningfulSeq = 0;
        for (HerculesWritable record : recordList) {
            // 排去null的下标
            for (int i = 0; i < columnNameList.size(); ++i) {
                String columnName = columnNameList.get(i);
                BaseWrapper columnValue = record.get(columnName);
                // 如果没有这列值，则meaningfulSeq不加
                if (columnValue == null) {
                    continue;
                }
                WrapperSetter<PreparedStatement> setter = getWrapperSetter(columnTypeMap.get(columnName));
                // meaningfulSeq + 1为prepared statement里问号的下标
                setter.set(columnValue, preparedStatement, null, null, ++meaningfulSeq);
            }
        }
        preparedStatement.addBatch();

        return preparedStatement;
    }
}
