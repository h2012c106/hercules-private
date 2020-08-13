package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class RDBMSBatchRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSBatchRecordWriter.class);

    @SchemaInfo(role = DataSourceRole.TARGET)
    private Schema schema;

    public RDBMSBatchRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType)
            throws Exception {
        super(context, tableName, exportType);
    }

    @Override
    protected String makeSql(String columnMask, Integer rowNum) {
        return statementGetter.getExportSql(tableName, schema.getColumnNameList(), columnMask, 1);
    }

    @Override
    protected boolean singleRowPerSql() {
        return true;
    }

    @Override
    protected PreparedStatement getPreparedStatement(RDBMSWorkerMission mission, Connection connection)
            throws Exception {
        List<HerculesWritable> recordList = mission.getHerculesWritableList();
        String sql = mission.getSql();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Update sql is: " + sql);
        }
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        for (HerculesWritable record : recordList) {
            // 排去null的下标
            int meaningfulSeq = 0;
            for (int i = 0; i < schema.getColumnNameList().size(); ++i) {
                String columnName = schema.getColumnNameList().get(i);
                BaseWrapper<?> columnValue = record.get(columnName);
                // 如果没有这列值，则meaningfulSeq不加
                if (columnValue == null) {
                    continue;
                }
                WrapperSetter<PreparedStatement> setter = getWrapperSetter(schema.getColumnTypeMap().get(columnName));
                // meaningfulSeq + 1为prepared statement里问号的下标
                setter.set(columnValue, preparedStatement, null, null, ++meaningfulSeq);
            }
            preparedStatement.addBatch();
        }
        return preparedStatement;
    }
}
