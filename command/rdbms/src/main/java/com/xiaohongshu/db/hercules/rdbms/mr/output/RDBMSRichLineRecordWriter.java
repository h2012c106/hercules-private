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

/**
 * rich line指一个sql里塞多行数据（也就是好多组?）
 */
public class RDBMSRichLineRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSRichLineRecordWriter.class);

    @SchemaInfo(role = DataSourceRole.TARGET)
    private Schema schema;

    public RDBMSRichLineRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType)
            throws Exception {
        super(context, tableName, exportType);
    }

    @Override
    protected String makeSql(String columnMask, Integer rowNum) {
        return statementGetter.getExportSql(tableName, schema.getColumnNameList(), columnMask, rowNum);
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
        }
        preparedStatement.addBatch();

        return preparedStatement;
    }
}
