package com.xiaohongshu.db.hercules.rdbms.output.mr;

import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.WrapperSetter;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class RDBMSRichLineRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSRichLineRecordWriter.class);

    public RDBMSRichLineRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType, RDBMSSchemaFetcher schemaFetcher)
            throws SQLException, ClassNotFoundException {
        super(context, tableName, exportType, schemaFetcher);
    }

    @Override
    protected PreparedStatement getPreparedStatement(List<HerculesWritable> recordList, Connection connection) throws Exception {
        String sql = statementGetter.getExportSql(tableName, columnNames, recordList.size());
        LOG.debug("Batch export sql is: " + sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        // 因为有很多行问号，所以下标值需要在循环间继承
        int meaningfulSeq = 0;
        for (HerculesWritable record : recordList) {
            // 排去null的下标
            for (int i = 0; i < columnNames.length; ++i) {
                String columnName = columnNames[i];

                if (columnName == null) {
                    continue;
                }

                // 源数据源中该列的下标，即HerculesWritable中的下标
                int sourceSeq = targetSourceColumnSeq.get(i);
                WrapperSetter<PreparedStatement> setter = wrapperSetterList.get(i);
                // meaningfulSeq + 1为prepared statement里问号的下标
                setter.set(record.get(sourceSeq), preparedStatement, null, ++meaningfulSeq);
            }
        }
        preparedStatement.addBatch();

        return preparedStatement;
    }
}
