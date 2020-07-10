package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

public class RDBMSUpdateRecordWriter extends RDBMSRecordWriter {

    private static final Log LOG = LogFactory.getLog(RDBMSUpdateRecordWriter.class);

    private List<String> updateKeyList;

    public RDBMSUpdateRecordWriter(TaskAttemptContext context, String tableName, ExportType exportType,
                                   RDBMSManager manager, RDBMSWrapperSetterFactory wrapperSetterFactory)
            throws Exception {
        super(context, tableName, exportType, manager, wrapperSetterFactory);

        updateKeyList = Arrays.asList(options.getTargetOptions().getStringArray(RDBMSOutputOptionsConf.UPDATE_KEY, null));
    }

    @Override
    protected String makeSql(String columnMask, Integer rowNum) {
        return statementGetter.getExportSql(tableName, columnNameList, columnMask, updateKeyList);
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
            for (int i = 0; i < updateKeyList.size(); ++i) {
                String columnName = updateKeyList.get(i);
                BaseWrapper columnValue = record.get(columnName);
                // record内必须要有update的列值
                if (columnValue == null) {
                    throw new MapReduceException(String.format("The update key [%s] should be the subset of source data source columns.", columnName));
                }
                // meaningfulSeq得续上
                getWrapperSetter(columnTypeMap.get(columnName)).set(columnValue, preparedStatement, null, null, ++meaningfulSeq);
            }
            preparedStatement.addBatch();
        }
        return preparedStatement;
    }
}
