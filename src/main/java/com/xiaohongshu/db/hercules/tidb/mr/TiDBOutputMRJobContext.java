package com.xiaohongshu.db.hercules.tidb.mr;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.mysql.mr.MysqlOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetterFactory;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

public class TiDBOutputMRJobContext extends MysqlOutputMRJobContext {

    private static final Log LOG = LogFactory.getLog(TiDBOutputMRJobContext.class);

    @Override
    public void postRun(WrappingOptions options) {
        GenericOptions targetOptions = options.getTargetOptions();
        if (targetOptions.hasProperty(RDBMSOutputOptionsConf.STAGING_TABLE)) {
            String stagingTable = targetOptions.getString(RDBMSOutputOptionsConf.STAGING_TABLE, null);
            Connection connection = null;
            Statement statement = null;
            try {
                connection = generateManager(targetOptions).getConnection();
                connection.setAutoCommit(true);
                statement = connection.createStatement();

                statement.execute("set @@tidb_batch_delete=1");
                statement.execute("set @@tidb_batch_insert=1");

                // 执行pre migrate sql
                if (targetOptions.hasProperty(RDBMSOutputOptionsConf.PRE_MIGRATE_SQL)) {
                    String preSql = targetOptions.getString(RDBMSOutputOptionsConf.PRE_MIGRATE_SQL, null);
                    for (String sql : splitSql(preSql)) {
                        if (sql.length() == 0) {
                            continue;
                        }
                        LOG.info("Execute pre migrate sql: " + sql);
                        statement.execute(sql);
                    }
                }

                // 执行migrate
                String targetTable = targetOptions.getString(RDBMSOptionsConf.TABLE, null);
                ExportType exportType = ExportType.valueOfIgnoreCase(targetOptions.getString(RDBMSOutputOptionsConf.EXPORT_TYPE,
                        null));
                StatementGetter statementGetter = StatementGetterFactory.get(exportType);

                List<String> columnNameList = Arrays.asList(targetOptions.getStringArray(BaseDataSourceOptionsConf.COLUMN, null));
                String migrateSql = statementGetter.getMigrateSql(targetTable, stagingTable, columnNameList);
                String deleteSql = String.format("DELETE FROM `%s`", stagingTable);

                LOG.info("Migrate sql: " + migrateSql);
                int updateCount = statement.executeUpdate(migrateSql);
                LOG.info("Migrated " + updateCount + " records from " + stagingTable
                        + " to " + targetTable);

                // Delete the records from the fromTable
                LOG.info("Delete sql: " + deleteSql);
                int deleteCount = statement.executeUpdate(deleteSql);
                LOG.info("Delete " + deleteCount + " records from " + stagingTable);

                statement.execute("set @@tidb_batch_delete=0");
                statement.execute("set @@tidb_batch_insert=0");

                // If the counts do not match, fail the transaction
                if (updateCount != deleteCount) {
                    throw new MapReduceException("Inconsistent record counts.");
                }
            } catch (SQLException e) {
                LOG.error("Something went wrong when execute pre migrate sql");
                throw new MapReduceException(e);
            } finally {
                SqlUtils.release(Lists.newArrayList(statement, connection));
            }
        }
    }
}
