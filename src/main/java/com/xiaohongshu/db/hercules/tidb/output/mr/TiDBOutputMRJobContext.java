package com.xiaohongshu.db.hercules.tidb.output.mr;

import com.xiaohongshu.db.hercules.core.exceptions.MapReduceException;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.output.mr.MysqlOutputMRJobContext;
import com.xiaohongshu.db.hercules.rdbms.common.options.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;
import com.xiaohongshu.db.hercules.rdbms.output.mr.statement.StatementGetter;
import com.xiaohongshu.db.hercules.rdbms.output.mr.statement.StatementGetterFactory;
import com.xiaohongshu.db.hercules.rdbms.output.options.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TiDBOutputMRJobContext extends MysqlOutputMRJobContext {

    private static final Log LOG = LogFactory.getLog(TiDBOutputMRJobContext.class);

    @Override
    public void postRun(GenericOptions options) {
        if (options.hasProperty(RDBMSOutputOptionsConf.STAGING_TABLE)) {
            String stagingTable = options.getString(RDBMSOutputOptionsConf.STAGING_TABLE, null);
            RDBMSSchemaFetcher schemaFetcher = getSchemaFetcher(options);
            Connection connection = null;
            Statement statement = null;
            try {
                connection = schemaFetcher.getManager().getConnection();
                connection.setAutoCommit(true);
                statement = connection.createStatement();

                statement.execute("set @@tidb_batch_delete=1");
                statement.execute("set @@tidb_batch_insert=1");

                // 执行pre migrate sql
                if (options.hasProperty(RDBMSOutputOptionsConf.PRE_MIGRATE_SQL)) {
                    String preSql = options.getString(RDBMSOutputOptionsConf.PRE_MIGRATE_SQL, null);
                    for (String sql : splitSql(preSql)) {
                        if (sql.length() == 0) {
                            continue;
                        }
                        LOG.info("Execute pre migrate sql: " + sql);
                        statement.execute(sql);
                    }
                }

                // 执行migrate
                String targetTable = options.getString(RDBMSOptionsConf.TABLE, null);
                ExportType exportType = ExportType.valueOfIgnoreCase(options.getString(RDBMSOutputOptionsConf.EXPORT_TYPE,
                        null));
                StatementGetter statementGetter = StatementGetterFactory.get(exportType);

                String migrateSql = statementGetter.getMigrateSql(targetTable,
                        stagingTable,
                        schemaFetcher.getColumnNameList().toArray(new String[0]));
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
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        LOG.warn("SQLException closing statement: " + ExceptionUtils.getStackTrace(e));
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        LOG.warn("SQLException closing connection: " + ExceptionUtils.getStackTrace(e));
                    }
                }
            }
        }
    }
}
