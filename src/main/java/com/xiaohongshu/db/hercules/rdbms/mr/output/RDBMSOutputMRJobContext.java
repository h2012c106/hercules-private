package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.mr.SchemaFetcherGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherFactory;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetterFactory;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.RDBMSSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

public class RDBMSOutputMRJobContext implements MRJobContext, SchemaFetcherGetter<RDBMSSchemaFetcher> {

    private static final Log LOG = LogFactory.getLog(RDBMSOutputMRJobContext.class);

    @Override
    public void configureInput() {

    }

    @Override
    public void configureOutput() {

    }

    @Override
    public void preRun(GenericOptions options) {
        if (options.hasProperty(RDBMSOutputOptionsConf.STAGING_TABLE)) {
            String stagingTable = options.getString(RDBMSOutputOptionsConf.STAGING_TABLE, null);
            RDBMSSchemaFetcher schemaFetcher = getSchemaFetcher(options);
            String sql = String.format("SELECT COUNT(1) FROM %s;", stagingTable);
            LOG.info("Execute sql to staging table: " + sql);
            long stagingColumnNum;
            try {
                stagingColumnNum = schemaFetcher.getManager()
                        .executeSelect(sql, 1, ResultSetGetter.LONG_GETTER).get(0);
            } catch (SQLException e) {
                throw new SchemaException(e);
            }
            if (stagingColumnNum > 0) {
                throw new SchemaException(String.format("There shouldn't be data in staging table [%s], but there are %d rows.",
                        stagingTable, stagingColumnNum));
            }
        }
    }

    protected List<String> splitSql(String sql) {
        return new MySqlStatementParser(sql)
                .parseStatementList()
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());
    }

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

    @Override
    public RDBMSSchemaFetcher getSchemaFetcher(GenericOptions options) {
        return SchemaFetcherFactory.getSchemaFetcher(options, RDBMSSchemaFetcher.class);
    }
}
