package com.xiaohongshu.db.hercules.rdbms.mr.output;

import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.exception.MapReduceException;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.mr.context.BaseMRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetter;
import com.xiaohongshu.db.hercules.rdbms.mr.output.statement.StatementGetterFactory;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.ResultSetGetter;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RDBMSOutputMRJobContext extends BaseMRJobContext {

    private static final Log LOG = LogFactory.getLog(RDBMSOutputMRJobContext.class);

    @Assembly
    private RDBMSManager manager;

    public RDBMSOutputMRJobContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void configureJob(Job job) {
    }

    @Override
    public void preRun() {
        GenericOptions targetOptions = getOptions();
        if (targetOptions.hasProperty(RDBMSOutputOptionsConf.STAGING_TABLE)) {
            String stagingTable = targetOptions.getString(RDBMSOutputOptionsConf.STAGING_TABLE, null);
            String sql = String.format("SELECT COUNT(1) FROM %s;", stagingTable);
            LOG.info("Execute sql to staging table: " + sql);
            long stagingColumnNum;
            try {
                stagingColumnNum = manager.executeSelect(sql, 1, ResultSetGetter.LONG_GETTER).get(0);
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
    public void postRun() {
        GenericOptions targetOptions = getOptions();
        if (targetOptions.hasProperty(RDBMSOutputOptionsConf.STAGING_TABLE)) {
            String stagingTable = targetOptions.getString(RDBMSOutputOptionsConf.STAGING_TABLE, null);
            Connection connection = null;
            Statement statement = null;
            try {
                connection = manager.getConnection();
                connection.setAutoCommit(true);
                statement = connection.createStatement();

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

                List<String> columnNameList = Arrays.asList(targetOptions.getTrimmedStringArray(TableOptionsConf.COLUMN, null));
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
