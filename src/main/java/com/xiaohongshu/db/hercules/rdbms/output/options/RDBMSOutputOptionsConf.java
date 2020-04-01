package com.xiaohongshu.db.hercules.rdbms.output.options;

import com.xiaohongshu.db.hercules.core.options.SingleOptionConf;
import com.xiaohongshu.db.hercules.rdbms.common.options.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RDBMSOutputOptionsConf extends RDBMSOptionsConf {

    public static final String EXPORT_TYPE = "export-type";
    public static final String UPDATE_KEY = "update-key";

    /**
     * 未经配置应当仅支持向staging table insert，其他操作一概不允许，staging table只是一个源数据的数据库形态，不应有任何改动
     * 如果向staging table做replace等依赖数据库键的操作，必须是staging table的键约束真低于目标表约束，一旦有多余的约束数据极其容易丢，
     * 然而这个东西本工具无法保证，那么仅允许insert staging table是必要的，这样就能变向约束staging表约束低于目标表
     * 另外，update不允许staging table，显而易见
     */
    public static final String STAGING_TABLE = "staging-table";
    public static final String CLOSE_FORCE_INSERT_STAGING = "close-force-insert-staging";
    public static final String PRE_MIGRATE_SQL = "pre-migrate-sql";

    public static final String BATCH = "batch";
    public static final String RECORD_PER_STATEMENT = "record-per-statement";
    public static final String STATEMENT_PER_COMMIT = "statement-per-commit";
    public static final String AUTOCOMMIT = "autocommit";
    public static final String EXECUTE_THREAD_NUM = "execute-thead-num";

    public static final long DEFAULT_RECORD_PER_STATEMENT = 100;
    public static final int DEFAULT_EXECUTE_THREAD_NUM = 1;

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
        tmpList.add(SingleOptionConf.builder()
                .name(TABLE)
                .needArg(true)
                .necessary(true)
                .description("The target table name.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(EXPORT_TYPE)
                .needArg(true)
                .necessary(true)
                .description(String.format("The export type: %s.", Arrays.stream(ExportType.values())
                        .map(Enum::name)
                        .collect(Collectors.joining(" / "))))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(UPDATE_KEY)
                .needArg(true)
                .description(String.format("Determine the update key, only used in update mode, " +
                        "delimited by %s.", COLUMN_DELIMITER))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(BATCH)
                .needArg(false)
                .description("Whether to use batch mode in prepare statement.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(STAGING_TABLE)
                .needArg(true)
                .description("Use staging table to keep target table clean.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(CLOSE_FORCE_INSERT_STAGING)
                .needArg(false)
                .description(String.format("If not specified, to keep data safe, the export method to staging table is always insert. " +
                        "After turning it off, the export method will inherit the '--%s' type, AND IT'S UNSAFE!", EXPORT_TYPE))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(PRE_MIGRATE_SQL)
                .needArg(true)
                .description("The sql executed after staging table filled and before migrate staging table to target table, " +
                        "if the sql like 'truncate ...' executed before and outside hercules, " +
                        "there will be plenty of meaningless time to endure the empty target table.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(RECORD_PER_STATEMENT)
                .needArg(true)
                .description("The record num each statement executed.")
                .defaultStringValue(Long.toString(DEFAULT_RECORD_PER_STATEMENT))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(STATEMENT_PER_COMMIT)
                .needArg(true)
                .description("The statement num each commit committed.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(AUTOCOMMIT)
                .needArg(false)
                .description(String.format("Whether need autocommit. Considering the performance, " +
                        "strongly recommended to close it unless the database does't support commit. " +
                        "This param will neutralize the '--%s' param.", STATEMENT_PER_COMMIT))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(EXECUTE_THREAD_NUM)
                .needArg(true)
                .description("The thread num when executing update statement.")
                .defaultStringValue(Integer.toString(DEFAULT_EXECUTE_THREAD_NUM))
                .build());
        return tmpList;
    }
}
