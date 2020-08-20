package com.xiaohongshu.db.hercules.rdbms.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.rdbms.ExportType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseDataSourceOptionsConf.COLUMN_DELIMITER;
import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf.TABLE;

public final class RDBMSOutputOptionsConf extends BaseOptionsConf {

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
    public static final String EXECUTE_THREAD_NUM = "execute-thread-num";

    public static final long DEFAULT_RECORD_PER_STATEMENT = 100;
    public static final int DEFAULT_EXECUTE_THREAD_NUM = 1;

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new BaseOutputOptionsConf(),
                new RDBMSOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
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
                        "delimited by %s. Must exist in source data.", COLUMN_DELIMITER))
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
                .description("The record num each statement executes.")
                .defaultStringValue(Long.toString(DEFAULT_RECORD_PER_STATEMENT))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(STATEMENT_PER_COMMIT)
                .needArg(true)
                .description("The statement num each commit commits.")
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

    @Override
    public void innerValidateOptions(GenericOptions options) {
        ParseUtils.validateDependency(options,
                PRE_MIGRATE_SQL,
                null,
                Lists.newArrayList(STAGING_TABLE),
                null);

        if (options.hasProperty(UPDATE_KEY)) {
            ParseUtils.assertTrue(ExportType.valueOfIgnoreCase(options.getString(EXPORT_TYPE, null)).isUpdate(),
                    "Update key can only used in update mode.");
        }
        if (options.getBoolean(BATCH, false)) {
            ParseUtils.assertTrue(!ExportType.valueOfIgnoreCase(options.getString(EXPORT_TYPE, null)).isUpdate(),
                    "When using batch update, you can only choose the insert-like mode.");
        }
        if (options.hasProperty(STAGING_TABLE)) {
            ParseUtils.assertTrue(!ExportType.valueOfIgnoreCase(options.getString(EXPORT_TYPE, null)).isUpdate(),
                    "When using staging table, you can only choose the insert-like mode.");
        }
        ParseUtils.assertTrue(!StringUtils.equalsIgnoreCase(options.getString(STAGING_TABLE, null),
                options.getString(TABLE, null)),
                "Disallowed to set the staging table name equaling the target name.");
        if (options.hasProperty(UPDATE_KEY)) {
            ParseUtils.assertTrue(options.getTrimmedStringArray(UPDATE_KEY, null).length > 0,
                    "It's meaningless to set a zero-length update key name list.");
            ParseUtils.assertTrue(ExportType.valueOfIgnoreCase(options.getString(EXPORT_TYPE, null)).isUpdate(),
                    "Update key can only used in update mode.");
        }
        ParseUtils.assertTrue(options.getLong(RECORD_PER_STATEMENT, DEFAULT_RECORD_PER_STATEMENT) > 0,
                "The record num per statement should > 0.");
        ParseUtils.assertTrue(options.getInteger(STATEMENT_PER_COMMIT, 1) > 0,
                "The statement num per commit should > 0.");
        ParseUtils.assertTrue(options.getInteger(EXECUTE_THREAD_NUM, DEFAULT_EXECUTE_THREAD_NUM) > 0,
                "The thread num should > 0.");
    }
}
