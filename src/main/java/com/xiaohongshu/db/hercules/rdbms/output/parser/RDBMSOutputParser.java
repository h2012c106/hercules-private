package com.xiaohongshu.db.hercules.rdbms.output.parser;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.parser.BaseDataSourceParser;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.rdbms.output.ExportType;
import com.xiaohongshu.db.hercules.rdbms.output.options.RDBMSOutputOptionsConf;
import org.apache.commons.lang3.StringUtils;

public class RDBMSOutputParser extends BaseDataSourceParser {
    @Override
    public DataSource getDataSource() {
        return DataSource.RDBMS;
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.TARGET;
    }

    @Override
    protected BaseDataSourceOptionsConf getOptionsConf() {
        return new RDBMSOutputOptionsConf();
    }

    @Override
    protected void validateOptions(GenericOptions options) {
        super.validateOptions(options);

        ParseUtils.validateDependency(options,
                RDBMSOutputOptionsConf.CLOSE_FORCE_INSERT_STAGING,
                null,
                Lists.newArrayList(RDBMSOutputOptionsConf.STAGING_TABLE),
                null);
        ParseUtils.validateDependency(options,
                RDBMSOutputOptionsConf.PRE_MIGRATE_SQL,
                null,
                Lists.newArrayList(RDBMSOutputOptionsConf.STAGING_TABLE),
                null);

        ParseUtils.assertTrue(options.hasProperty(RDBMSOutputOptionsConf.UPDATE_KEY)
                        && ExportType.valueOfIgnoreCase(options.getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null)).isUpdate(),
                "Update key can only used in update mode.");
        ParseUtils.assertTrue(options.getBoolean(RDBMSOutputOptionsConf.BATCH,false)
                        && !ExportType.valueOfIgnoreCase(options.getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null)).isUpdate(),
                "When using batch update, you can only choose the insert-like mode.");
        ParseUtils.assertTrue(options.hasProperty(RDBMSOutputOptionsConf.STAGING_TABLE)
                        && !ExportType.valueOfIgnoreCase(options.getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null)).isUpdate(),
                "When using staging table, you can only choose the insert-like mode.");
        ParseUtils.assertTrue(StringUtils.equalsIgnoreCase(options.getString(RDBMSOutputOptionsConf.STAGING_TABLE, null),
                options.getString(RDBMSOutputOptionsConf.TABLE, null)),
                "Disallowed to set the staging table name equaling the target name.");
        if (options.hasProperty(RDBMSOutputOptionsConf.UPDATE_KEY)) {
            ParseUtils.assertTrue(options.getStringArray(RDBMSOutputOptionsConf.UPDATE_KEY, null).length > 0,
                    "It's meaningless to set a zero-length update key name list.");
            ParseUtils.assertTrue(ExportType.valueOfIgnoreCase(options.getString(RDBMSOutputOptionsConf.EXPORT_TYPE, null)).isUpdate(),
                    "Update key can only used in update mode.");
        }
    }
}
