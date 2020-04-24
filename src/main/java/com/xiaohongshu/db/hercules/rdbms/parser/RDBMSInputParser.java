package com.xiaohongshu.db.hercules.rdbms.parser;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.parser.BaseDataSourceParser;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;

public class RDBMSInputParser extends BaseDataSourceParser {

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.SOURCE;
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.RDBMS;
    }

    @Override
    protected BaseDataSourceOptionsConf getOptionsConf() {
        return new RDBMSInputOptionsConf();
    }

    @Override
    protected void validateOptions(GenericOptions options) {
        super.validateOptions(options);

        ParseUtils.validateDependency(options,
                null,
                null,
                null,
                Lists.newArrayList(RDBMSOptionsConf.TABLE, RDBMSInputOptionsConf.QUERY));
        ParseUtils.validateDependency(options,
                RDBMSInputOptionsConf.CONDITION,
                null,
                Lists.newArrayList(RDBMSOptionsConf.TABLE),
                null);
        ParseUtils.validateDependency(options,
                RDBMSInputOptionsConf.BALANCE_SPLIT_SAMPLE_MAX_ROW,
                null,
                Lists.newArrayList(RDBMSInputOptionsConf.BALANCE_SPLIT),
                null);
        ParseUtils.validateDependency(options,
                RDBMSInputOptionsConf.RANDOM_FUNC_NAME,
                null,
                Lists.newArrayList(RDBMSInputOptionsConf.BALANCE_SPLIT),
                null);

        Integer splitMaxRow = options.getInteger(RDBMSInputOptionsConf.BALANCE_SPLIT_SAMPLE_MAX_ROW,
                null);
        if (splitMaxRow != null) {
            ParseUtils.assertTrue(splitMaxRow > 0,
                    "Illegal balance split max row num: " + splitMaxRow);
        }

        Integer fetchSize = options.getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null);
        if (fetchSize != null) {
            ParseUtils.assertTrue(fetchSize > 0,
                    "Illegal fetch size value: " + fetchSize);
        }

        String splitBy = options.getString(RDBMSInputOptionsConf.SPLIT_BY, null);
        if (splitBy != null) {
            ParseUtils.assertTrue(!splitBy.contains(","),
                    "Unsupported to use multiple split-by key.");
        }
    }
}
