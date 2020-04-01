package com.xiaohongshu.db.hercules.rdbms.input.parser;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.options.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.parser.BaseDataSourceParser;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.rdbms.common.options.RDBMSOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.input.options.RDBMSInputOptionsConf;

public class RDBMSInputParser extends BaseDataSourceParser {
    @Override
    public DataSource getDataSource() {
        return DataSource.RDBMS;
    }

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.SOURCE;
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
                RDBMSInputOptionsConf.QUERY,
                null,
                Lists.newArrayList(BaseDataSourceOptionsConf.COLUMN),
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
        ParseUtils.assertTrue(splitMaxRow == null || splitMaxRow > 0,
                "Illegal balance split max row num: " + splitMaxRow);

        Integer fetchSize = options.getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null);
        ParseUtils.assertTrue(fetchSize != null && fetchSize > 0,
                "Illegal fetch size value: " + fetchSize);
    }
}
