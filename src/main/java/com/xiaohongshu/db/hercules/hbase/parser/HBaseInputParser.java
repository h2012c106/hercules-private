package com.xiaohongshu.db.hercules.hbase.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.parser.BaseDataSourceParser;
import com.xiaohongshu.db.hercules.hbase2.option.HbaseInputOptionsConf;

public class HBaseInputParser extends BaseDataSourceParser {

    @Override
    public DataSourceRole getDataSourceRole() {
        return DataSourceRole.SOURCE;
    }

    @Override
    public DataSource getDataSource() {
        return DataSource.HBase;
    }

    @Override
    protected BaseDataSourceOptionsConf getOptionsConf() {
        return new HbaseInputOptionsConf();
    }

    @Override
    protected void validateOptions(GenericOptions options) {
        super.validateOptions(options);
        // TODO add some dependency check, some params are required
        
    }
}
