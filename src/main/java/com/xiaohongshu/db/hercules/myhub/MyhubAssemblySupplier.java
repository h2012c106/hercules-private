package com.xiaohongshu.db.hercules.myhub;

import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.myhub.mr.input.MyhubInputFormat;
import com.xiaohongshu.db.hercules.myhub.schema.MyhubSchemaFetcher;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;

public class MyhubAssemblySupplier extends MysqlAssemblySupplier {
    public MyhubAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected BaseSchemaFetcher setSchemaFetcher() {
        return new MyhubSchemaFetcher(options, generateConverter(), generateManager(options));
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return MyhubInputFormat.class;
    }
}
