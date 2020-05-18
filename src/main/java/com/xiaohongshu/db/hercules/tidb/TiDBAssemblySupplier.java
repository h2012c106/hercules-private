package com.xiaohongshu.db.hercules.tidb;

import com.xiaohongshu.db.hercules.core.mr.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;
import com.xiaohongshu.db.hercules.tidb.mr.TiDBInputFormat;
import com.xiaohongshu.db.hercules.tidb.mr.TiDBOutputMRJobContext;

public class TiDBAssemblySupplier extends MysqlAssemblySupplier {
    public TiDBAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return TiDBInputFormat.class;
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new TiDBOutputMRJobContext();
    }
}
