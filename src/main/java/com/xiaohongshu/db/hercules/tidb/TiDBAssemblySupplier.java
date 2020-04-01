package com.xiaohongshu.db.hercules.tidb;

import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.MysqlAssemblySupplier;
import com.xiaohongshu.db.hercules.tidb.output.mr.TiDBOutputMRJobContext;

public class TiDBAssemblySupplier extends MysqlAssemblySupplier {
    public TiDBAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new TiDBOutputMRJobContext();
    }
}
