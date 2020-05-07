package com.xiaohongshu.db.hercules.mysql;

import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.mr.input.HerculesInputFormat;
import com.xiaohongshu.db.hercules.core.mr.output.HerculesOutputFormat;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.mr.MysqlInputFormat;
import com.xiaohongshu.db.hercules.mysql.mr.MysqlOutputFormat;
import com.xiaohongshu.db.hercules.mysql.mr.MysqlOutputMRJobContext;
import com.xiaohongshu.db.hercules.mysql.schema.manager.MysqlManager;
import com.xiaohongshu.db.hercules.rdbms.RDBMSAssemblySupplier;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;

public class MysqlAssemblySupplier extends RDBMSAssemblySupplier {
    public MysqlAssemblySupplier(GenericOptions options) {
        super(options);
    }

    @Override
    protected Class<? extends HerculesInputFormat> setInputFormatClass() {
        return MysqlInputFormat.class;
    }

    @Override
    protected Class<? extends HerculesOutputFormat> setOutputFormatClass() {
        return MysqlOutputFormat.class;
    }

    @Override
    protected MRJobContext setJobContextAsTarget() {
        return new MysqlOutputMRJobContext();
    }

    @Override
    public RDBMSManager initializeManager(GenericOptions options) {
        return new MysqlManager(options);
    }
}
