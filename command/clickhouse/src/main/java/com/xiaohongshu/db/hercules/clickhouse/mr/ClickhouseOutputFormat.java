package com.xiaohongshu.db.hercules.clickhouse.mr;

import com.xiaohongshu.db.hercules.clickhouse.option.ClickhouseOutputOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSOutputFormat;
import com.xiaohongshu.db.hercules.rdbms.mr.output.RDBMSWrapperSetterFactory;

public class ClickhouseOutputFormat extends RDBMSOutputFormat {

    @Options(type = OptionsType.TARGET)
    private GenericOptions targetOptions;

    @Override
    protected RDBMSWrapperSetterFactory createWrapperSetterFactory() {
        boolean enableNull = targetOptions.getBoolean(ClickhouseOutputOptionsConf.ENABLE_NULL, false);
        return new ClickhouseWrapperSetterFactory(enableNull);
    }

}
