package com.xiaohongshu.db.hercules.hbase.mr;

import com.xiaohongshu.db.hercules.core.assembly.MRJobContext;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManager;
import com.xiaohongshu.db.hercules.hbase.schema.manager.HBaseManagerInitializer;

public class HBaseOutputMRJobContext implements MRJobContext, HBaseManagerInitializer {

    @Override
    public void configureInput() {

    }

    @Override
    public void configureOutput() {

    }

    @Override
    public void preRun(GenericOptions options) {
        
    }

    @Override
    public void postRun(GenericOptions options) {

    }

    @Override
    public HBaseManager initializeManager(GenericOptions options) {
        return new HBaseManager(options);
    }
}
