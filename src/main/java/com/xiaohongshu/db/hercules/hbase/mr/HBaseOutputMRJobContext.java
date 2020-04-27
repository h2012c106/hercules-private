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

    /**
     * 对于HBase来讲，要确保上游和下游 rowKeyCol 正确设置
     * 如果上游不是NoSQL，则要确保 rowKeyCol 在 columnNameList 中要存在，否则抛错
     * @param options
     */
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
