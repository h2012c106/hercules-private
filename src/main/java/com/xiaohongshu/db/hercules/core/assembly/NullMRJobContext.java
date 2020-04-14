package com.xiaohongshu.db.hercules.core.assembly;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public class NullMRJobContext implements MRJobContext {

    public static final MRJobContext INSTANCE=new NullMRJobContext();

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
}
