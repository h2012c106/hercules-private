package com.xiaohongshu.db.hercules.core.assembly;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public interface MRJobContext {
    public void configureInput();

    public void configureOutput();

    public void preRun(GenericOptions options);

    public void postRun(GenericOptions options);
}
