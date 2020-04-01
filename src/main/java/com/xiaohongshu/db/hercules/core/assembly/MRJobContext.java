package com.xiaohongshu.db.hercules.core.assembly;

public interface MRJobContext {
    public void configureInput();

    public void configureOutput();

    public void preRun();

    public void postRun();
}
