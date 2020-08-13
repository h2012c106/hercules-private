package com.xiaohongshu.db.hercules.core.mr.context;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;

public abstract class BaseMRJobContext implements MRJobContext, DataSourceRoleGetter {

    private final GenericOptions options;
    private final DataSourceRole role;

    public BaseMRJobContext(GenericOptions options) {
        this.options = options;
        this.role = options.getOptionsType().getRole();
    }

    protected final GenericOptions getOptions() {
        return options;
    }

    @Override
    public final DataSourceRole getRole() {
        return role;
    }

}
