package com.xiaohongshu.db.hercules.core.datasource;

import com.xiaohongshu.db.hercules.core.option.OptionsType;

public enum DataSourceRole {
    SOURCE(OptionsType.SOURCE),
    TARGET(OptionsType.TARGET);

    private OptionsType optionsType;

    DataSourceRole(OptionsType optionsType) {
        this.optionsType = optionsType;
    }

    public OptionsType getOptionsType() {
        return optionsType;
    }

    public boolean isSource() {
        return SOURCE.equals(this);
    }

    public boolean isTarget() {
        return TARGET.equals(this);
    }
}
