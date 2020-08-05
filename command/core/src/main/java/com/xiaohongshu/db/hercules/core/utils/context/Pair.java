package com.xiaohongshu.db.hercules.core.utils.context;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;

public class Pair<T> {

    private T sourceItem;
    private T targetItem;

    public Pair(T sourceItem, T targetItem) {
        this.sourceItem = sourceItem;
        this.targetItem = targetItem;
    }

    public T getSourceItem() {
        return sourceItem;
    }

    public T getTargetItem() {
        return targetItem;
    }

    public T getItem(DataSourceRole dataSourceRole) {
        if (dataSourceRole.isSource()) {
            return sourceItem;
        } else {
            return targetItem;
        }
    }

}
