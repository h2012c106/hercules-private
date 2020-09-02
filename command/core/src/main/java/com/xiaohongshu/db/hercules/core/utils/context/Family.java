package com.xiaohongshu.db.hercules.core.utils.context;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;

public class Family<T> {

    private T sourceItem = null;
    private T targetItem = null;
    private T derItem = null;
    private T serItem = null;

    public static <T> Family<T> initialize(T sourceItem, T targetItem, T derItem, T serItem) {
        Family<T> res = new Family<>();
        res.sourceItem = sourceItem;
        res.targetItem = targetItem;
        res.derItem = derItem;
        res.serItem = serItem;
        return res;
    }

    public static <T> Family<T> initializeDataSource(T sourceItem, T targetItem) {
        Family<T> res = new Family<>();
        res.sourceItem = sourceItem;
        res.targetItem = targetItem;
        return res;
    }

    public static <T> Family<T> initializeSerDer(T derItem, T serItem) {
        Family<T> res = new Family<>();
        res.derItem = derItem;
        res.serItem = serItem;
        return res;
    }

    private Family() {
    }

    public T getSourceItem() {
        return sourceItem;
    }

    public T getTargetItem() {
        return targetItem;
    }

    public T getDerItem() {
        return derItem;
    }

    public T getSerItem() {
        return serItem;
    }

    public T getDataSourceItem(DataSourceRole dataSourceRole) {
        switch (dataSourceRole) {
            case SOURCE:
                return sourceItem;
            case TARGET:
                return targetItem;
            default:
                throw new RuntimeException("Unknown datasource role: " + dataSourceRole);
        }
    }

    public T getSerDerItem(DataSourceRole dataSourceRole) {
        switch (dataSourceRole) {
            case DER:
                return derItem;
            case SER:
                return serItem;
            default:
                throw new RuntimeException("Unknown serder role: " + dataSourceRole);
        }
    }

    public T getItem(DataSourceRole dataSourceRole) {
        switch (dataSourceRole) {
            case SOURCE:
                return sourceItem;
            case TARGET:
                return targetItem;
            case DER:
                return derItem;
            case SER:
                return serItem;
            default:
                throw new RuntimeException("Illegal role: " + dataSourceRole);
        }
    }

}
