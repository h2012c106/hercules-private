package com.xiaohongshu.db.hercules.core.schema;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BaseSchemaNegotiatorContext implements SchemaNegotiatorContext, DataSourceRoleGetter {

    private final GenericOptions options;
    private final DataSourceRole role;

    public BaseSchemaNegotiatorContext(GenericOptions options) {
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

    @Override
    public void afterReadColumnNameList(List<String> columnName) {
    }

    @Override
    public void afterIntersectColumnNameList(List<String> before, List<String> after) {
    }

    @Override
    public void afterCopyColumnNameList(List<String> columnName) {
    }

    @Override
    public void afterReadColumnTypeMap(List<String> columnName, Map<String, DataType> columnType) {
    }

    @Override
    public void afterCopyColumnTypeMap(List<String> columnName, Map<String, DataType> before, Map<String, DataType> after) {
    }

    @Override
    public void afterReadIndexGroupList(List<Set<String>> indexGroupList) {
    }

    @Override
    public void afterReadUniqueKeyGroupList(List<Set<String>> uniqueKeyGroupList) {
    }

    @Override
    public void afterAll(List<String> columnName, Map<String, DataType> columnType) {
    }

}
