package com.xiaohongshu.db.hercules.core.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;

import java.util.List;
import java.util.Map;

public class BaseSchemaNegotiatorContext implements SchemaNegotiatorContext {

    private GenericOptions options;
    private BaseSchemaFetcher schemaFetcher;

    public BaseSchemaNegotiatorContext(GenericOptions options) {
        this.options = options;
        this.schemaFetcher = null;
    }

    protected GenericOptions getOptions() {
        return options;
    }

    protected BaseSchemaFetcher getSchemaFetcher() {
        return schemaFetcher;
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
    public void afterAll(List<String> columnName, Map<String, DataType> columnType) {
    }

    @Override
    public void setSchemaFetcher(BaseSchemaFetcher schemaFetcher) {
        this.schemaFetcher = schemaFetcher;
    }

}
