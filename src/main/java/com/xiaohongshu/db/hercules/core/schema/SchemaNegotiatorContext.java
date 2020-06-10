package com.xiaohongshu.db.hercules.core.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;

import java.util.List;
import java.util.Map;

public interface SchemaNegotiatorContext {

    public static final SchemaNegotiatorContext NULL_INSTANCE=new SchemaNegotiatorContext(){

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
        }

    };

    public void afterReadColumnNameList(List<String> columnName);

    public void afterIntersectColumnNameList(List<String> before, List<String> after);

    /**
     * 由于copy一定是之前没有值，故只给一个list
     * @param columnName
     */
    public void afterCopyColumnNameList(List<String> columnName);

    public void afterReadColumnTypeMap(List<String> columnName, Map<String, DataType> columnType);

    public void afterCopyColumnTypeMap(List<String> columnName, Map<String, DataType> before, Map<String, DataType> after);

    public void afterAll(List<String> columnName, Map<String, DataType> columnType);

    public void setSchemaFetcher(BaseSchemaFetcher schemaFetcher);

}
