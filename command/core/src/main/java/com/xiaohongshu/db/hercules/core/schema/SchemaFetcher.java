package com.xiaohongshu.db.hercules.core.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SchemaFetcher {

    abstract List<String> getColumnNameList();

    abstract Map<String, DataType> getColumnTypeMap();

    abstract List<Set<String>> getIndexGroupList();

    abstract List<Set<String>> getUniqueKeyGroupList();

}
