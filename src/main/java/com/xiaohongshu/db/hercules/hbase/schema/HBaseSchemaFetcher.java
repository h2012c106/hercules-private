package com.xiaohongshu.db.hercules.hbase.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseSchemaFetcher extends BaseSchemaFetcher<HBaseDataTypeConverter> {

    public HBaseSchemaFetcher(GenericOptions options, HBaseDataTypeConverter converter) {
        super(options, converter);
    }

    /**
     *  目前HBase的列名列表依靠用户输入，这个类是为了保证与框架统一
     */
    @Override
    protected List<String> innerGetColumnNameList() {
        return null;
    }

    // HBase 的 ColumnTypeMap 由用户全部指定，不需要从数据库中获取。
    @Override
    protected Map<String, DataType> innerGetColumnTypeMap(Set<String> columnNameSet) {
        return new HashMap<String, DataType>();
    }
}
