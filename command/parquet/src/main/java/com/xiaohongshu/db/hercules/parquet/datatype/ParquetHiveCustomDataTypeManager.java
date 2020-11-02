package com.xiaohongshu.db.hercules.parquet.datatype;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.parquet.mr.input.GroupWithSchemaInfo;
import org.apache.parquet.example.data.Group;

import java.util.List;

public class ParquetHiveCustomDataTypeManager extends BaseCustomDataTypeManager<GroupWithSchemaInfo, Group> {
    @Override
    protected List<Class<? extends CustomDataType<GroupWithSchemaInfo, Group, ?>>> generateTypeList() {
        return Lists.newArrayList(
                ParquetHiveMapCustomDataType.class,
                ParquetHiveListCustomDataType.class
        );
    }
}
