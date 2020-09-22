package com.xiaohongshu.db.hercules.nebula.datatype;

import com.google.common.collect.Lists;
import com.vesoft.nebula.client.graph.ResultSet;
import com.xiaohongshu.db.hercules.core.datatype.BaseCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.nebula.WritingRow;

import java.util.List;

public class NebulaCustomDataTypeManager extends BaseCustomDataTypeManager<ResultSet.Result, WritingRow> {
    @Override
    protected List<Class<? extends CustomDataType<ResultSet.Result, WritingRow, ?>>> generateTypeList() {
        return Lists.newArrayList(VidCustomDataType.class);
    }
}
