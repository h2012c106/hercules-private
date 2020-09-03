package com.xiaohongshu.db.hercules.redis.mr;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;


public abstract class MapWrapperSetter<T> extends WrapperSetter<T> {

    protected final DataType getType() {
        return BaseDataType.MAP;
    }

//    abstract protected void setNonnullValue(BaseWrapper value, T row, String rowName, String columnName, int columnSeq) throws Exception;
//
//    @Override
//    protected void setNonnull(@NonNull BaseWrapper<?> wrapper, T row, String rowName, String columnName, int columnSeq) throws Exception {
//        setNonnullValue(wrapper, row, rowName, columnName, columnSeq);
//    }

}
