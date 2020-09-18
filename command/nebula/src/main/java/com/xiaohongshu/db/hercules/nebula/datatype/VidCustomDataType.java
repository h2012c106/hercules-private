package com.xiaohongshu.db.hercules.nebula.datatype;

import com.vesoft.nebula.client.graph.ResultSet;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.DoubleWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.StringWrapper;
import com.xiaohongshu.db.hercules.nebula.WritingRow;

import java.util.function.Function;

public class VidCustomDataType extends CustomDataType<ResultSet.Result, WritingRow, Long> {

    public static final VidCustomDataType INSTANCE = new VidCustomDataType();

    protected VidCustomDataType() {
        super(
                "VID",
                BaseDataType.LONG,
                Long.class,
                new Function<Object, BaseWrapper<?>>() {
                    @Override
                    public BaseWrapper<?> apply(Object o) {
                        return new VidWrapper((Long) o);
                    }
                }
        );
    }

    @Override
    protected BaseTypeWrapperGetter<Long, ResultSet.Result> createWrapperGetter(CustomDataType<ResultSet.Result, WritingRow, Long> self) {
        return null;
    }

    @Override
    protected BaseTypeWrapperSetter<Long, WritingRow> createWrapperSetter(CustomDataType<ResultSet.Result, WritingRow, Long> self) {
        return new BaseTypeWrapperSetter<Long, WritingRow>() {
            @Override
            protected DataType getType() {
                return self;
            }

            @Override
            protected void setNonnullValue(Long value, WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                row.putKey(columnName, value);
            }

            @Override
            protected void setNull(WritingRow row, String rowName, String columnName, int columnSeq) throws Exception {
                // head肯定不能是null，之后拼sql的时候会抛错的
                row.putKey(columnName, null);
            }
        };
    }

    @Override
    protected Long innerWrite(IntegerWrapper wrapper) throws Exception {
        return wrapper.asLong();
    }

    @Override
    protected Long innerWrite(DoubleWrapper wrapper) throws Exception {
        return wrapper.asLong();
    }

    @Override
    protected Long innerWrite(StringWrapper wrapper) throws Exception {
        return wrapper.asLong();
    }

    @Override
    public Class<?> getJavaClass() {
        return Long.class;
    }
}
