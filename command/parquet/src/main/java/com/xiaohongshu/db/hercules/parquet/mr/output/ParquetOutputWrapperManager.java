package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetType;
import lombok.NonNull;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;

import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST;

/**
 * 由于要与{@link ParquetRecordWriter}逻辑强解耦，所以不能以内部类的方式存在，所以会有参数初始化以及一些多余的函数
 * 由于给repeated赋值只要无脑add即可，不需要记录下标，所以T为Group就够了
 */
public abstract class ParquetOutputWrapperManager extends WrapperSetterFactory<Group> {

    @Assembly
    private final ParquetDataTypeConverter dataTypeConverter = null;

    public ParquetOutputWrapperManager() {
        super(DataSourceRole.TARGET);
    }

    /**
     * 当上游不是list时，做一个singleton list
     *
     * @param wrapper
     * @param res
     * @param columnName
     * @throws Exception
     */
    private void wrapperToRepeated(BaseWrapper wrapper, Group res, String columnName) throws Exception {
        // 由于类型一定一致，所以可以事先取
        DataType columnType = dataTypeConverter.convertElementType(new ParquetType(res.getType().getType(columnName), false));
        WrapperSetter<Group> wrapperSetter = getWrapperSetter(columnType);
        if (wrapper.getType() == BaseDataType.LIST) {
            ListWrapper listWrapper = (ListWrapper) wrapper;
            for (int i = 0; i < listWrapper.size(); ++i) {
                // 在set里都是对某一列无脑add的，不用担心，比读的逻辑简单很多
                wrapperSetter.set(listWrapper.get(i), res, FAKE_PARENT_NAME_USED_BY_LIST, columnName, -1);
            }
        } else {
            wrapperSetter.set(wrapper, res, FAKE_PARENT_NAME_USED_BY_LIST, columnName, -1);
        }
    }

//    /**
//     * {@return}与{@param res}是同一个对象
//     * 和Mongo手段不一样，Mongo是你给什么我要什么，Parquet是我要什么你给什么，所以循环的目标一个是MapWrapper，一个是GroupType
//     * 对于数据类型也是这个道理，如果从map里取不到，Mongo是你给什么类型我就当什么类型，Parquet是我从自己schema里取
//     */
//    public Group mapWrapperToGroup(MapWrapper wrapper, Group res, String columnPath) throws Exception {
//        List<Type> groupSonTypeList = res.getType().getFields();
//        for (Type type : groupSonTypeList) {
//            String columnName = type.getName();
//            BaseWrapper value = wrapper.get(columnName);
//            if (value == null || value.isNull()) {
//                // 如果这列上游没有，那就不set，祈祷这列不是required吧
//                // 其实这里加了isNull判断，各个setter里的==null就全部木大了，之前脑子秀逗了
//                continue;
//            }
//            String fullColumnName = WritableUtils.concatColumn(columnPath, columnName);
//            DataType columnType = columnTypeMap.getOrDefault(fullColumnName, converter.convertElementType(new ParquetType(type)));
//            getWrapperSetter(columnType).set(value, res, columnPath, columnName, -1);
//        }
//        return res;
//    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<Group> getIntegerSetter() {
        return new BaseTypeWrapperSetter.IntegerSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Integer value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<Group> getLongSetter() {
        return new BaseTypeWrapperSetter.LongSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Long value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<Group> getFloatSetter() {
        return new BaseTypeWrapperSetter.FloatSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Float value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<Group> getDoubleSetter() {
        return new BaseTypeWrapperSetter.DoubleSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Double value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<Group> getBooleanSetter() {
        return new BaseTypeWrapperSetter.BooleanSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Boolean value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<Group> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(String value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, value);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<Group> getBytesSetter() {
        return new BaseTypeWrapperSetter.BytesSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(byte[] value, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                row.add(columnName, Binary.fromConstantByteArray(value));
            }

            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                byte[] value = wrapper.asBytes();
                if (value != null) {
                    row.add(columnName, Binary.fromConstantByteArray(value));
                }
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<Group> getNullSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<Group> getListSetter() {
        return new WrapperSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnull(@NonNull BaseWrapper<?> wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                // 不用对null做判断，在委托其他setter构建singleton list的时候如果为null，他们自然不会塞值
                // List不自己塞值，它找到真正类型后继续委托其他Setter塞值
                wrapperToRepeated(wrapper, row, columnName);
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getMapSetter() {
        return new WrapperSetter<Group>() {
            @Override
            protected void setNull(Group row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnull(@NonNull BaseWrapper<?> wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                Group newGroup = row.addGroup(rowName);
                // 如果上游不是一个Map，直接报错
                writeMapWrapper((MapWrapper) wrapper, newGroup, fullColumnName);
            }
        };
    }
}
