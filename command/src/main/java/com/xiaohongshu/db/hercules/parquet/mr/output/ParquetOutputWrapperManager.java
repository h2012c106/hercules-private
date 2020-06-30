package com.xiaohongshu.db.hercules.parquet.mr.output;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetType;
import lombok.NonNull;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

/**
 * 由于要与{@link ParquetRecordWriter}逻辑强解耦，所以不能以内部类的方式存在，所以会有参数初始化以及一些多余的函数
 * 由于给repeated赋值只要无脑add即可，不需要记录下标，所以T为Group就够了
 */
public abstract class ParquetOutputWrapperManager extends WrapperSetterFactory<Group> {

    private Map<String, DataType> columnTypeMap;
    private final ParquetDataTypeConverter converter;

    public ParquetOutputWrapperManager(ParquetDataTypeConverter converter) {
        this.converter = converter;
    }

    public void setColumnTypeMap(Map<String, DataType> columnTypeMap) {
        this.columnTypeMap = columnTypeMap;
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
        DataType columnType = converter.convertElementType(new ParquetType(res.getType().getType(columnName), false));
        WrapperSetter<Group> wrapperSetter = getWrapperSetter(columnType);
        if (wrapper.getType() == BaseDataType.LIST) {
            ListWrapper listWrapper = (ListWrapper) wrapper;
            for (int i = 0; i < listWrapper.size(); ++i) {
                // 在set里都是对某一列无脑add的，不用担心，比读的逻辑简单很多
                wrapperSetter.set(listWrapper.get(i), res, WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST, columnName, -1);
            }
        } else {
            wrapperSetter.set(wrapper, res, WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST, columnName, -1);
        }
    }

    /**
     * {@return}与{@param res}是同一个对象
     * 和Mongo手段不一样，Mongo是你给什么我要什么，Parquet是我要什么你给什么，所以循环的目标一个是MapWrapper，一个是GroupType
     * 对于数据类型也是这个道理，如果从map里取不到，Mongo是你给什么类型我就当什么类型，Parquet是我从自己schema里取
     */
    public Group mapWrapperToGroup(MapWrapper wrapper, Group res, String columnPath) throws Exception {
        List<Type> groupSonTypeList = res.getType().getFields();
        for (Type type : groupSonTypeList) {
            String columnName = type.getName();
            BaseWrapper value = wrapper.get(columnName);
            if (value == null || value.isNull()) {
                // 如果这列上游没有，那就不set，祈祷这列不是required吧
                // 其实这里加了isNull判断，各个setter里的==null就全部木大了，之前脑子秀逗了
                continue;
            }
            String fullColumnName = WritableUtils.concatColumn(columnPath, columnName);
            DataType columnType = columnTypeMap.getOrDefault(fullColumnName, converter.convertElementType(new ParquetType(type)));
            getWrapperSetter(columnType).set(value, res, columnPath, columnName, -1);
        }
        return res;
    }

    @Override
    protected WrapperSetter<Group> getIntegerSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger value = wrapper.asBigInteger();
                // 由于parquet没有null，null的之后直接不置
                if (value != null) {
                    row.add(columnName, value.intValueExact());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getLongSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                BigInteger value = wrapper.asBigInteger();
                // 由于parquet没有null，null的之后直接不置
                if (value != null) {
                    row.add(columnName, value.longValueExact());
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getFloatSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                BigDecimal value = wrapper.asBigDecimal();
                // 由于parquet没有null，null的之后直接不置
                if (value != null) {
                    row.add(columnName, OverflowUtils.numberToFloat(value));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getDoubleSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                BigDecimal value = wrapper.asBigDecimal();
                // 由于parquet没有null，null的之后直接不置
                if (value != null) {
                    row.add(columnName, OverflowUtils.numberToDouble(value));
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getBooleanSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                Boolean value = wrapper.asBoolean();
                // 由于parquet没有null，null的之后直接不置
                if (value != null) {
                    row.add(columnName, value);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getStringSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                String value = wrapper.asString();
                if (value != null) {
                    row.add(columnName, value);
                }
            }
        };
    }

    @Override
    protected WrapperSetter<Group> getBytesSetter() {
        return new WrapperSetter<Group>() {
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
    protected WrapperSetter<Group> getNullSetter() {
        return null;
    }

    @Override
    protected WrapperSetter<Group> getListSetter() {
        return new WrapperSetter<Group>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
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
            public void set(@NonNull BaseWrapper wrapper, Group row, String rowName, String columnName, int columnSeq) throws Exception {
                // 如果是null，就不报错了，不置值
                if (!wrapper.isNull()) {
                    String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                    Group newGroup = row.addGroup(rowName);
                    // 如果上游不是一个Map，直接报错
                    mapWrapperToGroup((MapWrapper) wrapper, newGroup, fullColumnName);
                }
            }
        };
    }
}
