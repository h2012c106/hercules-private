package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;

import java.util.List;
import java.util.Map;

import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST;

/**
 * 由于要与{@link ParquetRecordReader}逻辑强解耦，所以不能以内部类的方式存在，所以会有参数初始化以及一些多余的函数
 */
public abstract class ParquetInputWrapperManager extends WrapperGetterFactory<GroupWithSchemaInfo> {

    private Map<String, DataType> columnTypeMap;
    private final ParquetDataTypeConverter converter;

    public ParquetInputWrapperManager(ParquetDataTypeConverter converter) {
        this.converter = converter;
    }

    public void setColumnTypeMap(Map<String, DataType> columnTypeMap) {
        this.columnTypeMap = columnTypeMap;
    }

    /**
     * 注意！由于parquet没有null值，所以一个无值的optional类型有两种语义：null/无值。这个很matter，打个比方，
     * 如果下游为sql，null的话会认为这列上游有值于是置null；而无值的话会置default
     */
    abstract protected boolean emptyAsNull();

    private ListWrapper repeatedToListWrapper(Group group, String columnName) throws Exception {
        int repeatedTime = group.getFieldRepetitionCount(columnName);
        ListWrapper res = new ListWrapper(repeatedTime);
        // 由于类型一定一致，所以可以在循环外取
        DataType columnType = converter.convertElementType(new ParquetType(group.getType().getType(columnName), false));
        WrapperGetter<GroupWithSchemaInfo> wrapperGetter = getWrapperGetter(columnType);
        for (int i = 0; i < repeatedTime; ++i) {
            // 不可能是empty
            res.add(wrapperGetter.get(new GroupWithSchemaInfo(group, i, false), FAKE_PARENT_NAME_USED_BY_LIST, columnName, -1));
        }
        return res;
    }

    public MapWrapper groupToMapWrapper(Group group, String groupPosition) throws Exception {
        List<Type> groupSonTypeList = group.getType().getFields();
        MapWrapper res = new MapWrapper(groupSonTypeList.size());
        for (Type type : groupSonTypeList) {
            String columnName = type.getName();

            boolean isEmpty = !containsColumn(group, columnName);
            // 空且不把空值当null值，不传这一列
            if (isEmpty && !emptyAsNull()) {
                continue;
            }

            String fullColumnName = WritableUtils.concatColumn(groupPosition, columnName);
            DataType columnType = columnTypeMap.getOrDefault(fullColumnName, converter.convertElementType(new ParquetType(type)));
            // 先写一下这里无脑new出来GroupWithRepeatedInfo的理由，以防以后忘了：
            // 由于parquet schema里逻辑LIST与其他类型在同一层定义，所以会有不同（比如一个repeated int32既是一个LIST也是一个INTEGER）
            // 流程逻辑是如果读到其他类型，直接获得函数返回（一层）；如果读到LIST，调用ListGetter调用repeatedToListWrapper二次判断真正类型，然后根据这个类型获得的函数循环（valueSeq）获得每个元素的值（三层）。
            // 所以valueSeq一定是repeatedToListWrapper考虑的事情，这里不需要做判断。
            res.put(columnName, getWrapperGetter(columnType).get(new GroupWithSchemaInfo(group, isEmpty), groupPosition, columnName, -1));
        }
        return res;
    }

    protected final boolean containsColumn(Group group, String columnName) {
        // repeated允许长度为0
        return group.getType().getType(columnName).isRepetition(Type.Repetition.REPEATED)
                || group.getFieldRepetitionCount(columnName) > 0;
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getIntegerGetter() {
        return new ParquetWrapperGetter() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) {
                return new IntegerWrapper(row.getGroup().getInteger(columnName, row.getValueSeq()));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getLongGetter() {
        return new ParquetWrapperGetter() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) {
                return new IntegerWrapper(row.getGroup().getLong(columnName, row.getValueSeq()));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getFloatGetter() {
        return new ParquetWrapperGetter() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) {
                return new DoubleWrapper(row.getGroup().getFloat(columnName, row.getValueSeq()));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getDoubleGetter() {
        return new ParquetWrapperGetter() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) {
                if (!containsColumn(row.getGroup(), columnName)) {
                    return NullWrapper.INSTANCE;
                } else {
                    return new DoubleWrapper(row.getGroup().getDouble(columnName, row.getValueSeq()));
                }
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getBooleanGetter() {
        return new ParquetWrapperGetter() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) {
                return new BooleanWrapper(row.getGroup().getBoolean(columnName, row.getValueSeq()));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getStringGetter() {
        return new ParquetWrapperGetter() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) {
                return new StringWrapper(row.getGroup().getString(columnName, row.getValueSeq()));
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getBytesGetter() {
        return new ParquetWrapperGetter() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) {
                return new BytesWrapper(row.getGroup().getBinary(columnName, row.getValueSeq()).getBytes());
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getNullGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getListGetter() {
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                // 由于逻辑上LIST与其他类型在同一层，所以直接塞，在函数里二次判断真正类型并调用对应的方法
                // list不取值，只查出真正类型后取别的Getter取回的值
                return repeatedToListWrapper(row.getGroup(), columnName);
            }
        };
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getMapGetter() {
        return new ParquetWrapperGetter() {
            @Override
            public BaseWrapper get(GroupWithSchemaInfo row, String rowName, String columnName) throws Exception {
                return groupToMapWrapper(row.getGroup().getGroup(columnName, row.getValueSeq()),
                        WritableUtils.concatColumn(rowName, columnName));
            }
        };
    }
}
