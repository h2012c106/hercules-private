package com.xiaohongshu.db.hercules.parquet.mr.input;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;

import java.util.List;

import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST;

/**
 * 由于要与{@link ParquetRecordReader}逻辑强解耦，所以不能以内部类的方式存在，所以会有参数初始化以及一些多余的函数
 */
public abstract class ParquetInputWrapperManager extends WrapperGetterFactory<GroupWithSchemaInfo> {

    @SchemaInfo
    private Schema schema;

    @Assembly
    private final ParquetDataTypeConverter dataTypeConverter = null;

    public ParquetInputWrapperManager() {
        super(DataSourceRole.SOURCE);
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
        DataType columnType = dataTypeConverter.convertElementType(new ParquetType(group.getType().getType(columnName), false));
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
            DataType columnType = schema.getColumnTypeMap().get(fullColumnName);
            if (columnType == null) {
                columnType = dataTypeConverter.convertElementType(new ParquetType(type));
            }
            // 先写一下这里无脑new出来GroupWithRepeatedInfo的理由，以防以后忘了：
            // 由于parquet schema里逻辑LIST与其他类型在同一层定义，所以会有不同（比如一个repeated int32既是一个LIST也是一个INTEGER）
            // 流程逻辑是如果读到其他类型，直接获得函数返回（一层）；如果读到LIST，调用ListGetter调用repeatedToListWrapper二次判断真正类型，然后根据这个类型获得的函数循环（valueSeq）获得每个元素的值（三层）。
            // 所以valueSeq一定是repeatedToListWrapper考虑的事情，这里不需要做判断。
            res.put(columnName, getWrapperGetter(columnType).get(new GroupWithSchemaInfo(group, isEmpty), groupPosition, columnName, -1));
        }
        return res;
    }

    private boolean containsColumn(Group group, String columnName) {
        // repeated允许长度为0
        return group.getType().getType(columnName).isRepetition(Type.Repetition.REPEATED)
                || group.getFieldRepetitionCount(columnName) > 0;
    }

    @Override
    protected BaseTypeWrapperGetter.IntegerGetter<GroupWithSchemaInfo> getIntegerGetter() {
        return new BaseTypeWrapperGetter.IntegerGetter<GroupWithSchemaInfo>() {
            @Override
            protected Integer getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getGroup().getInteger(columnName, row.getValueSeq());
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.LongGetter<GroupWithSchemaInfo> getLongGetter() {
        return new BaseTypeWrapperGetter.LongGetter<GroupWithSchemaInfo>() {
            @Override
            protected Long getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getGroup().getLong(columnName, row.getValueSeq());
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.FloatGetter<GroupWithSchemaInfo> getFloatGetter() {
        return new BaseTypeWrapperGetter.FloatGetter<GroupWithSchemaInfo>() {
            @Override
            protected Float getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getGroup().getFloat(columnName, row.getValueSeq());
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty() || Float.isNaN(row.getGroup().getFloat(columnName, row.getValueSeq()));
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.DoubleGetter<GroupWithSchemaInfo> getDoubleGetter() {
        return new BaseTypeWrapperGetter.DoubleGetter<GroupWithSchemaInfo>() {
            @Override
            protected Double getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getGroup().getDouble(columnName, row.getValueSeq());
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty() || Double.isNaN(row.getGroup().getDouble(columnName, row.getValueSeq()));
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BooleanGetter<GroupWithSchemaInfo> getBooleanGetter() {
        return new BaseTypeWrapperGetter.BooleanGetter<GroupWithSchemaInfo>() {
            @Override
            protected Boolean getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getGroup().getBoolean(columnName, row.getValueSeq());
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.StringGetter<GroupWithSchemaInfo> getStringGetter() {
        return new BaseTypeWrapperGetter.StringGetter<GroupWithSchemaInfo>() {
            @Override
            protected String getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getGroup().getString(columnName, row.getValueSeq());
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.BytesGetter<GroupWithSchemaInfo> getBytesGetter() {
        return new BaseTypeWrapperGetter.BytesGetter<GroupWithSchemaInfo>() {
            @Override
            protected byte[] getNonnullValue(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.getGroup().getBinary(columnName, row.getValueSeq()).getBytes();
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }
        };
    }

    @Override
    protected BaseTypeWrapperGetter.NullGetter<GroupWithSchemaInfo> getNullGetter() {
        return null;
    }

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getListGetter() {
        return new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            protected DataType getType() {
                return BaseDataType.LIST;
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }

            @Override
            protected BaseWrapper<?> getNonnull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return repeatedToListWrapper(row.getGroup(), columnName);
            }
        };
    }

    public static WrapperGetter<GroupWithSchemaInfo> MAP_GETTER = null;

    @Override
    protected WrapperGetter<GroupWithSchemaInfo> getMapGetter() {
        MAP_GETTER = new WrapperGetter<GroupWithSchemaInfo>() {
            @Override
            protected DataType getType() {
                return BaseDataType.MAP;
            }

            @Override
            protected boolean isNull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return row.isEmpty();
            }

            @Override
            protected BaseWrapper<?> getNonnull(GroupWithSchemaInfo row, String rowName, String columnName, int columnSeq) throws Exception {
                return groupToMapWrapper(
                        row.getGroup().getGroup(columnName, row.getValueSeq()),
                        WritableUtils.concatColumn(rowName, columnName)
                );
            }
        };
        return MAP_GETTER;
    }
}
