package com.xiaohongshu.db.hercules.parquetschema.mr;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.entity.ExtendedDate;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.TypeBuilderTreeNode;
import lombok.NonNull;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST;
import static com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf.TRY_REQUIRED;
import static com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf.TYPE_AUTO_UPGRADE;

/**
 * 由于要与{@link ParquetSchemaRecordWriter}逻辑强解耦，所以不能以内部类的方式存在，所以会有参数初始化以及一些多余的函数
 * 由于给repeated赋值只要无脑add即可，不需要记录下标，所以T为Group就够了
 */
public class ParquetSchemaOutputWrapperManager extends WrapperSetterFactory<TypeBuilderTreeNode> {

    private final ParquetDataTypeConverter converter;
    private final Type.Repetition defaultRepetition;
    private final boolean typeAutoUpgrade;
    private boolean first = true;

    /**
     * 利用setter方法最后一个参数传递是否repeated，实际是违规了，但反正方便
     */
    private static final int REPEATED = 0;

    public ParquetSchemaOutputWrapperManager() {
        this.converter = (ParquetDataTypeConverter) HerculesContext.getAssemblySupplierPair().getTargetItem().getDataTypeConverter();
        this.defaultRepetition = HerculesContext.getWrappingOptions().getTargetOptions().getBoolean(TRY_REQUIRED, false)
                ? Type.Repetition.REQUIRED
                : Type.Repetition.OPTIONAL;
        this.typeAutoUpgrade = HerculesContext.getWrappingOptions().getTargetOptions().getBoolean(TYPE_AUTO_UPGRADE, false);
    }

    public ParquetDataTypeConverter getConverter() {
        return converter;
    }

    public void union(MapWrapper value, TypeBuilderTreeNode res) throws Exception {
        TypeBuilderTreeNode root = new TypeBuilderTreeNode(res.getColumnName(), Types.buildMessage(), null, BaseDataType.MAP);
        ParquetSchemaUtils.unionMapTree(res, writeMapWrapper(value, root, null), typeAutoUpgrade, first, false);
        if (first) {
            first = false;
        }
    }

    private TypeBuilderTreeNode makeGroupNode(Type.Repetition repetition) {
        return new TypeBuilderTreeNode(FAKE_PARENT_NAME_USED_BY_LIST,
                Types.buildGroup(repetition), null, BaseDataType.MAP);
    }

    private void wrapperToRepeatedNode(BaseWrapper<?> wrapper, TypeBuilderTreeNode root, String columnName) throws Exception {
        DataType baseDataType = wrapper.getType();
        if (baseDataType == BaseDataType.LIST) {
            ListWrapper listWrapper = (ListWrapper) wrapper;
            for (int i = 0; i < listWrapper.size(); ++i) {
                BaseWrapper<?> subWrapper = listWrapper.get(i);
                // 这个Repetition无所谓的
                TypeBuilderTreeNode tmpNode = makeGroupNode(Type.Repetition.REQUIRED);
                getWrapperSetter(baseDataType).set(subWrapper, tmpNode, FAKE_PARENT_NAME_USED_BY_LIST, columnName, REPEATED);
                ParquetSchemaUtils.unionMapTree(root, tmpNode, typeAutoUpgrade, true, true);
            }
        } else {
            getWrapperSetter(baseDataType).set(wrapper, root, FAKE_PARENT_NAME_USED_BY_LIST, columnName, REPEATED);
        }
    }

    private TypeBuilderTreeNode makeNode(String columnName, DataType baseDataType, TypeBuilderTreeNode parent, Type.Repetition repetition) {
        return new TypeBuilderTreeNode(columnName, repetition, parent, baseDataType);
    }

    private TypeBuilderTreeNode makeNode(String columnName, DataType baseDataType, TypeBuilderTreeNode parent, boolean repeated) {
        Type.Repetition repetition;
        if (repeated) {
            repetition = Type.Repetition.REPEATED;
        } else {
            repetition = defaultRepetition;
        }
        return makeNode(columnName, baseDataType, parent, repetition);
    }

    @Override
    protected BaseTypeWrapperSetter.ByteSetter<TypeBuilderTreeNode> getByteSetter() {
        return new BaseTypeWrapperSetter.ByteSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Byte value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
                TypeBuilderTreeNode node = makeNode(columnName, getType(), row, columnSeq == REPEATED);
                row.addAndReturnChildren(node);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.ShortSetter<TypeBuilderTreeNode> getShortSetter() {
        return new BaseTypeWrapperSetter.ShortSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Short value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
                TypeBuilderTreeNode node = makeNode(columnName, getType(), row, columnSeq == REPEATED);
                row.addAndReturnChildren(node);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.IntegerSetter<TypeBuilderTreeNode> getIntegerSetter() {
        return new BaseTypeWrapperSetter.IntegerSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Integer value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
                TypeBuilderTreeNode node = makeNode(columnName, getType(), row, columnSeq == REPEATED);
                row.addAndReturnChildren(node);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LongSetter<TypeBuilderTreeNode> getLongSetter() {
        return new BaseTypeWrapperSetter.LongSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Long value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
                TypeBuilderTreeNode node = makeNode(columnName, getType(), row, columnSeq == REPEATED);
                row.addAndReturnChildren(node);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.LonglongSetter<TypeBuilderTreeNode> getLonglongSetter() {
        return new BaseTypeWrapperSetter.LonglongSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(BigInteger value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
                TypeBuilderTreeNode node = makeNode(columnName, getType(), row, columnSeq == REPEATED);
                row.addAndReturnChildren(node);
            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.FloatSetter<TypeBuilderTreeNode> getFloatSetter() {
        return new BaseTypeWrapperSetter.FloatSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Float value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DoubleSetter<TypeBuilderTreeNode> getDoubleSetter() {
        return new BaseTypeWrapperSetter.DoubleSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Double value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DecimalSetter<TypeBuilderTreeNode> getDecimalSetter() {
        return new BaseTypeWrapperSetter.DecimalSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(BigDecimal value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BooleanSetter<TypeBuilderTreeNode> getBooleanSetter() {
        return new BaseTypeWrapperSetter.BooleanSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Boolean value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.StringSetter<TypeBuilderTreeNode> getStringSetter() {
        return new BaseTypeWrapperSetter.StringSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(String value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DateSetter<TypeBuilderTreeNode> getDateSetter() {
        return new BaseTypeWrapperSetter.DateSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.TimeSetter<TypeBuilderTreeNode> getTimeSetter() {
        return new BaseTypeWrapperSetter.TimeSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.DatetimeSetter<TypeBuilderTreeNode> getDatetimeSetter() {
        return new BaseTypeWrapperSetter.DatetimeSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(ExtendedDate value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.BytesSetter<TypeBuilderTreeNode> getBytesSetter() {
        return new BaseTypeWrapperSetter.BytesSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(byte[] value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected BaseTypeWrapperSetter.NullSetter<TypeBuilderTreeNode> getNullSetter() {
        return new BaseTypeWrapperSetter.NullSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnullValue(Void value, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {

            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getListSetter() {
        return new WrapperSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnull(@NonNull BaseWrapper<?> wrapper, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
                wrapperToRepeatedNode(wrapper, row, columnName);
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getMapSetter() {
        return new WrapperSetter<TypeBuilderTreeNode>() {
            @Override
            protected void setNull(TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            }

            @Override
            protected void setNonnull(@NonNull BaseWrapper<?> wrapper, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
                TypeBuilderTreeNode node = makeNode(columnName, BaseDataType.MAP, row, columnSeq == REPEATED);
                TypeBuilderTreeNode child = row.addAndReturnChildren(node);
                String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                writeMapWrapper((MapWrapper) wrapper, child, fullColumnName);
            }
        };
    }
}
