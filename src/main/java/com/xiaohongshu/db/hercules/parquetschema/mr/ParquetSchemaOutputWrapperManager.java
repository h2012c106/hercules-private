package com.xiaohongshu.db.hercules.parquetschema.mr;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.ListWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.parquet.ParquetSchemaUtils;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.TypeBuilderTreeNode;
import lombok.NonNull;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.Map;

import static com.xiaohongshu.db.hercules.core.utils.WritableUtils.FAKE_PARENT_NAME_USED_BY_LIST;
import static com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf.TRY_REQUIRED;
import static com.xiaohongshu.db.hercules.parquetschema.option.ParquetSchemaOptionsConf.TYPE_AUTO_UPGRADE;

/**
 * 由于要与{@link ParquetSchemaRecordWriter}逻辑强解耦，所以不能以内部类的方式存在，所以会有参数初始化以及一些多余的函数
 * 由于给repeated赋值只要无脑add即可，不需要记录下标，所以T为Group就够了
 */
public class ParquetSchemaOutputWrapperManager extends WrapperSetterFactory<TypeBuilderTreeNode> {

    private Map<String, DataType> columnTypeMap;
    private final ParquetDataTypeConverter converter;
    private final Type.Repetition defaultRepetition;
    private final boolean typeAutoUpgrade;
    private boolean first = true;

    /**
     * 利用setter方法最后一个参数传递是否repeated，实际是违规了，但反正方便
     */
    private static final int REPEATED = 0;
    private static final int NON_REPEATED = 1;

    public ParquetSchemaOutputWrapperManager(ParquetDataTypeConverter converter, GenericOptions options) {
        this.converter = converter;
        this.defaultRepetition = options.getBoolean(TRY_REQUIRED, false)
                ? Type.Repetition.REQUIRED
                : Type.Repetition.OPTIONAL;
        this.typeAutoUpgrade = options.getBoolean(TYPE_AUTO_UPGRADE, false);
    }

    public void setColumnTypeMap(Map<String, DataType> columnTypeMap) {
        this.columnTypeMap = columnTypeMap;
    }

    public ParquetDataTypeConverter getConverter() {
        return converter;
    }

    public void union(MapWrapper value, TypeBuilderTreeNode res) throws Exception {
        TypeBuilderTreeNode root = new TypeBuilderTreeNode(res.getColumnName(), Types.buildMessage(), null, BaseDataType.MAP);
        ParquetSchemaUtils.unionMapTree(res, mapWrapperToTree(value, root, null), typeAutoUpgrade, first, false);
        if (first) {
            first = false;
        }
    }

    private TypeBuilderTreeNode makeGroupNode(Type.Repetition repetition) {
        return new TypeBuilderTreeNode(FAKE_PARENT_NAME_USED_BY_LIST,
                Types.buildGroup(repetition), null, BaseDataType.MAP);
    }

    private void wrapperToRepeatedNode(BaseWrapper wrapper, TypeBuilderTreeNode root, String columnName) throws Exception {
        DataType baseDataType = wrapper.getType();
        if (baseDataType == BaseDataType.LIST) {
            ListWrapper listWrapper = (ListWrapper) wrapper;
            for (int i = 0; i < listWrapper.size(); ++i) {
                BaseWrapper subWrapper = listWrapper.get(i);
                // 这个Repetition无所谓的
                TypeBuilderTreeNode tmpNode = makeGroupNode(Type.Repetition.REQUIRED);
                getWrapperSetter(baseDataType).set(subWrapper, tmpNode, FAKE_PARENT_NAME_USED_BY_LIST, columnName, REPEATED);
                ParquetSchemaUtils.unionMapTree(root, tmpNode, typeAutoUpgrade, true, true);
            }
        } else {
            getWrapperSetter(baseDataType).set(wrapper, root, FAKE_PARENT_NAME_USED_BY_LIST, columnName, REPEATED);
        }
    }

    private TypeBuilderTreeNode mapWrapperToTree(MapWrapper wrapper, TypeBuilderTreeNode root, String wrapperPath) throws Exception {
        for (Map.Entry<String, BaseWrapper> entry : wrapper.entrySet()) {
            String columnName = entry.getKey();
            String fullColumnName = WritableUtils.concatColumn(wrapperPath, columnName);
            BaseWrapper subWrapper = entry.getValue();
            DataType columnType = columnTypeMap.getOrDefault(fullColumnName, subWrapper.getType());
            getWrapperSetter(columnType).set(subWrapper, root, wrapperPath, columnName, NON_REPEATED);
        }
        return root;
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
    protected WrapperSetter<TypeBuilderTreeNode> getByteSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.BYTE;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getShortSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.SHORT;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getIntegerSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.INTEGER;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getLongSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.LONG;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getLonglongSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.LONGLONG;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getFloatSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.FLOAT;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getDoubleSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.DOUBLE;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getDecimalSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.DECIMAL;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getBooleanSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.BOOLEAN;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getStringSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.STRING;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getDateSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.DATE;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getTimeSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.TIME;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getDatetimeSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.DATETIME;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getBytesSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.BYTES;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getNullSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public BaseDataType getType() {
                return BaseDataType.NULL;
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getListSetter() {
        return new WrapperSetter<TypeBuilderTreeNode>() {
            @Override
            public void set(@NonNull BaseWrapper wrapper, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
                wrapperToRepeatedNode(wrapper, row, columnName);
            }
        };
    }

    @Override
    protected WrapperSetter<TypeBuilderTreeNode> getMapSetter() {
        return new ParquetSchemaWrapperSetter() {
            @Override
            public DataType getType() {
                return BaseDataType.MAP;
            }

            @Override
            public void afterSet(BaseWrapper wrapper, TypeBuilderTreeNode child, String rowName, String columnName) throws Exception {
                String fullColumnName = WritableUtils.concatColumn(rowName, columnName);
                mapWrapperToTree((MapWrapper) wrapper, child, fullColumnName);
            }
        };
    }

    private abstract class ParquetSchemaWrapperSetter implements WrapperSetter<TypeBuilderTreeNode> {

        abstract public DataType getType();

        public void afterSet(BaseWrapper wrapper, TypeBuilderTreeNode child, String rowName, String columnName) throws Exception {
        }

        @Override
        public void set(@NonNull BaseWrapper wrapper, TypeBuilderTreeNode row, String rowName, String columnName, int columnSeq) throws Exception {
            TypeBuilderTreeNode node;
            if (wrapper.getType() != BaseDataType.NULL) {
                node = makeNode(columnName, getType(), row, columnSeq == REPEATED);
                TypeBuilderTreeNode child = row.addAndReturnChildren(node);
                afterSet(wrapper, child, rowName, columnName);
            }
        }
    }
}
