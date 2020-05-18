package com.xiaohongshu.db.hercules.parquet.schema;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.schema.DataTypeConverter;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import lombok.NonNull;
import org.apache.parquet.schema.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Parquet作为底层数据，各个逻辑类型写parquet的姿势（写入类型，写入方法）都会有不同，目前已知的有sqoop任务、hive以及parquet logic type的写入姿势。
 * 目前矛盾主要如下：（其中{@link DataType#LONGLONG}由于sql里没有对应类型，所以sqoop和hive不支持）
 * 数据类型\上层读写               sqoop			hive											parquet logic type
 * {@link DataType#BYTE}        int32           int32 (INTEGER(8,true))                         同hive
 * {@link DataType#SHORT}       int32           int32 (INTEGER(16,true))                        同hive
 * {@link DataType#LONGLONG}    不支持           不支持                                           int96
 * {@link DataType#DECIMAL}	    binary (STRING)	binary (DECIMAL(p,s)) (p、s在hercules体系无法获得)	{@see <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal">parquet decimal annotation</a>}
 * {@link DataType#DATE}		int64			int32 (DATE)									同hive
 * {@link DataType#TIME}		int64			不支持											{@see <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time">parquet time annotation</a>}
 * {@link DataType#DATETIME}    int64			int96											{@see <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp">parquet timestamp annotation</a>}
 * 为了适配各种上层读写parquet的姿势，故会做不同的类型转换/IO姿势，以子类方式实现。
 * 其中decimal由于precision+scale无法根据columnMap获得，所以decimal data type无法生成对应的schema，但是知道schema后IO是没问题的。
 */
public abstract class ParquetDataTypeConverter implements DataTypeConverter<ParquetType, MessageType> {

    private static final String GENERATED_MESSAGE_NAME = "hercules_generated_message";
    private static final Type.Repetition DEFAULT_REPETITION = Type.Repetition.OPTIONAL;

    protected String getAnnotationName(LogicalTypeAnnotation annotation) {
        Method getTypeMethod;
        try {
            getTypeMethod = LogicalTypeAnnotation.class.getDeclaredMethod("getType");
        } catch (NoSuchMethodException e) {
            throw new SchemaException(e);
        }
        boolean accessible = getTypeMethod.isAccessible();
        getTypeMethod.setAccessible(true);
        try {
            return getTypeMethod.invoke(annotation).toString();
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SchemaException(e);
        } finally {
            getTypeMethod.setAccessible(accessible);
        }
    }

    protected String getTypeName(Type type) {
        if (type.isPrimitive()) {
            return ((PrimitiveType) type).getPrimitiveTypeName().toString();
        } else {
            return "GROUP";
        }
    }

    private void recursiveGetMessageType(Map<String, DataType> res, Type type, String parentName) {
        String fullColumnName = WritableUtils.concatColumn(parentName, type.getName());
        if (type.isRepetition(Type.Repetition.REPEATED)) {
            res.put(fullColumnName, DataType.LIST);
            return;
        }
        if (type.isPrimitive()) {
            res.put(fullColumnName, convertElementType(new ParquetType(type)));
        } else {
            GroupType groupType = (GroupType) type;
            for (Type child : groupType.getFields()) {
                recursiveGetMessageType(res, child, fullColumnName);
            }
            res.put(fullColumnName, DataType.MAP);
        }
    }

    /**
     * 把除了repeated内部的都出了
     *
     * @param line
     * @return
     */
    @Override
    public Map<String, DataType> convertRowType(MessageType line) {
        Map<String, DataType> res = new HashMap<>();
        recursiveGetMessageType(res, line, null);
        return res;
    }

    abstract protected Types.Builder<?, ? extends Type> convertDataType(DataType dataType, Type.Repetition repetition);

    private Types.Builder<?, ? extends Type> convertDataType(DataType dataType) {
        return convertDataType(dataType, DEFAULT_REPETITION);
    }

    public MessageType convertTypeMap(List<String> columnNameList, Map<String, DataType> columnTypeMap) {
        if (columnNameList.size() == 0) {
            throw new RuntimeException("The parquet-schema-making need to know the transferred column list.");
        }
        if (!columnTypeMap.keySet().containsAll(columnNameList)) {
            Set<String> tmpSet = new HashSet<>(columnNameList);
            tmpSet.removeAll(columnTypeMap.keySet());
            throw new RuntimeException("The parquet-schema-making need to know all columns' type, now miss: " + tmpSet);
        }

        Set<String> columnNameSet = new LinkedHashSet<>(columnNameList);

        // 根节点是message
        BuilderTreeNode root = new BuilderTreeNode(GENERATED_MESSAGE_NAME, Types.buildMessage(), null, DataType.MAP);
        for (String columnName : columnNameSet) {
            BuilderTreeNode tmpNode = root;
            WritableUtils.ColumnSplitResult splitResult = WritableUtils.splitColumnWrapped(columnName);
            for (String parentColumnName : splitResult.getParentColumnList()) {
                tmpNode = tmpNode.addAndReturnChildren(new BuilderTreeNode(parentColumnName, convertDataType(DataType.MAP), tmpNode, DataType.MAP));
            }
            DataType finalColumnType = columnTypeMap.get(columnName);
            tmpNode.addAndReturnChildren(new BuilderTreeNode(splitResult.getFinalColumn(), convertDataType(finalColumnType), tmpNode, finalColumnType));
        }

        return (MessageType) calculateTree(root);
    }

    /**
     * 后序遍历树
     *
     * @return
     */
    private Type calculateTree(BuilderTreeNode node) {
        if (node.getChildren() == null) {
            return node.getValue().named(node.getColumnName());
        } else {
            Types.GroupBuilder<GroupType> groupBuilder = (Types.GroupBuilder<GroupType>) node.getValue();
            for (BuilderTreeNode child : node.getChildren().values()) {
                groupBuilder.addField(calculateTree(child));
            }
            return groupBuilder.named(node.getColumnName());
        }
    }

    /**
     * 用于记录Type Builder，因为parquet group type成型后就不允许加列了，所以得先把半成品记录成树，最后一次性成型
     */
    private class BuilderTreeNode {
        private String columnName;
        private DataType type;
        private Types.Builder<?, ? extends Type> value;
        private BuilderTreeNode parent;
        private Map<String, BuilderTreeNode> children;

        public BuilderTreeNode(String columnName, Types.Builder<?, ? extends Type> value,
                               BuilderTreeNode parent, DataType type) {
            this.columnName = columnName;
            this.type = type;
            this.value = value;
            this.parent = parent;
            this.children = type == DataType.MAP ? new LinkedHashMap<>() : null;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public Map<String, BuilderTreeNode> getChildren() {
            return children;
        }

        public DataType getType() {
            return type;
        }

        private List<String> getFullColumnName() {
            if (parent == null) {
                return new ArrayList<>();
            }
            List<String> res = parent.getFullColumnName();
            res.add(columnName);
            return res;
        }

        private String getFullColumnNameStr() {
            return WritableUtils.concatColumn(getFullColumnName());
        }

        public BuilderTreeNode addAndReturnChildren(@NonNull BuilderTreeNode newChildren) {
            if (type != DataType.MAP) {
                throw new RuntimeException("Unexpected to add children to a ungroup tree node.");
            }
            BuilderTreeNode storedValue = children.get(newChildren.getColumnName());
            if (storedValue == null) {
                children.put(newChildren.getColumnName(), newChildren);
                return newChildren;
            } else if (storedValue.equals(newChildren)) {
                // 返回原值，新值没有被插入
                return storedValue;
            } else {
                // 如果同名node类型不同，直接抛错
                throw new RuntimeException(String.format("Unequaled column type of column [%s]: %s vs %s",
                        getFullColumnNameStr(), storedValue.getType(), newChildren.getType()));
            }
        }

        public Types.Builder<?, ? extends Type> getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BuilderTreeNode that = (BuilderTreeNode) o;
            return com.google.common.base.Objects.equal(columnName, that.columnName) &&
                    type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(columnName, type);
        }
    }
}
