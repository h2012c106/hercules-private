package com.xiaohongshu.db.hercules.parquet.schema;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.serialize.DataType;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import lombok.NonNull;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TypeBuilderTreeNode {
    private String columnName;
    private DataType type;
    private Type.Repetition repetition;
    private TypeBuilderTreeNode parent;
    private Map<String, TypeBuilderTreeNode> children;

    public TypeBuilderTreeNode(String columnName, Type.Repetition repetition,
                               TypeBuilderTreeNode parent, DataType type) {
        this.columnName = columnName;
        this.type = type;
        this.repetition = repetition;
        this.parent = parent;
        this.children = type == DataType.MAP ? new LinkedHashMap<>() : null;
    }

    public TypeBuilderTreeNode(String columnName, Types.Builder<?, ? extends Type> value,
                               TypeBuilderTreeNode parent, DataType type) {
        this.columnName = columnName;
        this.type = type;
        this.repetition = value.named(columnName).getRepetition();
        this.parent = parent;
        this.children = type == DataType.MAP ? new LinkedHashMap<>() : null;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Map<String, TypeBuilderTreeNode> getChildren() {
        return children;
    }

    public DataType getType() {
        return type;
    }

    public void setType(DataType type) {
        this.type = type;
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

    public TypeBuilderTreeNode addAndReturnChildren(@NonNull TypeBuilderTreeNode newChildren) {
        if (type != DataType.MAP) {
            throw new RuntimeException("Unexpected to add children to a ungroup tree node.");
        }
        TypeBuilderTreeNode storedValue = children.get(newChildren.getColumnName());
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

    public Types.Builder<?, ? extends Type> getValue(ParquetDataTypeConverter converter) {
        if (parent == null) {
            return Types.buildMessage();
        } else {
            return converter.convertDataType(type, repetition);
        }
    }

    public void setRepetition(Type.Repetition repetition) {
        this.repetition = repetition;
    }

    public Type.Repetition getRepetition() {
        return repetition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypeBuilderTreeNode that = (TypeBuilderTreeNode) o;
        return com.google.common.base.Objects.equal(columnName, that.columnName) &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columnName, type);
    }
}
