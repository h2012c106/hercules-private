package com.xiaohongshu.db.hercules.parquet;

import com.google.common.collect.Sets;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetDataTypeConverter;
import com.xiaohongshu.db.hercules.parquet.schema.ParquetType;
import com.xiaohongshu.db.hercules.parquet.schema.TypeBuilderTreeNode;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class ParquetSchemaUtils {

    public static final String GENERATED_MESSAGE_NAME = "hercules_generated_message";

    private final static Map<DataType, Set<DataType>> TYPE_UPGRADE_MAP;

    static {
        TYPE_UPGRADE_MAP = new HashMap<>();
        for (BaseDataType baseDataType : BaseDataType.values()) {
            TYPE_UPGRADE_MAP.put(baseDataType, Sets.newHashSet(baseDataType));
        }
        TYPE_UPGRADE_MAP.get(BaseDataType.BYTE).addAll(Sets.newHashSet(BaseDataType.SHORT,
                BaseDataType.INTEGER,
                BaseDataType.LONG,
                BaseDataType.LONGLONG,
                BaseDataType.FLOAT,
                BaseDataType.DOUBLE,
                BaseDataType.DECIMAL));
        TYPE_UPGRADE_MAP.get(BaseDataType.SHORT).addAll(Sets.newHashSet(BaseDataType.INTEGER,
                BaseDataType.LONG,
                BaseDataType.LONGLONG,
                BaseDataType.FLOAT,
                BaseDataType.DOUBLE,
                BaseDataType.DECIMAL));
        TYPE_UPGRADE_MAP.get(BaseDataType.INTEGER).addAll(Sets.newHashSet(BaseDataType.LONG,
                BaseDataType.LONGLONG,
                BaseDataType.FLOAT,
                BaseDataType.DOUBLE,
                BaseDataType.DECIMAL));
        TYPE_UPGRADE_MAP.get(BaseDataType.LONG).addAll(Sets.newHashSet(BaseDataType.LONGLONG,
                BaseDataType.DECIMAL));
        TYPE_UPGRADE_MAP.get(BaseDataType.FLOAT).addAll(Sets.newHashSet(BaseDataType.DOUBLE,
                BaseDataType.DECIMAL));
        TYPE_UPGRADE_MAP.get(BaseDataType.DOUBLE).addAll(Sets.newHashSet(BaseDataType.DECIMAL));
    }

    public static TypeBuilderTreeNode buildTree(GroupType groupType, TypeBuilderTreeNode parent, ParquetDataTypeConverter converter) {
        TypeBuilderTreeNode res = new TypeBuilderTreeNode(groupType.getName(), groupType.getRepetition(), parent, BaseDataType.MAP);
        for (Type type : groupType.getFields()) {
            String columnName = type.getName();
            Type.Repetition repetition = type.getRepetition();
            DataType baseDataType = converter.convertElementType(new ParquetType(type, false));
            if (type.isPrimitive()) {
                res.addAndReturnChildren(new TypeBuilderTreeNode(columnName, repetition, res, baseDataType));
            } else {
                res.addAndReturnChildren(buildTree((GroupType) type, res, converter));
            }
        }
        return res;
    }

    /**
     * 后序遍历树
     *
     * @return
     */
    public static Type calculateTree(TypeBuilderTreeNode node, ParquetDataTypeConverter converter) {
        if (node.getChildren() == null) {
            return node.getValue(converter).named(node.getColumnName());
        } else {
            Types.GroupBuilder<? extends GroupType> groupBuilder = (Types.GroupBuilder<? extends GroupType>) node.getValue(converter);
            for (TypeBuilderTreeNode child : node.getChildren().values()) {
                groupBuilder.addField(calculateTree(child, converter));
            }
            return groupBuilder.named(node.getColumnName());
        }
    }

    private static DataType getGradeHigherOne(DataType x, DataType y) {
        // TYPE_UPGRADE_MAP存的是谁是key的大哥（包括自己）
        if (TYPE_UPGRADE_MAP.get(x).contains(y)) {
            return y;
        } else if (TYPE_UPGRADE_MAP.get(y).contains(x)) {
            return x;
        } else {
            return null;
        }
    }

    private static DataType getNegotiatedType(DataType x, DataType y, boolean typeAutoUpgrade) {
        if (typeAutoUpgrade) {
            return getGradeHigherOne(x, y);
        } else {
            return x == y ? x : null;
        }
    }

    /**
     * 将{@param source}合并进{@param target}，策略：
     * source\target    required    optional    repeated    不存在
     * required         required    optional    repeated    optional
     * optional         optional    optional    repeated    optional
     * repeated         repeated    repeated    repeated    repeated
     * 不存在            optional    optional    repeated    /
     * {@param source}、{@param target}必须为Map类型的
     * 不同类型的同名子元素将抛错
     *
     * @param allowTargetUnexist 允许目标少列，仅会发生于第一次当目标的node，第二次以后就为false
     * @param allowSourceUnexist 允许源少列，仅发生于循环list生产假node作为源时
     */
    public static void unionMapTree(TypeBuilderTreeNode target, TypeBuilderTreeNode source,
                                    boolean typeAutoUpgrade, boolean allowTargetUnexist, boolean allowSourceUnexist) {
        if (target.getType() != BaseDataType.MAP || source.getType() != BaseDataType.MAP) {
            throw new RuntimeException("Two sides of union must be MAP.");
        }
        Map<String, TypeBuilderTreeNode> targetChildren = target.getChildren();
        Map<String, TypeBuilderTreeNode> sourceChildren = source.getChildren();
        if (!allowSourceUnexist) {
            // 先看target里有source没有的列
            Set<String> targetMoreColumnSet = new LinkedHashSet<>(targetChildren.keySet());
            targetMoreColumnSet.removeAll(sourceChildren.keySet());
            for (String targetMoreColumn : targetMoreColumnSet) {
                TypeBuilderTreeNode child = targetChildren.get(targetMoreColumn);
                if (child.getRepetition() == Type.Repetition.REQUIRED) {
                    child.setRepetition(Type.Repetition.OPTIONAL);
                }
            }
        }
        // 再看source里存在的列
        for (Map.Entry<String, TypeBuilderTreeNode> entry : sourceChildren.entrySet()) {
            String columnName = entry.getKey();
            TypeBuilderTreeNode sourceChild = entry.getValue();
            TypeBuilderTreeNode targetChild = targetChildren.get(columnName);

            Type.Repetition sourceRepetition = sourceChild.getRepetition();
            DataType sourceBaseDataType = sourceChild.getType();
            if (targetChild == null) {
                if (sourceRepetition == Type.Repetition.REQUIRED && !allowTargetUnexist) {
                    sourceChild.setRepetition(Type.Repetition.OPTIONAL);
                }
                target.addAndReturnChildren(sourceChild);
            } else {
                Type.Repetition targetRepetition = targetChild.getRepetition();
                DataType targetBaseDataType = targetChild.getType();

                DataType negotiatedType = getNegotiatedType(targetBaseDataType, sourceBaseDataType, typeAutoUpgrade);
                if (negotiatedType == null) {
                    throw new RuntimeException(String.format("Unmatched data type of column [%s], expected [%s], actually [%s].", columnName, targetBaseDataType, sourceBaseDataType));
                } else {
                    targetChild.setType(negotiatedType);
                }

                // 如果存在子group需要合并子group
                if (targetBaseDataType == BaseDataType.MAP) {
                    // 目标、源node皆有此列，至少说明这列不论对于目标还是源都被正儿八经置过，故内部统统care少列
                    unionMapTree(targetChild, sourceChild, typeAutoUpgrade, false, false);
                }

                // 如果true，说明自己更严格，则抄更松的source，否则说明自己松，用自己
                if (targetRepetition.isMoreRestrictiveThan(sourceRepetition)) {
                    targetChild.setRepetition(sourceRepetition);
                }
            }
        }
    }
}
