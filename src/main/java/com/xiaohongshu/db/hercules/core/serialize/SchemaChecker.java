package com.xiaohongshu.db.hercules.core.serialize;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.xiaohongshu.db.hercules.common.options.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.exceptions.SchemaException;
import com.xiaohongshu.db.hercules.core.options.WrappingOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 用于检查上下游column
 * 大小写敏感
 */
public class SchemaChecker {

    private static final Log LOG = LogFactory.getLog(SchemaChecker.class);

    private BaseSchemaFetcher sourceSchemaFetcher;
    private BaseSchemaFetcher targetSchemaFetcher;
    private WrappingOptions options;

    public SchemaChecker(BaseSchemaFetcher sourceSchemaFetcher,
                         BaseSchemaFetcher targetSchemaFetcher,
                         WrappingOptions options) {
        this.sourceSchemaFetcher = sourceSchemaFetcher;
        this.targetSchemaFetcher = targetSchemaFetcher;
        this.options = options;
    }

    private <T> List<T> getCollectionDuplicate(Collection<T> collection) {
        List<T> duplicateList = new ArrayList<>();
        Set<T> checkedSet = new HashSet<>();
        for (T item : collection) {
            if (checkedSet.contains(item)) {
                duplicateList.add(item);
            } else {
                checkedSet.add(item);
            }
        }
        return duplicateList;
    }

    private void validateSourceTargetColumnNameList() {
        for (DataSourceRole role : DataSourceRole.values()) {
            BaseSchemaFetcher tmpFetcher = role.isSource() ? sourceSchemaFetcher : targetSchemaFetcher;
            List<String> duplicateList = getCollectionDuplicate((List<String>) tmpFetcher.getColumnNameList());
            if (duplicateList.size() > 0) {
                throw new SchemaException(String.format("Exist duplicate column name at %s: %s",
                        role.name(),
                        duplicateList.toString())
                );
            }
        }
    }

    private void validateConvertedColumnNameList(List<String> convertedSourceColumnNameList) {
        List<String> duplicateList = getCollectionDuplicate(convertedSourceColumnNameList);
        if (duplicateList.size() > 0) {
            throw new SchemaException("Exist duplicate column name after applying the column name map: " + duplicateList);
        }
    }

    /**
     * 检查映射好的上游列和下游列的对应关系（多列少列）
     *
     * @param convertedSourceColumnNameList
     * @param columnMap
     */
    private void validateColumns(List<String> convertedSourceColumnNameList, BiMap<String, String> columnMap) {
        Set<String> tmpSet;
        // 检查源数据源多列
        tmpSet = new HashSet<>(convertedSourceColumnNameList);
        tmpSet.removeAll((List<String>) targetSchemaFetcher.getColumnNameList());
        if (tmpSet.size() > 0) {
            Map<String, String> inversedColumnMap = columnMap.inverse();
            Set<String> tmpTmpSet = tmpSet.stream()
                    .map(columnName -> inversedColumnMap.getOrDefault(columnName, columnName))
                    .collect(Collectors.toSet());
            if (!options.getCommonOptions().getBoolean(CommonOptionsConf.ALLOW_SOURCE_MORE_COLUMN, false)) {
                // 需要把多的列名根据转换规则转回去，不然显示的是转换后的如果变动较大会造成看日志的人迷惑
                throw new SchemaException(String.format("Source data source has more columns: %s, " +
                                "if want to ignore, please use '--%s'",
                        tmpTmpSet, CommonOptionsConf.ALLOW_SOURCE_MORE_COLUMN
                ));
            } else {
                LOG.warn(String.format("Source data source has more columns: %s", tmpTmpSet));
            }
        }

        // 检查目标数据源多列
        tmpSet = new HashSet<>((List<String>) targetSchemaFetcher.getColumnNameList());
        tmpSet.removeAll(convertedSourceColumnNameList);
        if (tmpSet.size() > 0) {
            if (!options.getCommonOptions().getBoolean(CommonOptionsConf.ALLOW_TARGET_MORE_COLUMN, false)) {
                throw new SchemaException(String.format("Target data source has more columns: %s, " +
                        "if want to ignore, please use '--%s'", tmpSet, CommonOptionsConf.ALLOW_TARGET_MORE_COLUMN));
            } else {
                LOG.warn(String.format("Target data source has more columns: %s", tmpSet));
            }
        }

        // 检查两侧交集，至少得有一列才合理
        tmpSet = new HashSet<>((List<String>) targetSchemaFetcher.getColumnNameList());
        tmpSet.retainAll(convertedSourceColumnNameList);
        if (tmpSet.size() == 0) {
            throw new SchemaException("Not column is in the intersection, meaningless!");
        } else {
            LOG.info("The public columns is: " + tmpSet);
        }
    }

    /**
     * 检查转好后列名有无重复
     * 按名称且大小写敏感地检查上下游列是否对得上，再根据配置的是否报警来决定是否抛错
     */
    public void validate() {
        JSONObject jsonObject = options.getCommonOptions().getJson(CommonOptionsConf.COLUMN_MAP, new JSONObject());
        BiMap<String, String> columnMap = HashBiMap.create(jsonObject.size());
        for (String key : jsonObject.keySet()) {
            // 如果存在相同的value则bimap会报错
            columnMap.put(key, jsonObject.getString(key));
        }
        // 按照规则映射后的源数据库列名
        List<String> convertedSourceColumnNameList = ((List<String>) sourceSchemaFetcher.getColumnNameList())
                .stream()
                .map(columnName -> columnMap.getOrDefault(columnName, columnName))
                .collect(Collectors.toList());

        validateSourceTargetColumnNameList();
        validateConvertedColumnNameList(convertedSourceColumnNameList);
        validateColumns(convertedSourceColumnNameList, columnMap);
    }
}
