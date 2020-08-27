package com.xiaohongshu.db.hercules.core.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseDataSourceOptionsConf.COLUMN_DELIMITER;

public final class SchemaUtils {

    /**
     * 获得目标列列表在源列表的下标，若目标列表多列，值为null
     *
     * @param columnMap
     * @return
     */
    public static List<Integer> mapColumnSeq(List<String> sourceColumnList,
                                             List<String> targetColumnList,
                                             JSONObject columnMap) {
        sourceColumnList = sourceColumnList.stream()
                .map(columnName -> columnMap.getInnerMap().getOrDefault(columnName, columnName).toString())
                .collect(Collectors.toList());
        Map<String, Integer> sourceNameToSeq = new HashMap<>();
        for (int i = 0; i < sourceColumnList.size(); ++i) {
            sourceNameToSeq.put(sourceColumnList.get(i), i);
        }
        List<Integer> res = new ArrayList<>(targetColumnList.size());
        for (String columnName : targetColumnList) {
            res.add(sourceNameToSeq.getOrDefault(columnName, null));
        }
        return res;
    }

    public static BiMap<String, String> convertColumnMapFromOption(@NonNull JSONObject jsonObject) {
        // 源列名->目标列名
        BiMap<String, String> biColumnMap = HashBiMap.create(jsonObject.size());
        for (String key : jsonObject.keySet()) {
            // 如果存在相同的value则bimap会报错
            biColumnMap.put(key.trim(), jsonObject.getString(key).trim());
        }
        return biColumnMap;
    }

    public static List<String> convertNameFromOption(@NonNull String[] names) {
        return Arrays.stream(names).map(String::trim).filter(item -> item.length() > 0).collect(Collectors.toList());
    }

    public static Map<String, DataType> convertTypeFromOption(@NonNull JSONObject jsonObject) {
        return convertTypeFromOption(jsonObject, NullCustomDataTypeManager.INSTANCE);
    }

    public static Map<String, DataType> convertTypeFromOption(@NonNull JSONObject jsonObject, final CustomDataTypeManager<?, ?> manager) {
        return jsonObject.getInnerMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey().trim(),
                        entry -> DataType.valueOfIgnoreCase((String) entry.getValue(), manager)));
    }

    public static JSONObject convertTypeToOption(@NonNull Map<String, DataType> map) {
        JSONObject res = new JSONObject();
        res.putAll(map);
        return res;
    }

    public static List<Set<String>> convertIndexFromOption(@NonNull String[] array) {
        return convertIndexFromOption(Arrays.asList(array));
    }

    public static List<Set<String>> convertIndexFromOption(@NonNull List<String> list) {
        List<Set<String>> res = new ArrayList<>(list.size());
        for (String group : list.stream().map(String::trim).filter(item -> item.length() > 0).collect(Collectors.toList())) {
            Set<String> groupSet = Arrays.stream(StringUtils.split(group, COLUMN_DELIMITER))
                    .map(String::trim)
                    .filter(item -> item.length() > 0)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            if (groupSet.size() > 0) {
                res.add(groupSet);
            }
        }
        return res;
    }

    public static String[] convertIndexToOption(List<Set<String>> list) {
        List<String> res = new ArrayList<>(list.size());
        for (Set<String> group : list) {
            res.add(StringUtils.join(group, COLUMN_DELIMITER));
        }
        return res.toArray(new String[0]);
    }

    /**
     * 把顺序的索引列表按照最左匹配原则展开，适用于Mysql、Mongo
     *
     * @param indexList
     * @return
     */
    public static List<Set<String>> unwrapIndexList(List<String> indexList) {
        List<Set<String>> res = new ArrayList<>(indexList.size());
        for (int i = 1; i <= indexList.size(); ++i) {
            res.add(new LinkedHashSet<>(indexList.subList(0, i)));
        }
        return res;
    }
}
