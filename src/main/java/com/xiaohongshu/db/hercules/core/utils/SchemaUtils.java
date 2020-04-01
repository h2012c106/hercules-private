package com.xiaohongshu.db.hercules.core.utils;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.serialize.SchemaFetcherPair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class SchemaUtils {

    /**
     * 获得目标列列表在源列表的下标，若目标列表多列，值为null
     *
     * @param columnMap
     * @return
     */
    public static List<Integer> mapColumnSeq(JSONObject columnMap) {
        List<String> sourceColumnList = SchemaFetcherPair.get(DataSourceRole.SOURCE).getColumnNameList();
        List<String> targetColumnList = SchemaFetcherPair.get(DataSourceRole.TARGET).getColumnNameList();

        return mapColumnSeq(sourceColumnList, targetColumnList, columnMap);
    }

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
}
