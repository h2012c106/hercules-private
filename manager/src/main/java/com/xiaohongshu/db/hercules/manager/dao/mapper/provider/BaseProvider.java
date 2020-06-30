package com.xiaohongshu.db.hercules.manager.dao.mapper.provider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.xiaohongshu.db.hercules.manager.dao.typehandler.MapTypeHandler;
import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.share.utils.Constant.ID_COL_NAME;

public abstract class BaseProvider<T> {

    private Map<String, String> humpToUnderlineMap;
    private Set<String> unallowedPatchColumn;
    private Set<String> unallowedInsertColumn;

    public BaseProvider() {
        humpToUnderlineMap = humpToUnderlineException();
        humpToUnderlineMap = humpToUnderlineMap == null
                ? new ConcurrentHashMap<>()
                : new ConcurrentHashMap<>(humpToUnderlineMap);

        Set<String> tmpSet;

        tmpSet = unallowedPatchColumn();
        unallowedPatchColumn = Sets.newHashSet(ID_COL_NAME);
        if (tmpSet != null) {
            unallowedPatchColumn.addAll(tmpSet);
        }

        tmpSet = unallowedInsertColumn();
        unallowedInsertColumn = Sets.newHashSet(ID_COL_NAME);
        if (tmpSet != null) {
            unallowedInsertColumn.addAll(tmpSet);
        }
    }

    abstract protected String getTableName();

    /**
     * 仅允许系统内部修改的变量，不允许外部通过rest修改，无需写id，会自动加进去
     *
     * @return
     */
    protected Set<String> unallowedPatchColumn() {
        return null;
    }

    /**
     * 初始化时无需insert的变量名
     *
     * @return
     */
    protected Set<String> unallowedInsertColumn() {
        return null;
    }

    /**
     * 驼峰转下划线命名的例外情况
     *
     * @return
     */
    protected Map<String, String> humpToUnderlineException() {
        return null;
    }

    private String humpToUnderline(String name) {
        return humpToUnderlineMap.computeIfAbsent(name, key -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < key.length(); ++i) {
                char c = key.charAt(i);
                if (Character.isUpperCase(c)) {
                    sb.append('_');
                }
                sb.append(c);
            }
            return sb.toString().toLowerCase();
        });
    }

    public String updateWithMap(Map<String, Object> map) throws JsonProcessingException {
        List<String> setList = new ArrayList<>(map.size() - 1);
        long id = new BigInteger(map.get(ID_COL_NAME).toString()).longValueExact();
        for (String notAllowedPatch : unallowedPatchColumn) {
            map.remove(notAllowedPatch);
        }
        for (String key : map.keySet()) {
            setList.add("`" + humpToUnderline(key) + "` = #{" + key
                    + ((map.get(key) instanceof Map) ? (",typeHandler=" + MapTypeHandler.class.getCanonicalName()) : "")
                    + "}");
        }
        return "update `" + getTableName() + "` set " + StringUtils.join(setList, ", ") + " where `id` = '" + id + "';";
    }

    public String insert(T entity) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> objectMap = objectMapper.convertValue(entity, Map.class);
        List<String> propertyNameList = new ArrayList<>(objectMap.keySet());
        propertyNameList = propertyNameList.stream().filter(name -> !unallowedInsertColumn.contains(name)).filter(name -> objectMap.get(name) != null).collect(Collectors.toList());
        List<String> columnNameList = propertyNameList.stream().map(this::humpToUnderline).collect(Collectors.toList());
        propertyNameList = propertyNameList.stream()
                .map(name -> "#{" + name + "}")
                .collect(Collectors.toList());
        columnNameList = columnNameList.stream().map(name -> "`" + name + "`").collect(Collectors.toList());
        return "insert into `" + getTableName() + "` (" + StringUtils.join(columnNameList, ", ") + ") values (" + StringUtils.join(propertyNameList, ", ") + ");";
    }

}
