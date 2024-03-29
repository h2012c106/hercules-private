package com.xiaohongshu.db.hercules.core.schema;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Schema {

    private List<String> columnNameList = null;
    private Map<String, DataType> columnTypeMap = null;
    private List<Set<String>> indexGroupList = null;
    private List<Set<String>> uniqueKeyGroupList = null;

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public Map<String, DataType> getColumnTypeMap() {
        return columnTypeMap;
    }

    public void setColumnTypeMap(Map<String, DataType> columnTypeMap) {
        this.columnTypeMap = columnTypeMap;
    }

    public List<Set<String>> getIndexGroupList() {
        return indexGroupList;
    }

    public void setIndexGroupList(List<Set<String>> indexGroupList) {
        this.indexGroupList = indexGroupList;
    }

    public List<Set<String>> getUniqueKeyGroupList() {
        return uniqueKeyGroupList;
    }

    public void setUniqueKeyGroupList(List<Set<String>> uniqueKeyGroupList) {
        this.uniqueKeyGroupList = uniqueKeyGroupList;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("columnNameList", columnNameList)
                .append("columnTypeMap", columnTypeMap)
                .append("indexGroupList", indexGroupList)
                .append("uniqueKeyGroupList", uniqueKeyGroupList)
                .toString();
    }

    public void toOptions(GenericOptions options) {
        options.set(TableOptionsConf.COLUMN, columnNameList.toArray(new String[0]));
        options.set(TableOptionsConf.COLUMN_TYPE, SchemaUtils.convertTypeToOption(columnTypeMap).toJSONString());
        options.set(TableOptionsConf.INDEX, SchemaUtils.convertIndexToOption(indexGroupList));
        options.set(TableOptionsConf.UNIQUE_KEY, SchemaUtils.convertIndexToOption(uniqueKeyGroupList));
    }

    public static Schema fromOptions(GenericOptions options, CustomDataTypeManager<?, ?> customDataTypeManager) {
        Schema res = new Schema();
        res.setColumnNameList(SchemaUtils.convertNameFromOption(options.getTrimmedStringArray(TableOptionsConf.COLUMN, new String[0])));
        res.setColumnTypeMap(SchemaUtils.convertTypeFromOption(options.getJson(TableOptionsConf.COLUMN_TYPE, new JSONObject()), customDataTypeManager));
        res.setIndexGroupList(SchemaUtils.convertIndexFromOption(options.getTrimmedStringArray(TableOptionsConf.INDEX, new String[0])));
        res.setUniqueKeyGroupList(SchemaUtils.convertIndexFromOption(options.getTrimmedStringArray(TableOptionsConf.UNIQUE_KEY, new String[0])));
        return res;
    }

}
