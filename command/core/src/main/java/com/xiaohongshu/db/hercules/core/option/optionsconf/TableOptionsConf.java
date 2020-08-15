package com.xiaohongshu.db.hercules.core.option.optionsconf;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.BaseDataSourceOptionsConf.COLUMN_DELIMITER;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.BaseDataSourceOptionsConf.GROUP_DELIMITER;

public class TableOptionsConf extends BaseOptionsConf {

    public static final String COLUMN = "column";
    public static final String COLUMN_TYPE = "column-type";
    public static final String INDEX = "index";
    public static final String UNIQUE_KEY = "unique-key";

    private final static JSONObject DEFAULT_COLUMN_TYPE = new JSONObject();

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(COLUMN)
                .needArg(true)
                .description(String.format("The table column name list, delimited by %s.", COLUMN_DELIMITER))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(COLUMN_TYPE)
                .needArg(true)
                .description(String.format("The table column type map, formatted in json, type: %s.", Arrays.toString(BaseDataType.values())))
                .defaultStringValue(DEFAULT_COLUMN_TYPE.toJSONString())
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(INDEX)
                .needArg(true)
                .description(String.format("The table index group list, group separated by: %s, column delimited by: %s.", GROUP_DELIMITER, COLUMN_DELIMITER))
                .defaultStringValue("")
                .list(true)
                .listDelimiter(GROUP_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(UNIQUE_KEY)
                .needArg(true)
                .description(String.format("The table unique key group list, group separated by: %s, column delimited by: %s.", GROUP_DELIMITER, COLUMN_DELIMITER))
                .defaultStringValue("")
                .list(true)
                .listDelimiter(GROUP_DELIMITER)
                .build());
        return tmpList;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {

    }
}
