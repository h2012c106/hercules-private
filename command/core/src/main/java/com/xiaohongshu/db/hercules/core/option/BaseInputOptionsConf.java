package com.xiaohongshu.db.hercules.core.option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class BaseInputOptionsConf extends BaseOptionsConf {

    public static final String BLACK_COLUMN = "black-column";

    private final static String[] DEFAULT_BLACK_COLUMN = new String[0];

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Collections.singletonList(new BaseDataSourceOptionsConf());
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(BLACK_COLUMN)
                .needArg(true)
                .description(String.format("The table column name black list, delimited by %s. The constraint is stronger than '--%s'.", BaseDataSourceOptionsConf.COLUMN_DELIMITER, BaseDataSourceOptionsConf.COLUMN))
                .defaultStringValue(String.join(BaseDataSourceOptionsConf.COLUMN_DELIMITER, DEFAULT_BLACK_COLUMN))
                .list(true)
                .listDelimiter(BaseDataSourceOptionsConf.COLUMN_DELIMITER)
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
