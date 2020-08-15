package com.xiaohongshu.db.hercules.core.option.optionsconf;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.BaseDataSourceOptionsConf.COLUMN_DELIMITER;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.COLUMN;

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
                .description(String.format("The table column name black list, delimited by %s. The constraint is stronger than '--%s'.", COLUMN_DELIMITER, COLUMN))
                .defaultStringValue(String.join(COLUMN_DELIMITER, DEFAULT_BLACK_COLUMN))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
    }
}
