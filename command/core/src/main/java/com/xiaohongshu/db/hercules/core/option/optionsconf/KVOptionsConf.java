package com.xiaohongshu.db.hercules.core.option.optionsconf;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.*;

public class KVOptionsConf extends BaseOptionsConf {

    public static final String KEY_NAME = "key-name";
    public static final String VALUE_NAME = "value-name";
    public static final String KEY_TYPE = "key-type";
    public static final String VALUE_TYPE = "value-type";

    public final static String SERDER_SUPPLIER = "serder-supplier";

    public static final String DEFAULT_KEY_COL_NAME = "HERCULES_KEY_COL_NAME";
    public static final String DEFAULT_VALUE_COL_NAME = "HERCULES_VAL_COL_NAME";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(KEY_NAME)
                .needArg(true)
                .description(String.format("Because hercules stores everything as a table, please specify a name as the key column name, default to [%s].", DEFAULT_KEY_COL_NAME))
                .defaultStringValue(DEFAULT_KEY_COL_NAME)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(VALUE_NAME)
                .needArg(true)
                .description(String.format("Because hercules stores everything as a table, please specify a name as the value column name, default to [%s].", DEFAULT_VALUE_COL_NAME))
                .defaultStringValue(DEFAULT_VALUE_COL_NAME)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(KEY_TYPE)
                .needArg(true)
                .description(String.format("Key type: %s.", Arrays.toString(BaseDataType.values())))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(VALUE_TYPE)
                .needArg(true)
                .description(String.format("Value type: %s.", Arrays.toString(BaseDataType.values())))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SERDER_SUPPLIER)
                .needArg(true)
                .description("The supplier class path to provide key value converter and options.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        ParseUtils.assertTrue(!StringUtils.isEmpty(options.getString(KEY_NAME, null)),
                "The key name must not be empty.");
        ParseUtils.assertTrue(!StringUtils.isEmpty(options.getString(VALUE_NAME, null)),
                "The value name must not be empty.");
    }

    @Override
    protected void innerProcessOptions(GenericOptions options) {
        // 面儿上看起来是kv的形式，但是在schema negotiate的时候还得老老实实按照table的形式来，不然乱套了，所以还是把信息拼成一张table
        String keyName = options.getString(KEY_NAME, null);
        String valueName = options.getString(VALUE_NAME, null);

        options.set(COLUMN, new String[]{keyName, valueName});

        JSONObject json = new JSONObject();
        if (options.hasProperty(KEY_TYPE)) {
            json.put(keyName, options.getString(KEY_TYPE, null));
        }
        if (options.hasProperty(VALUE_TYPE)) {
            json.put(valueName, options.getString(VALUE_TYPE, null));
        }
        options.set(COLUMN_TYPE, json.toJSONString());

        options.set(INDEX, keyName);
        options.set(UNIQUE_KEY, keyName);
    }
}
