package com.xiaohongshu.db.hercules.hbase.option;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.*;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.COLUMN;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.COLUMN_TYPE;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseDataSourceOptionsConf.COLUMN_DELIMITER;

public final class HBaseOptionsConf extends BaseOptionsConf {

    public final static String HB_ZK_QUORUM = "zookeeper-quorum";
    public final static String HB_ZK_PORT = "zookeeper-port";
    public final static String TABLE = "hbase-table";

    public final static String HIVE_METASTORE_URL = "hive-metastore-url";
    public final static String HIVE_USER = "hive-user";
    public final static String HIVE_PASSWD = "hive-passwd";
    public final static String HIVE_TABLE = "hive-table";

    private final static String VALUE_EMPTY = "$%^&HBASE_VALUE_EMPTY_VALUE&^%$";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return Lists.newArrayList(
                new KVOptionsConf()
        );
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(HB_ZK_QUORUM)
                .needArg(true)
                .necessary(true)
                .description("The zookeeper quorum.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HB_ZK_PORT)
                .needArg(true)
                .description("The zookeeper port.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(TABLE)
                .needArg(true)
                .necessary(true)
                .description("Job parameter that specifies the input table.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(VALUE_NAME)
                .needArg(true)
                .description("Specify the hbase column name, split by: " + COLUMN_DELIMITER)
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .defaultStringValue(VALUE_EMPTY)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(VALUE_TYPE)
                .needArg(true)
                .description("Specify the value type, formatted by json.")
                .build());
//        tmpList.add(SingleOptionConf.builder()
//                .name(HIVE_USER)
//                .needArg(true)
//                .description("User name used to connect Hive database.")
//                .build());
//        tmpList.add(SingleOptionConf.builder()
//                .name(HIVE_PASSWD)
//                .needArg(true)
//                .description("Password use to connect Hive database")
//                .build());
//        tmpList.add(SingleOptionConf.builder()
//                .name(HIVE_TABLE)
//                .needArg(true)
//                .description("The table name used to specify hive table. Default it would be the the same as HBase table.")
//                .build());
//        tmpList.add(SingleOptionConf.builder()
//                .name(HIVE_METASTORE_URL)
//                .needArg(true)
//                .description("JDBC url to connect hive metastore(mysql).")
//                .build());
        return tmpList;
    }

    @Override
    protected List<String> deleteOptions() {
        return null;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        // 确保 hive 相关参数完整
//        ParseUtils.validateDependency(options,
//                HIVE_METASTORE_URL,
//                null,
//                Lists.newArrayList(HIVE_USER, HIVE_PASSWD, HIVE_TABLE),
//                null);
    }

    @Override
    protected void innerProcessOptions(GenericOptions options) {
        String keyName = options.getString(KEY_NAME, null);
        // 覆写map，不然会导致用列表形状的value_name值做key，而value是json形状的value_type
        JSONObject columnType = new JSONObject();
        columnType.put(options.getString(KEY_NAME, null), BaseDataType.BYTES.name());
        // 因为KvOptionsConf validate不允许为空，但是这里可以为空，所以有占位符
        if (StringUtils.equals(options.getString(VALUE_NAME, null), VALUE_EMPTY)) {
            options.set(VALUE_NAME, "");
            // 只给了key也会导致抄不了，所以要让它抄就不能塞column
            options.set(COLUMN, new String[0]);
        } else {
            // HBase的value比较特殊，能设置成列表，由于是后代类，这个方法会覆写KVOptionsConf里的写column的值
            List<String> columnList = new LinkedList<>();
            columnList.add(keyName);
            // 其实这里可以直接插String，因为内部保存的时候已经是用内部分隔符保存了，而且比较挫没加转义之类的玩意，所以裸加也ok，但不能趁人之挫，挫上加挫
            columnList.addAll(Arrays.asList(options.getTrimmedStringArray(VALUE_NAME, new String[0])));
            options.set(COLUMN, columnList.toArray(new String[0]));

            JSONObject valueType = options.getJson(VALUE_TYPE, new JSONObject());
            columnType.putAll(valueType);
        }
        options.set(COLUMN_TYPE, columnType.toJSONString());
    }
}
