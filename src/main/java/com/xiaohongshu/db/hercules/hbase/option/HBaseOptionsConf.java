package com.xiaohongshu.db.hercules.hbase.option;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf.COLUMN;
import static com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf.COLUMN_DELIMITER;

public final class HBaseOptionsConf extends BaseOptionsConf {

    public final static String HB_ZK_QUORUM="zookeeper-quorum";
    public final static String HB_ZK_PORT="zookeeper-port";
    public final static String TABLE="hbase-table";

    public final static String HIVE_METASTORE_URL="hive-metastore-url";
    public final static String HIVE_USER="hive-user";
    public final static String HIVE_PASSWD="hive-passwd";
    public final static String HIVE_TABLE ="hbase-table";

    public static final String ROW_KEY_COL_NAME = "rowkeycolname";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
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
                .name(COLUMN)
                .needArg(true)
                .necessary(true)
                .description(String.format("The table column name list, delimited by %s.", COLUMN_DELIMITER))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_USER)
                .needArg(true)
                .description("User name used to connect Hive metastore.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_PASSWD)
                .needArg(true)
                .description("Password use to connect Hive metastore")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_TABLE)
                .needArg(true)
                .description("The table name used to specify hive table. Default it would be the the same as HBase table.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_METASTORE_URL)
                .needArg(true)
                .description("JDBC url to connect hive metastore(mysql).")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        // 确保 hive 相关参数完整
        ParseUtils.validateDependency(options,
                HIVE_METASTORE_URL,
                null,
                Lists.newArrayList(HIVE_USER, HIVE_PASSWD, HIVE_TABLE),
                null);
    }

    @Override
    public void processOptions(GenericOptions options) {
        super.processOptions(options);

        String rowKeyCol = options.getString(HBaseOptionsConf.ROW_KEY_COL_NAME, null);
        if (rowKeyCol == null) {
            return;
        }
        Map<String, DataType> columnTypeMap = SchemaUtils.convert(options.getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null));
        columnTypeMap.put(rowKeyCol, BaseDataType.BYTES);
        options.set(BaseDataSourceOptionsConf.COLUMN_TYPE, SchemaUtils.convert(columnTypeMap).toJSONString());
    }
}
