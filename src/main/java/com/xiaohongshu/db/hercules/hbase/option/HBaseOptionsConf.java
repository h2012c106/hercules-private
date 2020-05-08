package com.xiaohongshu.db.hercules.hbase.option;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

import static com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf.COLUMN;
import static com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf.COLUMN_DELIMITER;

public final class HBaseOptionsConf extends BaseOptionsConf {

    private static final Log LOG = LogFactory.getLog(HBaseOptionsConf.class);
    public final static String HB_ZK_QUORUM="hbase.zookeeper.quorum";
    public final static String HB_ZK_PORT="hbase.zookeeper.port";
    public final static String TABLE="hbase.table";

    public final static String HIVE_URL="hbase.hive.url";
    public final static String HIVE_USER="hbase.hive.user";
    public final static String HIVE_PASSWD="hbase.hive.passwd";
    public final static String HIVE_Table="hbase.hive.table";
    public final static String HBASE_COLUMN_TYPE_MAP="hbase.column.type.map";

    /**
     * hbase 测需要维护自己的类型，同时保证框架测的类型符合要求。
     */
    @Override
    public void innerProcessOptions(GenericOptions options) {
        JSONObject columnTypeJson = options.getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, null);
        for(String key:columnTypeJson.keySet()){
            String dataType = columnTypeJson.getString(key);
            switch(dataType.toLowerCase()){
                case "short":
                case "int":
                case "long":
                    dataType = "INTEGER";
                    break;
                case "float":
                case "double":
                case "bigdecimal":
                    dataType = "DOUBLE";
                    break;
                default:
            }
            columnTypeJson.fluentPut(key, dataType);
        }
        options.set(HBASE_COLUMN_TYPE_MAP, options.getString(BaseDataSourceOptionsConf.COLUMN_TYPE,null));
        options.set(BaseDataSourceOptionsConf.COLUMN_TYPE, columnTypeJson.toString());
    }

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
                .name(HIVE_URL)
                .needArg(true)
                .description("The hive table to extract schema for HBase.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_USER)
                .needArg(true)
                .description("User name used to connect Hive database.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_PASSWD)
                .needArg(true)
                .description("Password use to connect Hive database")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HIVE_Table)
                .needArg(true)
                .description("The table name used to specify hive table. Default it would be the the same as HBase table.")
                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {

    }
}
