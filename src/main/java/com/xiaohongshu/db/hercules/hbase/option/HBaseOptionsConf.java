package com.xiaohongshu.db.hercules.hbase.option;

import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;

import java.util.Arrays;
import java.util.List;

public class HBaseOptionsConf extends BaseDataSourceOptionsConf {

    public final static String HB_ZK_QUORUM="hbase.zookeeper.quorum";
    public final static String HB_ZK_PORT="hbase.zookeeper.port";

    @Override
    protected List<SingleOptionConf> setOptionConf() {
        List<SingleOptionConf> tmpList = super.setOptionConf();
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
                .name(COLUMN)
                .needArg(true)
                .necessary(true)
                .description(String.format("The table column name list, delimited by %s.", COLUMN_DELIMITER))
                .list(true)
                .listDelimiter(COLUMN_DELIMITER)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(COLUMN_TYPE)
                .needArg(true)
                .necessary(true)
                .description(String.format("The table column type map, formatted in json, type: %s.", Arrays.toString(DataType.values())))
                .defaultStringValue(DEFAULT_COLUMN_TYPE.toJSONString())
                .build());
        return tmpList;
    }
}
