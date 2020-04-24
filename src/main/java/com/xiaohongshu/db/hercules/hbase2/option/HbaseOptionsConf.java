package com.xiaohongshu.db.hercules.hbase2.option;

import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.List;

public class HbaseOptionsConf extends BaseDataSourceOptionsConf {

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
        return tmpList;
    }
}
