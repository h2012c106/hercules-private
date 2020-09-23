package com.xiaohongshu.db.hercules.myhub.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.myhub.MyhubUtils;
import com.xiaohongshu.db.hercules.mysql.schema.MysqlSchemaFetcher;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MyhubSchemaFetcher extends MysqlSchemaFetcher implements InjectedClass {

    private static final Log LOG = LogFactory.getLog(MyhubSchemaFetcher.class);

    private boolean isShard;

    @Assembly
    private RDBMSManager manager;

    public MyhubSchemaFetcher(GenericOptions options) {
        super(options);
    }

    private void initializeIsShard(GenericOptions options, RDBMSManager manager) {
        try {
            this.isShard = MyhubUtils.isShard(options, manager);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterInject() {
        super.afterInject();
        initializeIsShard(getOptions(), manager);
    }

    @Override
    protected String getNoneLineSql(String baseSql) {
        if (isShard) {
            return "/*MYHUB SHARD_NODES:0; SLAVE_PREFER*/" + super.getNoneLineSql(baseSql);
        } else {
            return "/*SLAVE_PREFER*/" + super.getNoneLineSql(baseSql);
        }
    }

    @Override
    protected List<Set<String>> innerGetUniqueKeyGroupList() {
        LOG.info("Myhub's unique key is not reliable, return empty.");
        return Collections.emptyList();
    }
}
