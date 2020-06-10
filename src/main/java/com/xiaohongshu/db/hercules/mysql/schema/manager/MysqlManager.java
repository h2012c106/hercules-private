package com.xiaohongshu.db.hercules.mysql.schema.manager;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.mysql.option.MysqlOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf.FETCH_SIZE;

public class MysqlManager extends RDBMSManager {

    private static final Log LOG = LogFactory.getLog(MysqlManager.class);

    private static final String ALLOW_ZERO_DATE_SQL = "set session sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';";

    public MysqlManager(GenericOptions options) {
        super(options);
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection res;
        if (options.getInteger(FETCH_SIZE, null) == null) {
            res = super.getConnection();
        } else {
            Properties properties = new Properties();
            properties.put("useCursorFetch", "true");
            res = getConnection(properties);
        }
        if (!options.getBoolean(MysqlOutputOptionsConf.ABANDON_ZERO_DATE, false)) {
            LOG.warn("To allow the zero timestamp, execute sql: " + ALLOW_ZERO_DATE_SQL);
            Statement statement = null;
            try {
                statement = res.createStatement();
                statement.execute(ALLOW_ZERO_DATE_SQL);
            } finally {
                SqlUtils.release(Lists.newArrayList(statement));
            }
        }
        return res;
    }

    @Override
    public String getRandomFunc() {
        return options.getString(RDBMSInputOptionsConf.RANDOM_FUNC_NAME, null);
    }
}
