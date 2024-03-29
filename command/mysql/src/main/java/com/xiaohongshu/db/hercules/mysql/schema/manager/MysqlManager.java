package com.xiaohongshu.db.hercules.mysql.schema.manager;

import com.google.common.collect.Lists;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.mysql.option.MysqlOutputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSInputOptionsConf;
import com.xiaohongshu.db.hercules.rdbms.schema.SqlUtils;
import com.xiaohongshu.db.hercules.rdbms.schema.manager.RDBMSManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf.JOB_NAME;

public class MysqlManager extends RDBMSManager {

    private static final Log LOG = LogFactory.getLog(MysqlManager.class);

    private static final String TRACK_ID_PREFIX = "hercules-task";

    protected static final String ALLOW_ZERO_DATE_SQL = "set session sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';";

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    public MysqlManager(GenericOptions options) {
        super(options);
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection res;
        if (options.getOptionsType().isSource()
                && options.getInteger(RDBMSInputOptionsConf.FETCH_SIZE, null) == null) {
            res = super.getConnection();
        } else {
            Properties properties = new Properties();
            properties.put("useCursorFetch", "true");
            res = getConnection(properties);
        }
        if (options.getOptionsType().isTarget()
                && options.getBoolean(MysqlOutputOptionsConf.ALLOW_ZERO_DATE, false)) {
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

    @Override
    public String makeBaseQuery(boolean useAsterisk) {
        String jobName = commonOptions.getString(JOB_NAME, null);
        String prefix = TRACK_ID_PREFIX;
        if (!StringUtils.isEmpty(jobName)) {
            // 去掉危险字符
            jobName = jobName.replaceAll("[^(A-Za-z0-9_\\-)]", "");
        } else {
            jobName = "ANONYMOUS";
        }
        prefix += ("." + jobName);
        return "/*TRACE CAT_ID:\"" + prefix + "\"*/" + super.makeBaseQuery(useAsterisk);
    }
}
