package com.xiaohongshu.db.hercules.mysql.schema;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.xiaohongshu.db.hercules.mysql.option.MysqlOptionsConf.IGNORE_AUTOINCREMENT;

public class MysqlSchemaNegotiatorContext extends BaseSchemaNegotiatorContext {

    private static final Log LOG = LogFactory.getLog(MysqlSchemaNegotiatorContext.class);

    @Assembly
    private MysqlSchemaFetcher schemaFetcher;

    public MysqlSchemaNegotiatorContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void afterReadColumnNameList(List<String> columnName) {
        // 清理autoincrement字段
        if (getOptions().getBoolean(IGNORE_AUTOINCREMENT, false)) {
            Set<String> removeSet = new HashSet<>(1);
            for (String autoincrement : schemaFetcher.getAutoincrementColumn()) {
                for (String column : columnName) {
                    if (StringUtils.equalsIgnoreCase(column, autoincrement)) {
                        removeSet.add(column);
                        break;
                    }
                }
            }
            LOG.warn(String.format("Column(s) [%s] will not be covered as it's an autoincrement column.", removeSet));
            columnName.removeAll(removeSet);
        }
    }
}
