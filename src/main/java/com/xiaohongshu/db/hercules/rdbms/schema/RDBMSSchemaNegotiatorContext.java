package com.xiaohongshu.db.hercules.rdbms.schema;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaNegotiatorContext;
import com.xiaohongshu.db.hercules.rdbms.option.RDBMSOptionsConf;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RDBMSSchemaNegotiatorContext extends BaseSchemaNegotiatorContext {

    private static final Log LOG = LogFactory.getLog(RDBMSSchemaNegotiatorContext.class);

    public RDBMSSchemaNegotiatorContext(GenericOptions options) {
        super(options);
    }

    @Override
    public void afterReadColumnNameList(List<String> columnName) {
        // 清理autoincrement字段
        if (getOptions().getBoolean(RDBMSOptionsConf.IGNORE_AUTOINCREMENT, false)) {
            RDBMSSchemaFetcher schemaFetcher = (RDBMSSchemaFetcher) getSchemaFetcher();
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
