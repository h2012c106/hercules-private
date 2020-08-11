package com.xiaohongshu.db.hercules.core.schema;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 仅用于从数据源fetch schema，且全局仅允许fetch一次
 */
public abstract class BaseSchemaFetcher<T extends DataTypeConverter<?, ?>> extends SchemaFetcher {

    private static final Log LOG = LogFactory.getLog(BaseSchemaFetcher.class);

    private final GenericOptions options;
    private final DataSourceRole role;

    public BaseSchemaFetcher(GenericOptions options) {
        this.options = options;
        this.role = options.getOptionsType().getRole();
    }

    protected GenericOptions getOptions() {
        return options;
    }

    abstract protected List<String> innerGetColumnNameList();

    private List<String> columnNameList;

    /**
     * 出于对列名列表一致性的考虑，强烈建议在mapper阶段勿调此方法，使用通过Configuration传来的列名列表，保证全局仅取一次列名列表
     * 考虑这样一种情况：源端rdbms，在主进程起map job前，[a,b,c,d]，在主进程起map后，map进程初始化schema fetcher前，[a,b,c,d,e]，
     * 那么schema事实上并不会经过schema checker的核验，列的形态并不安全
     * <p>
     * 仅允许negotiator调用
     *
     * @return
     */
    @Override
    final List<String> getColumnNameList() {
        if (columnNameList == null) {
            columnNameList = innerGetColumnNameList();
            LOG.info("Fetched column name list: " + columnNameList);
        }
        return columnNameList;
    }

    /**
     * 获得每列的类型
     *
     * @return
     */
    abstract protected Map<String, DataType> innerGetColumnTypeMap();

    private Map<String, DataType> columnTypeMap;

    /**
     * 仅允许negotiator调用
     *
     * @return
     */
    @Override
    final Map<String, DataType> getColumnTypeMap() {
        if (columnTypeMap == null) {
            columnTypeMap = innerGetColumnTypeMap();
            LOG.info("Fetched column type map: " + columnTypeMap);
        }
        return columnTypeMap;
    }

    /**
     * 获得表的索引组
     *
     * @return
     */
    protected List<Set<String>> innerGetIndexGroupList() {
        return Collections.emptyList();
    }

    private List<Set<String>> indexGroupList;

    /**
     * 仅允许negotiator调用
     *
     * @return
     */
    @Override
    final List<Set<String>> getIndexGroupList() {
        if (indexGroupList == null) {
            indexGroupList = innerGetIndexGroupList();
            LOG.info("Fetched index group list: " + indexGroupList);
        }
        return indexGroupList;
    }

    /**
     * 获得表的唯一键组
     *
     * @return
     */
    protected List<Set<String>> innerGetUniqueKeyGroupList() {
        return Collections.emptyList();
    }

    private List<Set<String>> uniqueKeyGroupList;

    /**
     * 仅允许negotiator调用
     *
     * @return
     */
    @Override
    final List<Set<String>> getUniqueKeyGroupList() {
        if (uniqueKeyGroupList == null) {
            uniqueKeyGroupList = innerGetUniqueKeyGroupList();
            LOG.info("Fetched unique key group list: " + uniqueKeyGroupList);
        }
        return uniqueKeyGroupList;
    }

}
