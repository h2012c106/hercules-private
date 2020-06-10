package com.xiaohongshu.db.hercules.core.schema;

import com.xiaohongshu.db.hercules.core.datatype.BaseCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.datatype.NullCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.xml.crypto.Data;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 仅用于从数据源fetch schema，且全局仅允许fetch一次
 */
public abstract class BaseSchemaFetcher<T extends DataTypeConverter<?, ?>> {

    private static final Log LOG = LogFactory.getLog(BaseSchemaFetcher.class);

    private GenericOptions options;
    private List<String> columnNameList;
    private Map<String, DataType> columnTypeMap;
    protected T converter;
    protected BaseCustomDataTypeManager customDataTypeManager;

    public BaseSchemaFetcher(GenericOptions options, T converter) {
        this(options, converter, NullCustomDataTypeManager.INSTANCE);
    }

    public BaseSchemaFetcher(GenericOptions options, T converter,
                             BaseCustomDataTypeManager customDataTypeManager) {
        this.options = options;
        this.converter = converter;
        this.customDataTypeManager = customDataTypeManager;
    }

    public BaseCustomDataTypeManager getCustomDataTypeManager() {
        return customDataTypeManager;
    }

    protected GenericOptions getOptions() {
        return options;
    }

    abstract protected List<String> innerGetColumnNameList();

    /**
     * 出于对列名列表一致性的考虑，强烈建议在mapper阶段勿调此方法，使用通过Configuration传来的列名列表，保证全局仅取一次列名列表
     * 考虑这样一种情况：源端rdbms，在主进程起map job前，[a,b,c,d]，在主进程起map后，map进程初始化schema fetcher前，[a,b,c,d,e]，
     * 那么schema事实上并不会经过schema checker的核验，列的形态并不安全
     * <p>
     * 仅允许negotiator调用
     *
     * @return
     */
    final List<String> getColumnNameList() {
        if (columnNameList == null) {
            columnNameList = innerGetColumnNameList();
        }
        return columnNameList;
    }

    /**
     * 获得每列的类型
     *
     * @return
     */
    abstract protected Map<String, DataType> innerGetColumnTypeMap(Set<String> columnNameSet);

    /**
     * 仅允许negotiator调用
     *
     * @param columnNameSet
     * @return
     */
    final Map<String, DataType> getColumnTypeMap(Set<String> columnNameSet) {
        if (columnTypeMap == null) {
            columnTypeMap = innerGetColumnTypeMap(columnNameSet);
        }
        return columnTypeMap;
    }

    /**
     * 首先，需要同步的列一定需要试图取类型。其次还可能存在一些不需要同步，但是同样需要取值的列（如split-by、update-key）也需要类型。
     *
     * @return
     */
    protected Set<String> getAdditionalNeedTypeColumn() {
        return new HashSet<>(0);
    }
}
