package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * @param <T> 判断数据源类型的标准，例如sql为int，内部使用switch...case；mongo为Object，内部使用if...instance of
 */
public abstract class BaseSchemaFetcher<T> {

    private static final Log LOG = LogFactory.getLog(BaseSchemaFetcher.class);

    private GenericOptions options;
    private List<String> columnNameList;
    private StingyMap<String, DataType> columnTypeMap;

    public BaseSchemaFetcher(GenericOptions options) {
        this.options = options;
    }

    public GenericOptions getOptions() {
        return options;
    }

    abstract public DataSource getDataSource();

    abstract protected List<String> innerGetColumnNameList();

    /**
     * 出于对列名列表一致性的考虑，强烈建议在mapper阶段勿调此方法，使用通过Configuration传来的列名列表，保证全局仅取一次列名列表
     * 考虑这样一种情况：源端rdbms，在主进程起map job前，[a,b,c,d]，在主进程起map后，map进程初始化schema fetcher前，[a,b,c,d,e]，
     * 那么schema事实上并不会经过schema checker的核验，列的形态并不安全
     *
     * @return
     */
    public final List<String> getColumnNameList() {
        if (columnNameList == null) {
            columnNameList = innerGetColumnNameList();
            LOG.info("The column name list is: " + columnNameList.toString());
        }
        return columnNameList;
    }

    /**
     * 数据源类型到内部wrapper类型枚举值的转换
     *
     * @param standard
     * @return
     */
    abstract protected DataType convertType(T standard);

    /**
     * 获得每列的类型，配合{@link #convertType}使用
     *
     * @return
     */
    abstract protected StingyMap<String, DataType> innerGetColumnTypeMap();

    public final StingyMap<String, DataType> getColumnTypeMap() {
        if (columnTypeMap == null) {
            columnTypeMap = innerGetColumnTypeMap();
            LOG.info("The column type map is: " + columnTypeMap.toString());
        }
        return columnTypeMap;
    }
}
