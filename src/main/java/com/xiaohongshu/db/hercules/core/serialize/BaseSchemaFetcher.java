package com.xiaohongshu.db.hercules.core.serialize;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.serialize.datatype.DataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.Map;

/**
 * @param <T> 判断数据源类型的标准，例如sql为int，内部使用switch...case；mongo为Object，内部使用if...instance of
 */
public abstract class BaseSchemaFetcher<T> {

    private static final Log LOG = LogFactory.getLog(BaseSchemaFetcher.class);

    private GenericOptions options;
    private List<String> columnNameList;
    private Map<String, DataType> columnTypeMap;

    public BaseSchemaFetcher(GenericOptions options) {
        this.options = options;
    }

    public GenericOptions getOptions() {
        return options;
    }

    abstract public DataSource getDataSource();

    abstract protected List<String> innerGetColumnNameList();

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
    abstract protected Map<String, DataType> innerGetColumnTypeMap();

    public final Map<String, DataType> getColumnTypeMap() {
        if (columnTypeMap == null) {
            columnTypeMap = innerGetColumnTypeMap();
        }
        return columnTypeMap;
    }
}
