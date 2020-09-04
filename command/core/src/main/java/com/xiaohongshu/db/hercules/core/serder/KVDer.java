package com.xiaohongshu.db.hercules.core.serder;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.serder.SerDerOptionsConf.NOT_CONTAINS_KEY;

/**
 * 读进一列ser过的数据展开成有意义的数据
 *
 * @param <T>
 */
public abstract class KVDer<T> implements DataSourceRoleGetter {

    private static final Log LOG = LogFactory.getLog(KVDer.class);

    protected WrapperGetterFactory<T> wrapperGetterFactory;

    private String keyName;
    private String valueName;

    @Options(type = OptionsType.DER)
    private GenericOptions options;

    private long time = 0L;

    public KVDer(WrapperGetterFactory<T> wrapperGetterFactory) {
        this.wrapperGetterFactory = wrapperGetterFactory;
        HerculesContext.instance().inject(wrapperGetterFactory);
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.DER;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public void setValueName(String valueName) {
        this.valueName = valueName;
    }

    abstract protected List<MapWrapper> readValue(BaseWrapper<?> inValue) throws IOException, InterruptedException;

    public final List<HerculesWritable> read(HerculesWritable in) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        try {
            BaseWrapper<?> key = in.get(keyName);
            BaseWrapper<?> value = in.get(valueName);
            // 上游给一对kv，其中有一个值不存在，则认为这行没意义，不予处理
            if (key == null || value == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Meaningless deserialize row without non-null key [%s] or value [%s]: %s", keyName, valueName, in.toString()));
                }
                return null;
            } else {
                List<MapWrapper> valueMapList = readValue(value);
                // 说明这行der不解，直接跳过
                if (valueMapList == null) {
                    return null;
                }
                List<HerculesWritable> res = new LinkedList<>();
                for (MapWrapper valueMap : valueMapList) {
                    HerculesWritable out = new HerculesWritable();
                    for (Map.Entry<String, BaseWrapper<?>> derValue : valueMap.entrySet()) {
                        String columnName = derValue.getKey();
                        BaseWrapper<?> columnValue = derValue.getValue();
                        out.put(columnName, columnValue);
                    }
                    // 如果序列化类中不包含key列，则反序列化的时候需要把key信息一并写进行信息中
                    if (options.getBoolean(NOT_CONTAINS_KEY, false)) {
                        out.put(keyName, key);
                    }
                    res.add(out);
                }
                return res;
            }
        } finally {
            time += (System.currentTimeMillis() - startTime);
        }
    }

    protected void innerClose() throws IOException {
    }

    public final void close() throws IOException {
        long startTime = System.currentTimeMillis();
        innerClose();
        time += (System.currentTimeMillis() - startTime);
        LOG.info(String.format("Spent %.3fs on deserialize.", (double) time / 1000.0));
    }

    protected final WrapperGetter<T> getWrapperGetter(DataType dataType) {
        return wrapperGetterFactory.getWrapperGetter(dataType);
    }
}
