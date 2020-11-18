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
import com.xiaohongshu.db.hercules.core.utils.ErrorLoggerUtils;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.context.InjectedClass;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesCounter;
import com.xiaohongshu.db.hercules.core.utils.counter.HerculesStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf.ALLOW_SKIP;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.serder.SerDerOptionsConf.NOT_CONTAINS_KEY;

/**
 * 读进一列ser过的数据展开成有意义的数据
 *
 * @param <T>
 */
public abstract class KVDer<T> implements DataSourceRoleGetter, InjectedClass {

    private static final Log LOG = LogFactory.getLog(KVDer.class);

    protected WrapperGetterFactory<T> wrapperGetterFactory;

    private String keyName;
    private String valueName;

    @Options(type = OptionsType.DER)
    private GenericOptions derOptions;

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    private static final String MISS_KV_TAG = "Der miss key/value";
    private static final String THROWN_BY_DER_TAG = "Thrown by der";
    private boolean allowSkip;
    private int seq = 0;

    public KVDer(WrapperGetterFactory<T> wrapperGetterFactory) {
        this.wrapperGetterFactory = wrapperGetterFactory;
        HerculesContext.instance().inject(wrapperGetterFactory);
    }

    @Override
    public void afterInject() {
        allowSkip = commonOptions.getBoolean(ALLOW_SKIP, false);
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
        HerculesStatus.setHerculesMapStatus(HerculesStatus.HerculesMapStatus.DESERIALIZING);
        long startTime = System.currentTimeMillis();
        try {
            BaseWrapper<?> key = in.get(keyName);
            BaseWrapper<?> value = in.get(valueName);
            // 上游给一对kv，其中有一个值不存在，则认为这行没意义，不予处理
            if (key == null || value == null) {
                if (allowSkip) {
                    ErrorLoggerUtils.add(MISS_KV_TAG, in, seq);
                    return null;
                } else {
                    throw new RuntimeException(String.format("Meaningless serialize row: %s", in.toString()));
                }
            } else {
                List<MapWrapper> valueMapList = readValue(value);
                // 说明这行der不解，直接跳过
                if (valueMapList == null) {
                    if (allowSkip) {
                        ErrorLoggerUtils.add(THROWN_BY_DER_TAG, in, seq);
                        return null;
                    } else {
                        throw new RuntimeException(String.format("Meaningless serialize row: %s", in.toString()));
                    }
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
                    if (derOptions.getBoolean(NOT_CONTAINS_KEY, false)) {
                        out.put(keyName, key);
                    }
                    res.add(out);
                }
                return res;
            }
        } finally {
            ++seq;
            HerculesStatus.add(null, HerculesCounter.DER_TIME, System.currentTimeMillis() - startTime);
        }
    }

    protected void innerClose() throws IOException {
    }

    public final void close() throws IOException {
        innerClose();
        LOG.info(String.format("Spent %s on deserialize.", HerculesStatus.getStrValue(HerculesCounter.DER_TIME)));
        try {
            ErrorLoggerUtils.print(MISS_KV_TAG, LOG);
            ErrorLoggerUtils.print(THROWN_BY_DER_TAG, LOG);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    protected final WrapperGetter<T> getWrapperGetter(DataType dataType) {
        return wrapperGetterFactory.getWrapperGetter(dataType);
    }
}
