package com.xiaohongshu.db.hercules.core.serder;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.WritableUtils;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.serder.SerDerOptionsConf.NOT_CONTAINS_KEY;

/**
 * 从Mapper出来将结构化数据写成一列值的东西
 *
 * @param <T>
 */
public abstract class KVSer<T> implements DataSourceRoleGetter {

    private static final Log LOG = LogFactory.getLog(KVSer.class);

    protected WrapperSetterFactory<T> wrapperSetterFactory;

    private String keyName;
    private String valueName;

    @Options(type = OptionsType.SER)
    private GenericOptions options;

    public KVSer(WrapperSetterFactory<T> wrapperSetterFactory) {
        this.wrapperSetterFactory = wrapperSetterFactory;
        HerculesContext.instance().inject(wrapperSetterFactory);
    }

    @Override
    public final DataSourceRole getRole() {
        return DataSourceRole.SER;
    }

    public void setKeyName(String keyName) {
        this.keyName = keyName;
    }

    public void setValueName(String valueName) {
        this.valueName = valueName;
    }

    /**
     * 默认实现是从原来的行当中拿出一列作为key值，但是可以用子类复写，比如直接提供一个当前线程号+时间戳的key
     * 只有写的时候有可能有这种自造数据的需求（如kafka平衡?)，读的时候不需要，反正需要了再说，加起来由于是继承的方式，很简单
     *
     * @param in
     * @return
     */
    protected BaseWrapper<?> writeKey(HerculesWritable in) throws IOException, InterruptedException {
        return in.get(keyName);
    }

    abstract protected BaseWrapper<?> writeValue(HerculesWritable in) throws IOException, InterruptedException;

    public final HerculesWritable write(HerculesWritable in) throws IOException, InterruptedException {
        HerculesWritable out = new HerculesWritable(2);
        BaseWrapper<?> key = writeKey(in);
        // 如果序列化结构中不需要包含key值，则从行中拿走这列再序列化
        if (options.getBoolean(NOT_CONTAINS_KEY, false)) {
            WritableUtils.remove(in.getRow(), keyName);
        }
        BaseWrapper<?> value = writeValue(in);
        // 转出一对kv，其中有一个值不存在，则认为这行没意义，不予写下游
        if (key == null || value == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Meaningless row: %s", in.toString()));
            }
            return null;
        } else {
            out.put(keyName, key);
            out.put(valueName, value);
            return out;
        }
    }

    protected final WrapperSetter<T> getWrapperSetter(DataType dataType) {
        return wrapperSetterFactory.getWrapperSetter(dataType);
    }
}