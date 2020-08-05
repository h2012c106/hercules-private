package com.xiaohongshu.db.hercules.core.serializer;

import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetterFactory;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetterFactory;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.utils.context.HerculesContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 处理KV型数据尤其是V的解析与打包，如CanalEntry等。
 * @param <I>
 * @param <O>
 */
public abstract class KvSerializer<I, O> {

    private static final Log LOG = LogFactory.getLog(KvSerializer.class);

    protected WrapperGetterFactory<I> wrapperGetterFactory;
    protected WrapperSetterFactory<O> wrapperSetterFactory;
    protected WrappingOptions options;

    public KvSerializer(WrapperGetterFactory<I> wrapperGetterFactory, WrapperSetterFactory<O> wrapperSetterFactory) {
        this.wrapperGetterFactory = wrapperGetterFactory;
        this.wrapperSetterFactory = wrapperSetterFactory;
        this.options = HerculesContext.getWrappingOptions();
    }

    abstract protected void readKey(BaseWrapper<?> inKey, HerculesWritable out);

    abstract protected void readValue(BaseWrapper<?> inValue, HerculesWritable out);

    abstract protected String getReadKeyColumnName();

    abstract protected String getReadValueColumnName();

    public final HerculesWritable read(HerculesWritable in) {
        HerculesWritable out = new HerculesWritable();
        // TODO 搞个kvreader里把key和value列在writable里的列名搞成常量
        String keyName = getReadKeyColumnName();
        String valueName = getReadValueColumnName();
        BaseWrapper<?> key = in.get(keyName);
        BaseWrapper<?> value = in.get(valueName);
        // 上游给一对kv，其中有一个值不存在，则认为这行没意义，不予处理
        if (key == null || value == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Meaningless row without non-null key [%s] or value [%s]: %s", keyName, valueName, in.toString()));
            }
        } else {
            readKey(key, out);
            readValue(value, out);
        }
        return out;
    }

    abstract protected BaseWrapper<?> writeKey(HerculesWritable in);

    abstract protected BaseWrapper<?> writeValue(HerculesWritable in);

    abstract protected String getWriteKeyColumnName();

    abstract protected String getWriteValueColumnName();

    public final HerculesWritable write(HerculesWritable in) {
        HerculesWritable out = new HerculesWritable(2);
        BaseWrapper<?> key = writeKey(in);
        BaseWrapper<?> value = writeValue(in);
        String keyName = getWriteKeyColumnName();
        String valueName = getWriteValueColumnName();
        // 转出一对kv，其中有一个值不存在，则认为这行没意义，不予写下游
        if (key == null || value == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Meaningless row: %s", in.toString()));
            }
        } else {
            out.put(keyName, key);
            out.put(valueName, value);
        }
        return out;
    }

    protected final WrapperGetter<I> getWrapperGetter(DataType dataType) {
        return wrapperGetterFactory.getWrapperGetter(dataType);
    }

    protected final WrapperSetter<O> getWrapperSetter(DataType dataType) {
        return wrapperSetterFactory.getWrapperSetter(dataType);
    }
}
