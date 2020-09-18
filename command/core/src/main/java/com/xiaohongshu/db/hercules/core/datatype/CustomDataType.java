package com.xiaohongshu.db.hercules.core.datatype;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.BaseTypeWrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.input.wrapper.WrapperGetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.BaseTypeWrapperSetter;
import com.xiaohongshu.db.hercules.core.mr.output.wrapper.WrapperSetter;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 定义特殊数据源类型
 * 1. 只要子类包含其他参数，就应当继承initialize方法。
 * 2. 必须包含公共无参构造函数。
 * 3. 不得和{@link BaseDataType}出现同名类型
 * TODO 以上两点在单元测试中检查
 */
public abstract class CustomDataType<I, O, T> implements DataType {

    private String name;
    private BaseDataType baseType;
    private Class<?> storageClass;
    private List<String> params;
    private final Function<Object, BaseWrapper<?>> readFunction;
    private final Function<BaseWrapper<?>, Object> writeFunction;
    private final WrapperGetter<I> wrapperGetter;
    private final WrapperSetter<O> wrapperSetter;

    protected CustomDataType(String name, BaseDataType baseType, Class<?> storageClass,
                             Function<Object, BaseWrapper<?>> readFunction) {
        this.name = name;
        this.baseType = baseType;
        this.storageClass = storageClass;
        this.params = new ArrayList<>(0);
        this.readFunction = readFunction;
        this.writeFunction = new Function<BaseWrapper<?>, Object>() {
            @SneakyThrows
            @Override
            public Object apply(BaseWrapper<?> wrapper) {
                return write(wrapper);
            }
        };
        this.wrapperGetter = createWrapperGetter(this);
        this.wrapperSetter = createWrapperSetter(this);
    }

    abstract protected BaseTypeWrapperGetter<T, I> createWrapperGetter(final CustomDataType<I, O, T> self);

    abstract protected BaseTypeWrapperSetter<T, O> createWrapperSetter(final CustomDataType<I, O, T> self);

    public void initialize(List<String> params) {
        // 记录params
        this.params = params;
        innerInitialize(params);
    }

    protected void innerInitialize(List<String> params) {
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final BaseDataType getBaseDataType() {
        return baseType;
    }

    @Override
    public final Class<?> getStorageClass() {
        return storageClass;
    }

    @Override
    public final boolean isCustom() {
        return true;
    }

    public final T write(@NonNull BaseWrapper<?> wrapper) throws Exception {
        if (wrapper.getClass() == IntegerWrapper.class) {
            return innerWrite((IntegerWrapper) wrapper);
        } else if (wrapper.getClass() == BooleanWrapper.class) {
            return innerWrite((BooleanWrapper) wrapper);
        } else if (wrapper.getClass() == DoubleWrapper.class) {
            return innerWrite((DoubleWrapper) wrapper);
        } else if (wrapper.getClass() == StringWrapper.class) {
            return innerWrite((StringWrapper) wrapper);
        } else if (wrapper.getClass() == BytesWrapper.class) {
            return innerWrite((BytesWrapper) wrapper);
        } else if (wrapper.getClass() == DateWrapper.class) {
            return innerWrite((DateWrapper) wrapper);
        } else if (wrapper.getClass() == NullWrapper.class) {
            return innerWrite((NullWrapper) wrapper);
        } else if (wrapper.getClass() == ListWrapper.class) {
            return innerWrite((ListWrapper) wrapper);
        } else if (wrapper.getClass() == MapWrapper.class) {
            return innerWrite((MapWrapper) wrapper);
        } else {
            return innerSpecialWrite(wrapper);
        }
    }

    protected T innerWrite(IntegerWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerWrite(BooleanWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerWrite(DoubleWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerWrite(StringWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerWrite(BytesWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerWrite(DateWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerWrite(NullWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerWrite(ListWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerWrite(MapWrapper wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected T innerSpecialWrite(BaseWrapper<?> wrapper) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public final Function<Object, BaseWrapper<?>> getReadFunction() {
        return readFunction;
    }

    @Override
    public final Function<BaseWrapper<?>, Object> getWriteFunction() {
        return writeFunction;
    }

    public final WrapperGetter<I> getWrapperGetter() {
        return wrapperGetter;
    }

    public final WrapperSetter<O> getWrapperSetter() {
        return wrapperSetter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomDataType that = (CustomDataType) o;
        return Objects.equal(name, that.name) &&
                baseType == that.baseType &&
                Objects.equal(storageClass, that.storageClass) &&
                Objects.equal(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, baseType, storageClass, params);
    }

    /**
     * 若子类的某些变量具有默认值，则不可完全依照param toString。
     *
     * @return
     */
    @Override
    public String toString() {
        if (params.size() == 0) {
            return name;
        } else {
            return name + '(' + StringUtils.join(params, ',') + ')';
        }
    }
}
