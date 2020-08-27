package com.xiaohongshu.db.hercules.core.datatype;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.*;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 定义特殊数据源类型
 * 1. 只要子类包含其他参数，就应当继承initialize方法。
 * 2. 必须包含公共无参构造函数。
 * 3. 不得和{@link BaseDataType}出现同名类型
 * TODO 以上两点在单元测试中检查
 *
 * @param <I> 读时数据源类型，如ResultSet
 * @param <O> 写时数据源类型，如PreparedStatement
 */
public abstract class CustomDataType<I, O> implements DataType {

    private String name;
    private BaseDataType baseType;
    private Class<?> storageClass;
    private List<String> params;

    protected CustomDataType(String name, BaseDataType baseType, Class<?> storageClass) {
        this.name = name;
        this.baseType = baseType;
        this.storageClass = storageClass;
        this.params = new ArrayList<>(0);
    }

    public void initialize(List<String> params) {
        // 记录params
        this.params = params;
        innerInitialize(params);
    }

    protected void innerInitialize(List<String> params) {
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BaseDataType getBaseDataType() {
        return baseType;
    }

    @Override
    public Class<?> getStorageClass() {
        return storageClass;
    }

    @Override
    public final boolean isCustom() {
        return true;
    }

    abstract public boolean isNull(I row, String rowName, String columnName, int columnSeq) throws Exception;

    /**
     * 确保下游抄类型作业能抄到特殊类型
     *
     * @param row
     * @param rowName
     * @param columnName
     * @param columnSeq
     * @return
     * @throws Exception
     */
    public final BaseWrapper<?> read(I row, String rowName, String columnName, int columnSeq) throws Exception {
        BaseWrapper<?> res = innerRead(row, rowName, columnName, columnSeq);
        res.setType(this);
        return res;
    }

    /**
     * 此处可以返回一个特殊wrapper，用于定义与众不同的各个as方法，当然也可以返回一个预定义好的wrapper
     *
     * @param row
     * @param rowName
     * @param columnName
     * @param columnSeq
     * @return
     * @throws Exception
     */
    abstract protected BaseWrapper<?> innerRead(I row, String rowName, String columnName, int columnSeq) throws Exception;

    abstract public void writeNull(O row, String rowName, String columnName, int columnSeq) throws Exception;

    public void write(@NonNull BaseWrapper<?> wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        if (wrapper.getClass() == IntegerWrapper.class) {
            innerWrite((IntegerWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else if (wrapper.getClass() == BooleanWrapper.class) {
            innerWrite((BooleanWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else if (wrapper.getClass() == DoubleWrapper.class) {
            innerWrite((DoubleWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else if (wrapper.getClass() == StringWrapper.class) {
            innerWrite((StringWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else if (wrapper.getClass() == BytesWrapper.class) {
            innerWrite((BytesWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else if (wrapper.getClass() == DateWrapper.class) {
            innerWrite((DateWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else if (wrapper.getClass() == NullWrapper.class) {
            innerWrite((NullWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else if (wrapper.getClass() == ListWrapper.class) {
            innerWrite((ListWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else if (wrapper.getClass() == MapWrapper.class) {
            innerWrite((MapWrapper) wrapper, row, rowName, columnName, columnSeq);
        } else {
            innerSpecialWrite(wrapper, row, rowName, columnName, columnSeq);
        }
    }

    protected void innerWrite(IntegerWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void innerWrite(BooleanWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void innerWrite(DoubleWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void innerWrite(StringWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void innerWrite(BytesWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void innerWrite(DateWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    abstract protected void innerWrite(NullWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception;

    protected void innerWrite(ListWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void innerWrite(MapWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected void innerSpecialWrite(BaseWrapper wrapper, O row, String rowName, String columnName, int columnSeq) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomDataType<?, ?> that = (CustomDataType<?, ?>) o;
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
