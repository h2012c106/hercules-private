package com.xiaohongshu.db.hercules.core.datatype;

public interface CustomDataTypeManager<I, O> {

    public boolean contains(String typeName);

    public CustomDataType<I, O> get(String name);

    public CustomDataType<I, O> getIgnoreCase(String name);

}
