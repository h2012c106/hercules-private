package com.xiaohongshu.db.hercules.core.datatype;

import java.util.Collections;
import java.util.List;

public class NullCustomDataTypeManager extends BaseCustomDataTypeManager<Void, Void> {

    public static final NullCustomDataTypeManager INSTANCE = new NullCustomDataTypeManager();

    private NullCustomDataTypeManager() {
    }

    @Override
    protected List<Class<? extends CustomDataType<Void, Void, ?>>> generateTypeList() {
        return Collections.emptyList();
    }
}
