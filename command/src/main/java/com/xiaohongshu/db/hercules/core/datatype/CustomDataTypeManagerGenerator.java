package com.xiaohongshu.db.hercules.core.datatype;

public interface CustomDataTypeManagerGenerator {
    default BaseCustomDataTypeManager<?, ?> generateCustomDataTypeManager() {
        return NullCustomDataTypeManager.INSTANCE;
    }
}
