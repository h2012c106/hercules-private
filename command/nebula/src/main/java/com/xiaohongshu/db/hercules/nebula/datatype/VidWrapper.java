package com.xiaohongshu.db.hercules.nebula.datatype;

import com.xiaohongshu.db.hercules.core.serialize.wrapper.IntegerWrapper;

public class VidWrapper extends IntegerWrapper {
    public VidWrapper(Long value) {
        super(value);
        setType(VidCustomDataType.INSTANCE);
    }
}
