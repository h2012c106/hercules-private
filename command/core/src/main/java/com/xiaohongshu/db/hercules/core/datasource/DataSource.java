package com.xiaohongshu.db.hercules.core.datasource;

public interface DataSource {

    public String name();

    public String getFilePositionParam();

    /**
     * 没人会用这个方法，可使用注入的serder组件是否为null判断同样的东西
     * @return
     */
    @Deprecated
    default public boolean hasKvSerDer() {
        return false;
    }

}
