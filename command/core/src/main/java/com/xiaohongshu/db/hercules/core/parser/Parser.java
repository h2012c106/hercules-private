package com.xiaohongshu.db.hercules.core.parser;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;

/**
 * @param <T> 用户入参形式
 */
public interface Parser<T> {

    public GenericOptions parse(T input);

}
