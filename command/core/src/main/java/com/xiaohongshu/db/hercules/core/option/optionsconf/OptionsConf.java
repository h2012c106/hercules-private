package com.xiaohongshu.db.hercules.core.option.optionsconf;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;

import java.util.Map;

public interface OptionsConf {

    public Map<String, SingleOptionConf> getOptionsMap();

    public void validateOptions(GenericOptions options);

    public void processOptions(GenericOptions options);

}
