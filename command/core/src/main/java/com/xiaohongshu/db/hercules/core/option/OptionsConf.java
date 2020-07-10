package com.xiaohongshu.db.hercules.core.option;

import java.util.Map;

public interface OptionsConf {

    public Map<String, SingleOptionConf> getOptionsMap();

    public void validateOptions(GenericOptions options);

    public void processOptions(GenericOptions options);

}
