package com.xiaohongshu.db.hercules.core.options;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 配置options，例如各个option名以及帮助命令行option的建立
 */
public abstract class BaseOptionsConf {

    public static final String HELP = "help";

    protected Map<String, SingleOptionConf> optionConfMap = new HashMap<>();

    public BaseOptionsConf() {
        for (SingleOptionConf conf : setOptionConf()) {
            optionConfMap.put(conf.getName(), conf);
        }
    }

    /**
     * 配置参数列表
     *
     * @return
     */
    abstract protected List<SingleOptionConf> setOptionConf();

    protected List<SingleOptionConf> clearOption(List<SingleOptionConf> confList, String confName) {
        return confList.stream()
                .filter(conf -> !conf.getName().equals(confName))
                .collect(Collectors.toList());
    }

    public Map<String, SingleOptionConf> getOptionsMap() {
        return optionConfMap;
    }

}
