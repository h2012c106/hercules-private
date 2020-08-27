package com.xiaohongshu.db.hercules.manager.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.share.utils.Constant;

import java.util.*;

/**
 * TODO 丑陋！在重构完Parser和OptionsConf后再做打算
 */
public class MapParser {

    private BaseOptionsConf optionsConf;
    private DataSource dataSource;
    private DataSourceRole dataSourceRole;

    public MapParser(BaseOptionsConf optionsConf, DataSource dataSource, DataSourceRole dataSourceRole) {
        this.optionsConf = optionsConf;
        this.dataSource = dataSource;
        this.dataSourceRole = dataSourceRole;
    }

    private String getParamKey() {
        if (dataSourceRole == null) {
            return Constant.COMMON_PARAM_MAP_KEY;
        }
        switch (dataSourceRole) {
            case SOURCE:
                return Constant.SOURCE_PARAM_MAP_KEY;
            case TARGET:
                return Constant.TARGET_PARAM_MAP_KEY;
            default:
                throw new ParseException("Unknown data source role: " + dataSourceRole);
        }
    }

    private GenericOptions parseCommandLine(Map<String, String> args) {
        // 先检查有没有多的参数
        Set<String> argParamSet = new HashSet<>(args.keySet());
        argParamSet.removeAll(optionsConf.getOptionsMap().keySet());
        if (argParamSet.size() > 0) {
            throw new RuntimeException("Unknown param(s): " + argParamSet);
        }

        GenericOptions options = new GenericOptions();
        List<String> missedParamList = new LinkedList<>();
        for (Map.Entry<String, SingleOptionConf> entry : optionsConf.getOptionsMap().entrySet()) {
            String paramName = entry.getKey();
            SingleOptionConf optionConf = entry.getValue();

            String optionValue = null;
            if (optionConf.isNeedArg()) {
                // 如果未设置necessary且未设置default value，用户不指定此参数时，不会把这个参数塞到options里
                if (optionConf.isSetDefaultStringValue()) {
                    optionValue = args.getOrDefault(paramName, optionConf.getDefaultStringValue());
                } else {
                    if (args.containsKey(paramName)) {
                        optionValue = args.get(paramName);
                    }
                }
                if (!args.containsKey(paramName) && optionConf.isNecessary()) {
                    missedParamList.add(paramName);
                }
            } else {
                optionValue = Boolean.toString(args.containsKey(paramName));
            }

            if (optionValue == null) {
                continue;
            }
            if (optionConf.isList()) {
                options.set(paramName, optionValue.split(optionConf.getListDelimiter()));
            } else {
                options.set(paramName, optionValue);
            }
        }
        if (missedParamList.size() > 0) {
            throw new RuntimeException("Miss the param(s): " + missedParamList);
        }
        return options;
    }

    public final GenericOptions parse(Map<String, Map<String, String>> map) {
        Map<String, String> paramMap = map.get(getParamKey());
        GenericOptions options = parseCommandLine(paramMap);

        // validate
        try {
            optionsConf.validateOptions(options);
        } catch (Exception e) {
            throw new ParseException(e);
        }

        return options;
    }

}
