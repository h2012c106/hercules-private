package com.xiaohongshu.db.hercules.core.utils;

import com.xiaohongshu.db.hercules.core.DataSource;
import com.xiaohongshu.db.hercules.core.exceptions.ParseException;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author huanghanxiang
 */
public final class ParseUtils {

    private static final Log LOG = LogFactory.getLog(ParseUtils.class);

    private static final String DATA_SOURCE_SEPARATOR = "::";

    /**
     * 根据xxx->yyy获得xxx与yyy的枚举值
     *
     * @param arg
     * @return 一个长度为2的array，#0为source，#1为target
     */
    public static DataSource[] getDataSources(String arg) {
        String regex = String.format("^(.+?)%s(.+?)$", DATA_SOURCE_SEPARATOR);
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(arg);
        if (matcher.find()) {
            String sourceStr = matcher.group(1);
            String targetStr = matcher.group(2);
            return new DataSource[]{
                    DataSource.valueOfIgnoreCase(sourceStr),
                    DataSource.valueOfIgnoreCase(targetStr)
            };
        } else {
            throw new ParseException(String.format("Wrong definitive format, should be like 'xxx%syyy'", DATA_SOURCE_SEPARATOR));
        }
    }

    public static void assertTrue(boolean success, String errorMessage) {
        if (!success) {
            throw new ParseException(errorMessage);
        }
    }

    /**
     * 检查参数的依赖是否正确
     *
     * @param options
     * @param param   受检查的参数，为null时，代表根结点，用于判断某些必须存在的参数(这个可以通过required()约束)或某些只能取其一的参数
     * @param value   受检查的参数为某值时才进行约束，可以为null，代表任何值都进行约束
     * @param all     受检查参数存在时，必须同时存在的参数列表，e.g. --balance之于--balance-sampling-ratio
     * @param one     受检查参数存在时，只能同时存在有且仅有一个的参数列表，e.g. 没想好
     */
    public static void validateDependency(GenericOptions options, String param, String value,
                                          List<String> all, List<String> one) {
        // 当不包含本参数时，不用检查依赖
        if (param != null && !options.hasProperty(param)) {
            return;
        }
        // 当对参数的值有要求且当前值不符合要求时，不用检查依赖
        if (param != null && value != null && !StringUtils.equals(options.getString(param, null), value)) {
            return;
        }

        String paramName;
        if (param == null) {
            paramName = "Commandline";
        } else {
            if (value == null) {
                paramName = String.format("Param[%s]", param);
            } else {
                paramName = String.format("Param[%s=%s]", param, value);
            }
        }

        // 先检查all
        Set<String> missedParam = new HashSet<>();
        for (String neededParam : all == null ? new ArrayList<String>(0) : all) {
            if (!options.hasProperty(neededParam)) {
                missedParam.add(neededParam);
            }
        }
        if (missedParam.size() > 0) {
            throw new ParseException(String.format("%s miss param dependency: %s", paramName, missedParam.toString()));
        }

        // 再检查one
        if (one == null) {
            return;
        }
        Set<String> hasParam = new HashSet<>();
        for (String neededOnlyOnceParam : one) {
            if (options.hasProperty(neededOnlyOnceParam)) {
                hasParam.add(neededOnlyOnceParam);
            }
        }
        if (hasParam.size() < 1) {
            throw new ParseException(String.format("%s need at least one param dependency: %s", paramName, one));
        } else if (hasParam.size() > 1) {
            throw new ParseException(String.format("%s can only depend on at most one param dependency: %s", paramName, hasParam.toString()));
        }
    }

}
