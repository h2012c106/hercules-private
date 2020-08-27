package com.xiaohongshu.db.hercules.manager.utils;

import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.parser.BaseParser;
import com.xiaohongshu.db.hercules.core.parser.ParserFactory;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;
import com.xiaohongshu.db.hercules.manager.parser.MapParser;
import com.xiaohongshu.db.share.entity.Job;
import com.xiaohongshu.db.share.utils.Constant;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * 瞎几把写的，在重构hercules-command的时候需要改
 */
public final class CommandGenerator {

    private static final String JOB_NAME_PREFIX = "hercules";

    private static void validateTwoMapNoIntersect(Map<String, ?> map, Map<String, ?> mbp) {
        Set<String> tmpSet = new HashSet<>(map.keySet());
        tmpSet.retainAll(mbp.keySet());
        if (tmpSet.size() > 0) {
            throw new RuntimeException("Two map has public key(s): " + tmpSet);
        }
    }

    private static Map<String, String> mergeParamMap(String paramType, Map<String, String> map, Map<String, String> templateMap, Map<String, List<String>> templateValueMap) {
        // 不允许一个param既是final的又是template的，正常使用前端不该出现这种问题
        validateTwoMapNoIntersect(map, templateMap);

        Map<String, String> res = new LinkedHashMap<>(map.size() + templateMap.size());

        res.putAll(map);

        for (Map.Entry<String, String> entry : templateMap.entrySet()) {
            String paramName = entry.getKey();
            String template = entry.getValue();
            List<String> templateValue = templateValueMap.get(paramName);
            if (templateValue == null) {
                throw new RuntimeException(String.format("The template param [%s] of %s miss value: %s", paramName, paramType, template));
            }
            String s;
            try {
                s = String.format(template, templateValue.toArray(new Object[0]));
            } catch (MissingFormatArgumentException e) {
                throw new RuntimeException(String.format("Missing param [%s] of %s template [%s] args, now has [%d] arg(s).", paramName, paramType, template, templateValue.size()), e);
            }
            res.put(paramName, s);
        }

        // 清除null值
        res.values().removeIf(Objects::isNull);
        return res;
    }

    private static String generateJobName(Job job) {
        List<String> name = new LinkedList<>();
        name.add(JOB_NAME_PREFIX);
        name.add(job.getSource().name());
        name.add(job.getTarget().name());
        name.add(job.getName());
        return StringUtils.join(name, "_");
    }

    private static Map<String, Map<String, String>> mergeMap(Job job,
                                                             Map<String, List<String>> sourceTemplateMap, Map<String, List<String>> targetTemplateMap,
                                                             Map<String, List<String>> commonTemplateMap, Map<String, List<String>> dTemplateMap) {
        Map<String, Map<String, String>> res = new HashMap<>();
        res.put(Constant.D_PARAM_MAP_KEY, mergeParamMap(Constant.D_PARAM_MAP_KEY, job.getDParam(), job.getDTemplateParam(), dTemplateMap));
        res.put(Constant.SOURCE_PARAM_MAP_KEY, mergeParamMap(Constant.SOURCE_PARAM_MAP_KEY, job.getSourceParam(), job.getSourceTemplateParam(), sourceTemplateMap));
        res.put(Constant.TARGET_PARAM_MAP_KEY, mergeParamMap(Constant.TARGET_PARAM_MAP_KEY, job.getTargetParam(), job.getTargetTemplateParam(), targetTemplateMap));
        res.put(Constant.COMMON_PARAM_MAP_KEY, mergeParamMap(Constant.COMMON_PARAM_MAP_KEY, job.getCommonParam(), job.getCommonTemplateParam(), commonTemplateMap));
        return res;
    }

    public static String generateHerculesCommand(Job job,
                                                 @NonNull Map<String, List<String>> sourceTemplateMap, @NonNull Map<String, List<String>> targetTemplateMap,
                                                 @NonNull Map<String, List<String>> commonTemplateMap, @NonNull Map<String, List<String>> dTemplateMap) {
        Map<String, Map<String, String>> mergedMap = CommandGenerator.mergeMap(job, sourceTemplateMap, targetTemplateMap, commonTemplateMap, dTemplateMap);

        MapParser parser;
        // 检查common参数
        parser = new MapParser(new CommonOptionsConf(), null, null);
        parser.parse(mergedMap);
        // 检查source参数
        DataSource sourceDataSource = job.getSource();
        BaseOptionsConf sourceOptionsConf = ParserFactory.getParser(sourceDataSource, DataSourceRole.SOURCE).getOptionsConf();
        parser = new MapParser(sourceOptionsConf, sourceDataSource, DataSourceRole.SOURCE);
        parser.parse(mergedMap);
        // 检查target参数
        DataSource targetDataSource = job.getTarget();
        BaseOptionsConf targetOptionsConf = ParserFactory.getParser(targetDataSource, DataSourceRole.TARGET).getOptionsConf();
        parser = new MapParser(targetOptionsConf, targetDataSource, DataSourceRole.TARGET);
        parser.parse(mergedMap);

        List<String> command = new LinkedList<>();
        command.add("hercules");
        command.add(job.getSource().name() + ParseUtils.DATA_SOURCE_SEPARATOR + job.getTarget().name());
        Map<String, String> paramMap;
        // set -D参数
        paramMap = mergedMap.get(Constant.D_PARAM_MAP_KEY);
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            command.add("-D" + entry.getKey() + "=" + entry.getValue());
        }
        // set source参数
        paramMap = mergedMap.get(Constant.SOURCE_PARAM_MAP_KEY);
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            command.add("--" + BaseParser.SOURCE_OPTIONS_PREFIX + entry.getKey());
            if (sourceOptionsConf.getOptionsMap().get(entry.getKey()).isNeedArg()) {
                command.add("'" + StringUtils.replace(entry.getValue(), "'", "'\"'\"'") + "'");
            }
        }
        // set target参数
        paramMap = mergedMap.get(Constant.TARGET_PARAM_MAP_KEY);
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            command.add("--" + BaseParser.TARGET_OPTIONS_PREFIX + entry.getKey());
            if (targetOptionsConf.getOptionsMap().get(entry.getKey()).isNeedArg()) {
                command.add("'" + StringUtils.replace(entry.getValue(), "'", "'\"'\"'") + "'");
            }
        }
        // set common参数
        paramMap = mergedMap.get(Constant.COMMON_PARAM_MAP_KEY);
        // set job name
        paramMap.put(CommonOptionsConf.JOB_NAME, generateJobName(job));
        for (Map.Entry<String, String> entry : paramMap.entrySet()) {
            command.add("--" + entry.getKey());
            if (new CommonOptionsConf().getOptionsMap().get(entry.getKey()).isNeedArg()) {
                command.add("'" + StringUtils.replace(entry.getValue(), "'", "'\"'\"'") + "'");
            }
        }
        return StringUtils.join(command, " ");
    }

}
