package com.xiaohongshu.db.hercules.common.option;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.utils.ParseUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.COLUMN;

/**
 * 千万不能出现以"source-"或"target-"开头的通用配置，大多数情况没问题，但是万一和源或目标端的参数碰了
 */
public class CommonOptionsConf extends BaseOptionsConf {

    public static final String NUM_MAPPER = "num-mapper";
    public static final String LOG_LEVEL = "log-level";
    public static final String JOB_NAME = "job-name";
    public static final String ALLOW_SOURCE_MORE_COLUMN = "allow-source-more-column";
    public static final String ALLOW_TARGET_MORE_COLUMN = "allow-target-more-column";
    public static final String COLUMN_MAP = "column-map";
    public static final String RELIABLE_COLUMN_MAP = "reliable-column-map";
    public static final String MAX_WRITE_QPS = "max-write-qps";
    public static final String ALLOW_COPY_COLUMN_NAME = "allow-copy-column-name";
    public static final String ALLOW_COPY_COLUMN_TYPE = "allow-copy-column-type";
    public static final String ALLOW_COPY_KEY = "allow-copy-key";

    public static final int DEFAULT_NUM_MAPPER = 4;
    public static final Level DEFAULT_LOG_LEVEL = Level.INFO;
    public static final JSONObject DEFAULT_COLUMN_MAP = new JSONObject();

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(NUM_MAPPER)
                .needArg(true)
                .description(String.format("The mapper num used to read & write, default to %d.", DEFAULT_NUM_MAPPER))
                .defaultStringValue(Integer.toString(DEFAULT_NUM_MAPPER))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(LOG_LEVEL)
                .needArg(true)
                .description(String.format("The log level (case sensitive): " +
                        "OFF / SEVERE / WARNING / INFO / CONFIG / FINE / FINER / FINEST / ALL, " +
                        "default to %s.", DEFAULT_LOG_LEVEL.toString()))
                .defaultStringValue(DEFAULT_LOG_LEVEL.toString())
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(JOB_NAME)
                .needArg(true)
                .necessary(false)
                .description("The mapReduce job name.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(ALLOW_SOURCE_MORE_COLUMN)
                .needArg(false)
                .description("If allow source datasource has more column, " +
                        "the additional column will be abandoned when reading, else, a exception will be thrown.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(ALLOW_TARGET_MORE_COLUMN)
                .needArg(false)
                .description("If allow target datasource has more column, " +
                        "the additional column will not be set when writing " +
                        "(in mysql case, the target mysql will use the default value instead), " +
                        "else, a exception will be thrown.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(COLUMN_MAP)
                .needArg(true)
                .description("The map between source columns and target columns, " +
                        "key is for source and value is for target. " +
                        "It will convert to a BiMap, make sure that either key or value cannot be duplicate.")
                .defaultStringValue(DEFAULT_COLUMN_MAP.toJSONString())
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(RELIABLE_COLUMN_MAP)
                .needArg(false)
                .description(String.format("If specified, '--%s''s key and value will add to source and target '--%s' param value.", COLUMN_MAP, COLUMN))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(MAX_WRITE_QPS)
                .needArg(true)
                .description(String.format("The OVERALL write qps limit applied to target data source, " +
                        "will automatically divide the '--%s' value to adapt the multi-map situation, " +
                        "it's unnecessary to calculate it according to different '--%s' value.", COLUMN_MAP, COLUMN_MAP))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(HELP)
                .needArg(false)
                .description("")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(ALLOW_COPY_COLUMN_NAME)
                .needArg(false)
                .description("If one side of datasource doesn't define the column name list, " +
                        "and unable to fetch it from datasource, " +
                        "this option will allow it to copy the information from the other side.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(ALLOW_COPY_COLUMN_TYPE)
                .needArg(false)
                .description("This option will allow datasource to copy " +
                        "the additional column type information which it doesn't has from the other side.")
                .build());
//        tmpList.add(SingleOptionConf.builder()
//                .name(ALLOW_COPY_KEY)
//                .needArg(false)
//                .description("This option will allow datasource to copy " +
//                        "the additional column type information which it doesn't has from the other side.")
//                .build());
        return tmpList;
    }

    @Override
    public void innerValidateOptions(GenericOptions options) {
        Integer numMapper = options.getInteger(CommonOptionsConf.NUM_MAPPER, CommonOptionsConf.DEFAULT_NUM_MAPPER);
        ParseUtils.assertTrue(numMapper > 0, "Illegal num mapper: " + numMapper);

        if (options.hasProperty(CommonOptionsConf.MAX_WRITE_QPS)) {
            ParseUtils.assertTrue(options.getDouble(CommonOptionsConf.MAX_WRITE_QPS, null) > 0,
                    "Illegal max write qps: " + numMapper);
        }

        // parse一下看看是不是json
        options.getJson(CommonOptionsConf.COLUMN_MAP, null);
    }
}
