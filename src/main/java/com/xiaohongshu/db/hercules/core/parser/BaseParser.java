package com.xiaohongshu.db.hercules.core.parser;

import com.xiaohongshu.db.hercules.core.DataSourceRole;
import com.xiaohongshu.db.hercules.core.exceptions.ParseException;
import com.xiaohongshu.db.hercules.core.options.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.options.GenericOptions;
import com.xiaohongshu.db.hercules.core.options.SingleOptionConf;
import org.apache.commons.cli.*;

import java.util.ListIterator;
import java.util.Map;

/**
 * parser基类，除了common每个parser实现应当向{@link ParserFactory}注册自己的实现对象
 *
 * @param <T> optionsConf类
 */
public abstract class BaseParser<T extends BaseOptionsConf> {

    public static final String SOURCE_OPTIONS_PREFIX = "source-";
    public static final String TARGET_OPTIONS_PREFIX = "target-";

    abstract public DataSourceRole getDataSourceRole();


    /**
     * 获得一个{@link BaseOptionsConf}子类对象用于{@link #getCliOptions()}、
     * {@link #parseCommandLine(CommandLine)}、{@link #validateOptions(GenericOptions)}
     *
     * @return
     */
    abstract protected T getOptionsConf();

    private OptionsType getRole() {
        if (getDataSourceRole() == null) {
            return OptionsType.COMMON;
        }
        switch (getDataSourceRole()) {
            case SOURCE:
                return OptionsType.SOURCE;
            case TARGET:
                return OptionsType.TARGET;
            default:
                throw new ParseException("Unknown data source role: " + getDataSourceRole().name());
        }
    }

    private String getOptionsPrefix() {
        switch (getRole()) {
            case SOURCE:
                return SOURCE_OPTIONS_PREFIX;
            case TARGET:
                return TARGET_OPTIONS_PREFIX;
            default:
                return "";
        }
    }

    /**
     * 获得当前parser的命令行options
     *
     * @return
     */
    private Options getCliOptions() {
        String prefix = getOptionsPrefix();
        Options options = new Options();
        for (SingleOptionConf optionConf : getOptionsConf().getOptionsMap().values()) {
            Option option = new Option(null,
                    prefix + optionConf.getName(),
                    optionConf.isNeedArg(),
                    optionConf.getDescription());
            // 不需要arg的参数必然不是必须出现的参数
            if (optionConf.isNeedArg() && optionConf.isNecessary()) {
                option.setRequired(true);
            }
            options.addOption(option);
        }
        return options;
    }

    /**
     * 将commandline对象拿到的属性塞到一个options当中
     *
     * @param cli
     * @return
     */
    private GenericOptions parseCommandLine(CommandLine cli) {
        String prefix = getOptionsPrefix();
        GenericOptions options = new GenericOptions();
        for (Map.Entry<String, SingleOptionConf> entry : getOptionsConf().getOptionsMap().entrySet()) {
            String paramName = entry.getKey();
            String optionName = prefix + paramName;
            SingleOptionConf optionConf = entry.getValue();

            String optionValue;
            if (optionConf.isNeedArg()) {
                optionValue = cli.getOptionValue(optionName, optionConf.getDefaultStringValue());
            } else {
                optionValue = Boolean.toString(cli.hasOption(optionName));
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
        return options;
    }

    /**
     * 检查options，包括类型、参数间依赖关系等一切需要检查的东西，遇错直接抛
     *
     * @param options
     */
    abstract protected void validateOptions(GenericOptions options);

    /**
     * 根据读入的命令行参数输出options对象
     *
     * @param args
     * @return
     */
    public final GenericOptions parse(String[] args) {
        // 解析
        CommandLineParser cliParser = new IgnorableParser(getOptionsPrefix());
        CommandLine cli;
        try {
            cli = cliParser.parse(this.getCliOptions(), args);
        } catch (Exception e) {
            throw new ParseException(e);
        }

        // 塞options
        GenericOptions options = parseCommandLine(cli);

        // validate
        try {
            validateOptions(options);
        } catch (Exception e) {
            throw new ParseException(e);
        }

        return options;
    }

    static private class IgnorableParser extends GnuParser {
        private String prefix;

        public IgnorableParser(String prefix) {
            this.prefix = prefix;
        }

        @Override
        protected void processOption(final String arg, final ListIterator iter) throws org.apache.commons.cli.ParseException {
            if (arg.startsWith(prefix)) {
                super.processOption(arg, iter);
            }
        }
    }
}
