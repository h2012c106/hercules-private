package com.xiaohongshu.db.hercules.core.parser;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import org.apache.commons.cli.*;

import java.util.ListIterator;
import java.util.Map;

/**
 * parser基类，除了common每个parser实现应当向{@link ParserFactory}注册自己的实现对象
 */
public abstract class BaseParser {

    public static final String SOURCE_OPTIONS_PREFIX = "source-";
    public static final String TARGET_OPTIONS_PREFIX = "target-";

    private boolean help;

    private BaseOptionsConf optionsConf;
    private DataSource dataSource;
    private DataSourceRole dataSourceRole;

    public BaseParser(BaseOptionsConf optionsConf, DataSource dataSource, DataSourceRole dataSourceRole) {
        this.optionsConf = optionsConf;
        this.dataSource = dataSource;
        this.dataSourceRole = dataSourceRole;
    }

    public BaseOptionsConf getOptionsConf() {
        return optionsConf;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public DataSourceRole getDataSourceRole() {
        return dataSourceRole;
    }

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
                throw new com.xiaohongshu.db.hercules.core.exception.ParseException("Unknown data source role: " + getDataSourceRole().name());
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
        for (SingleOptionConf optionConf : optionsConf.getOptionsMap().values()) {
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
        for (Map.Entry<String, SingleOptionConf> entry : optionsConf.getOptionsMap().entrySet()) {
            String paramName = entry.getKey();
            String optionName = prefix + paramName;
            SingleOptionConf optionConf = entry.getValue();

            String optionValue = null;
            if (optionConf.isNeedArg()) {
                // 如果未设置necessary且未设置default value，用户不指定此参数时，不会把这个参数塞到options里
                if (optionConf.isSetDefaultStringValue()) {
                    optionValue = cli.getOptionValue(optionName, optionConf.getDefaultStringValue());
                } else {
                    if (cli.hasOption(optionName)) {
                        optionValue = cli.getOptionValue(optionName);
                    }
                }
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

    private void help(Options cliOptions) {
        String helpHeader;
        OptionsType type = getRole();
        switch (type) {
            case SOURCE:
                helpHeader = String.format("Datasource [%s]'s param as source:\n" +
                                "(if datasource name doesn't match the actual one, relax, " +
                                "just because they share the same param parser, definitely not a bug)\n\n",
                        getDataSource().name());
                break;
            case TARGET:
                helpHeader = String.format("Datasource [%s]'s param as target:\n" +
                                "(if datasource name doesn't match the actual one, relax, " +
                                "just because they share the same param parser, definitely not a bug)\n\n",
                        getDataSource().name());
                break;
            case COMMON:
                helpHeader = "Common param:\n\n";
                break;
            default:
                throw new RuntimeException("Unknown option type: " + type);
        }
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(String.format("%s USAGE: ", type.name()), helpHeader, cliOptions, "", true);
    }

    private void help(GenericOptions options, Options cliOptions) {
        help = options.getBoolean(BaseOptionsConf.HELP, false);
        if (help) {
            help(cliOptions);
        }
    }

    public boolean isHelp() {
        return help;
    }

    /**
     * 根据读入的命令行参数输出options对象
     *
     * @param args
     * @return
     */
    public final GenericOptions parse(String[] args) {
        // 解析
        CommandLineParser cliParser;
        if (getRole() == OptionsType.COMMON) {
            cliParser = new IgnorableParser(getOptionsPrefix(), true);
        } else {
            cliParser = new IgnorableParser(getOptionsPrefix(), false);
        }
        CommandLine cli;
        Options cliOptions = this.getCliOptions();
        try {
            cli = cliParser.parse(cliOptions, args);
        } catch (Exception e) {
            help(cliOptions);
            throw new com.xiaohongshu.db.hercules.core.exception.ParseException(e);
        }

        // 塞options
        GenericOptions options = parseCommandLine(cli);

        help(options, cliOptions);

        // validate
        try {
            optionsConf.validateOptions(options);
        } catch (Exception e) {
            throw new com.xiaohongshu.db.hercules.core.exception.ParseException(e);
        }

        // 处理一些options的转换
        optionsConf.processOptions(options);

        return options;
    }

    static private class IgnorableParser extends GnuParser {
        private String prefix;
        private boolean ignore;

        public IgnorableParser(String prefix, boolean ignore) {
            this.prefix = prefix;
            this.ignore = ignore;
        }

        @Override
        protected void processOption(final String arg, final ListIterator<String> iter)
                throws org.apache.commons.cli.ParseException {
            boolean hasOption = getOptions().hasOption(arg);
            if (hasOption) {
                super.processOption(arg, iter);
            } else {
                if (!ignore && arg.startsWith(String.format("--%s", prefix))) {
                    super.processOption(arg, iter);
                }
            }
        }
    }
}
