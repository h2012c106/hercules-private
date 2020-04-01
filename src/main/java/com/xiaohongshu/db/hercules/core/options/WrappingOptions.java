package com.xiaohongshu.db.hercules.core.options;

import com.xiaohongshu.db.hercules.core.parser.OptionsType;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;

public class WrappingOptions {
    private GenericOptions sourceOptions;
    private GenericOptions targetOptions;
    private GenericOptions commonOptions;

    public WrappingOptions() {
        this.sourceOptions = new GenericOptions();
        this.targetOptions = new GenericOptions();
        this.commonOptions = new GenericOptions();
    }

    public WrappingOptions(GenericOptions sourceOptions, GenericOptions targetOptions, GenericOptions commonOptions) {
        this.sourceOptions = sourceOptions;
        this.targetOptions = targetOptions;
        this.commonOptions = commonOptions;
    }

    public GenericOptions getSourceOptions() {
        return sourceOptions;
    }

    public GenericOptions getTargetOptions() {
        return targetOptions;
    }

    public GenericOptions getCommonOptions() {
        return commonOptions;
    }

    public void toConfiguration(Configuration configuration){
        sourceOptions.toConfiguration(configuration, OptionsType.SOURCE);
        targetOptions.toConfiguration(configuration, OptionsType.TARGET);
        commonOptions.toConfiguration(configuration, OptionsType.COMMON);
    }

    public void fromConfiguration(Configuration configuration){
        sourceOptions.fromConfiguration(configuration, OptionsType.SOURCE);
        targetOptions.fromConfiguration(configuration, OptionsType.TARGET);
        commonOptions.fromConfiguration(configuration, OptionsType.COMMON);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("sourceOptions", sourceOptions)
                .append("targetOptions", targetOptions)
                .append("commonOptions", commonOptions)
                .toString();
    }
}
