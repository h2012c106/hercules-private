package com.xiaohongshu.db.hercules.core.option;

import org.apache.commons.lang.StringUtils;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.function.Function;

/**
 * 用于配置单个配置项的类
 */
public final class SingleOptionConf {
    @Nonnull
    private String name;
    /**
     * 如果不需要arg，说明是根据有没有此参来判断01的布尔值，那么{@link #defaultStringValue}没必要填，填了也没用
     */
    @Nonnull
    private Boolean needArg;
    /**
     * 若是必须的参数，则{@link #defaultStringValue}没必要填，填了也没用，本配置晚于{@link #needArg}判断
     */
    private boolean necessary = false;

    private boolean setDefaultStringValue = false;
    private String defaultStringValue;

    @Nonnull
    private String description;

    private boolean list = false;

    private String listDelimiter = ",";

    private Function<String, Void> validateFunction = null;

    public static final Function<String, Void> NOT_EMPTY = new Function<String, Void>() {
        @Override
        public Void apply(String s) {
            if (StringUtils.isEmpty(s)) {
                throw new RuntimeException("Unallowed empty value: " + s);
            }
            return null;
        }
    };
    public static final Function<String, Void> NUMBER_AND_GT_ZERO = new Function<String, Void>() {
        @Override
        public Void apply(String s) {
            if (new BigDecimal(s).compareTo(BigDecimal.ZERO) <= 0) {
                throw new RuntimeException("Unallowed lte zero value: " + s);
            }
            return null;
        }
    };

    public String getName() {
        return name;
    }

    public boolean isNeedArg() {
        return needArg;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultStringValue() {
        return defaultStringValue;
    }

    public boolean isList() {
        return list;
    }

    public String getListDelimiter() {
        return listDelimiter;
    }

    public boolean isNecessary() {
        return necessary;
    }

    public boolean isSetDefaultStringValue() {
        return setDefaultStringValue;
    }

    public Function<String, Void> getValidateFunction() {
        return validateFunction;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final SingleOptionConf singleOptionConf = new SingleOptionConf();

        public Builder name(String name) {
            singleOptionConf.name = name;
            return this;
        }

        public Builder needArg(boolean needArg) {
            singleOptionConf.needArg = needArg;
            return this;
        }

        public Builder necessary(boolean necessary) {
            singleOptionConf.necessary = necessary;
            return this;
        }

        public Builder defaultStringValue(String defaultStringValue) {
            singleOptionConf.defaultStringValue = defaultStringValue;
            singleOptionConf.setDefaultStringValue = true;
            return this;
        }

        public Builder description(String description) {
            singleOptionConf.description = description;
            return this;
        }

        public Builder list(boolean list) {
            singleOptionConf.list = list;
            return this;
        }

        public Builder listDelimiter(String listDelimiter) {
            singleOptionConf.listDelimiter = listDelimiter;
            return this;
        }

        public Builder validateFunction(Function<String, Void> validateFunction) {
            singleOptionConf.validateFunction = validateFunction;
            return this;
        }

        private void checkNonnull() {
            Field[] fields = singleOptionConf.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (field.isAnnotationPresent(Nonnull.class)) {
                    boolean access = field.isAccessible();
                    try {
                        field.setAccessible(true);
                        Object obj = field.get(singleOptionConf);
                        if (obj == null) {
                            throw new RuntimeException(String.format("The [%s] option conf must be set a non-null value.", field.getName()));
                        }
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    } finally {
                        field.setAccessible(access);
                    }
                }
            }
        }

        public SingleOptionConf build() {
            checkNonnull();
            return singleOptionConf;
        }
    }
}
