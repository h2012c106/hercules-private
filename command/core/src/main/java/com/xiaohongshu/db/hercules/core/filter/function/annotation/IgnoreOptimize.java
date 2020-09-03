package com.xiaohongshu.db.hercules.core.filter.function.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author huanghanxiang
 */
@Target(METHOD)
@Retention(RUNTIME)
@Documented
public @interface IgnoreOptimize {
}
