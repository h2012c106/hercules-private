package com.xiaohongshu.db.hercules.core.utils.context.annotation;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author huanghanxiang
 */
@Target({FIELD})
@Retention(RUNTIME)
@Documented
public @interface SchemaInfo {
    DataSourceRole role();
}
