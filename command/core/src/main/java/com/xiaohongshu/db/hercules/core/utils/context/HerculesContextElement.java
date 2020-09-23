package com.xiaohongshu.db.hercules.core.utils.context;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Assembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Filter;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

public enum HerculesContextElement {
    /**
     * 模块
     */
    ASSEMBLY(Assembly.class, new ContextReader<Assembly>() {
        @SneakyThrows
        @Override
        public Object pulloutValueFromContext(HerculesContext context, final Field field,
                                              Assembly annotation, DataSourceRole role) {
            Object supplierObject;
            role = getRole(annotation.role(), role);
            switch (role) {
                case SOURCE:
                case TARGET:
                    supplierObject = context.getAssemblySupplierPair().getItem(role);
                    break;
                case DER:
                case SER:
                    supplierObject = context.getKvSerDerSupplierPair().getItem(role);
                    if (supplierObject == null) {
                        LOG.debug(String.format("%s KvSerDerSupplier is null, inject the assembly field [%s] with null.", role, field));
                        return null;
                    }
                    break;
                default:
                    throw new RuntimeException();
            }

            // 如果用户给了方法名了，直接反射就完事儿了
            String annotatedMethodName = annotation.getMethodName();
            if (!StringUtils.isEmpty(annotatedMethodName)) {
                Method method = supplierObject.getClass().getMethod(annotatedMethodName);
                return method.invoke(supplierObject);
            }

            List<Method> supplierPublicMethodList = getGetMethodList(supplierObject.getClass(), Object.class);

            // 根据返回类型来，必须得一摸一样
            List<Method> sameReturnClassMethodList = supplierPublicMethodList.stream()
                    .filter(method -> field.getType() == method.getReturnType())
                    .collect(Collectors.toList());
            if (sameReturnClassMethodList.size() == 1) {
                return sameReturnClassMethodList.get(0).invoke(supplierObject);
            } else if (sameReturnClassMethodList.size() == 0) {
                LOG.debug(String.format("Not find [%s]-return-class method for field.", field.getType().getCanonicalName()));
            } else {
                LOG.debug(String.format("Find more than one method that [%s]-return-class method for field: %s.", field.getType(), sameReturnClassMethodList));
            }

            // 最后根据field名和method名来，若field叫aaa，则会寻找getAaa() method
            String concatMethodName = GET_METHOD_NAME_PREFIX + field.getName();
            List<Method> sameNameMethodList = supplierPublicMethodList.stream()
                    .filter(method -> StringUtils.equalsIgnoreCase(concatMethodName, method.getName()))
                    .collect(Collectors.toList());
            if (sameNameMethodList.size() == 1) {
                return sameNameMethodList.get(0).invoke(supplierObject);
            } else if (sameNameMethodList.size() == 0) {
                LOG.debug(String.format("Not find [%s]-name (ignore-case) method for field.", concatMethodName));
            } else {
                LOG.debug(String.format("Find more than one method that [%s]-name (ignore-case) method for field: %s.", concatMethodName, sameNameMethodList));
            }

            throw new RuntimeException(String.format("Cannot inject the assembly into field [%s] from [%s].", field, supplierObject.getClass().getCanonicalName()));
        }
    }),
    /**
     * 参数
     */
    OPTIONS(Options.class, new ContextReader<Options>() {
        @Override
        public Object pulloutValueFromContext(HerculesContext context, Field field,
                                              Options annotation, DataSourceRole role) {
            return context.getWrappingOptions().getGenericOptions(annotation.type());
        }
    }),
    /**
     * schema
     */
    SCHEMA(SchemaInfo.class, new ContextReader<SchemaInfo>() {
        @Override
        public Object pulloutValueFromContext(HerculesContext context, Field field,
                                              SchemaInfo annotation, DataSourceRole role) {
            return context.getSchemaFamily().getItem(getRole(annotation.role(), role));
        }
    }),
    /**
     * filter
     */
    FILTER(Filter.class, new ContextReader<Filter>() {
        @Override
        public Object pulloutValueFromContext(HerculesContext context, Field field,
                                              Filter annotation, DataSourceRole role) {
            return context.getFilter();
        }
    });

    private static final Log LOG = LogFactory.getLog(HerculesContextElement.class);

    private static final String GET_METHOD_NAME_PREFIX = "get";

    private final Class<? extends Annotation> injectAnnotation;

    private final ContextReader contextReader;

    HerculesContextElement(Class<? extends Annotation> injectAnnotation, ContextReader contextReader) {
        this.injectAnnotation = injectAnnotation;
        this.contextReader = contextReader;
    }

    public Class<? extends Annotation> getInjectAnnotation() {
        return injectAnnotation;
    }

    public ContextReader getContextReader() {
        return contextReader;
    }

    private static DataSourceRole getRole(DataSourceRole[] annotationRole, DataSourceRole classRole) {
        if (annotationRole.length == 0 && classRole == null) {
            throw new RuntimeException("Cannot inject a object field without annotation role or class role (specified by DataSourceRoleGetter interface).");
        }
        // 优先用用户注解配置
        if (annotationRole.length > 0) {
            return annotationRole[0];
        } else {
            return classRole;
        }
    }

    /**
     * 包头不包尾
     *
     * @param start
     * @param end
     * @return
     */
    private static List<Method> getAllMethodList(Class<?> start, Class<?> end) {
        List<Method> res = new LinkedList<>();
        for (Class<?> tmpClazz = start; tmpClazz != null && tmpClazz != end; tmpClazz = tmpClazz.getSuperclass()) {
            res.addAll(Arrays.asList(tmpClazz.getDeclaredMethods()));
        }
        return res;
    }

    private static List<Method> getGetMethodList(Class<?> clazz, Class<?> end) {
        Map<String, Method> methodMap = new LinkedHashMap<>();
        for (Method method : getAllMethodList(clazz, end)) {
            // 静态方法、非public方法不考虑
            if (Modifier.isStatic(method.getModifiers()) || !Modifier.isPublic(method.getModifiers())) {
                continue;
            }
            // 若存在泛型父类的泛型方法实现或某重写方法返回了父类返回类型的子类，使用getDeclaredMethods时会获得父类的此同名方法，此处需筛
            if (method.isBridge()) {
                continue;
            }
            // 必须无入参
            if (method.getParameterCount() != 0) {
                continue;
            }
            // 同名函数使用子类实现
            methodMap.putIfAbsent(method.getName(), method);
        }
        return new LinkedList<>(methodMap.values());
    }

    public interface ContextReader<T extends Annotation> {
        public Object pulloutValueFromContext(HerculesContext context, Field field, T annotation, DataSourceRole role);
    }
}
