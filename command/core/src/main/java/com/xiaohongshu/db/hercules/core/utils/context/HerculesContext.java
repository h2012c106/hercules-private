package com.xiaohongshu.db.hercules.core.utils.context;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRoleGetter;
import com.xiaohongshu.db.hercules.core.filter.expr.Expr;
import com.xiaohongshu.db.hercules.core.filter.function.FilterCoreFunction;
import com.xiaohongshu.db.hercules.core.filter.parser.Parser;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.supplier.AssemblySupplier;
import com.xiaohongshu.db.hercules.core.supplier.KvSerDerSupplier;
import com.xiaohongshu.db.hercules.core.utils.reflect.ReflectUtils;
import com.xiaohongshu.db.hercules.core.utils.reflect.Reflector;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf.FILTER;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.KVOptionsConf.SERDER_SUPPLIER;

public final class HerculesContext {

    private static final Log LOG = LogFactory.getLog(BaseSchemaFetcher.class);

    private final Family<AssemblySupplier> assemblySupplierPair;

    private final Family<KvSerDerSupplier> kvSerDerSupplierPair;

    private final WrappingOptions wrappingOptions;

    private final Family<Schema> schemaFamily;

    private final Expr filter;

    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static HerculesContext INSTANCE;

    /**
     * 防止循环引用，当第二次调用某个方法的inject的时候，说明出现循环引用，在inject结束后，取出此元素
     * 初始化完全是单线程的，相关代码无需担心多线程情况
     */
    private final Set<Class<?>> injectingClass = new HashSet<>();

    /**
     * 避免还在初始化instance就有人要取之，用来加读写锁，其实不会出现写锁冲突，这点由INITIALIZED保证，主要用来解读写与读读的问题
     */
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();

    private final Reflector reflector;

    public static HerculesContext initialize(Configuration configuration) {
        WrappingOptions options = new WrappingOptions();
        options.fromConfiguration(configuration);
        return initialize(options);
    }

    public static HerculesContext initialize(WrappingOptions wrappingOptions) {
        if (INITIALIZED.getAndSet(true)) {
            LOG.debug("Initialize HerculesContext repeatedly, ignore this init.");
        } else {
            LOCK.writeLock().lock();
            try {
                INSTANCE = new HerculesContext(wrappingOptions);
            } finally {
                LOCK.writeLock().unlock();
            }
        }
        return INSTANCE;
    }

    public static HerculesContext initialize(WrappingOptions wrappingOptions,
                                             AssemblySupplier sourceSupplier,
                                             AssemblySupplier targetSupplier,
                                             Reflector reflector) {
        if (INITIALIZED.getAndSet(true)) {
            LOG.debug("Initialize HerculesContext repeatedly, ignore this init.");
        } else {
            LOCK.writeLock().lock();
            try {
                INSTANCE = new HerculesContext(wrappingOptions, sourceSupplier, targetSupplier, reflector);
            } finally {
                LOCK.writeLock().unlock();
            }
        }
        return INSTANCE;
    }

    public static HerculesContext instance() {
        if (INITIALIZED.get()) {
            LOCK.readLock().lock();
            try {
                return INSTANCE;
            } finally {
                LOCK.readLock().unlock();
            }
        } else {
            throw new RuntimeException("The HerculesContext is not initialized yet.");
        }
    }

    private HerculesContext(WrappingOptions wrappingOptions) {
        this.reflector = new Reflector();

        this.wrappingOptions = wrappingOptions;
        this.assemblySupplierPair = extractAssemblySupplierPair(wrappingOptions);
        this.kvSerDerSupplierPair = extractKvSerDerSupplierPair(wrappingOptions);
        this.schemaFamily = extractSchemaFamily(wrappingOptions, this.assemblySupplierPair, this.kvSerDerSupplierPair);
        this.filter = extractFilter(wrappingOptions);

        // setOptions
        assemblySupplierPair.getSourceItem().setOptions(wrappingOptions.getSourceOptions());
        assemblySupplierPair.getTargetItem().setOptions(wrappingOptions.getTargetOptions());
        if (hasSerDer(DataSourceRole.SOURCE)) {
            kvSerDerSupplierPair.getSourceItem().setOptions(wrappingOptions.getGenericOptions(OptionsType.DER));
        }
        if (hasSerDer(DataSourceRole.TARGET)) {
            kvSerDerSupplierPair.getTargetItem().setOptions(wrappingOptions.getGenericOptions(OptionsType.SER));
        }
    }

    private HerculesContext(WrappingOptions wrappingOptions, AssemblySupplier sourceSupplier, AssemblySupplier targetSupplier, Reflector reflector) {
        this.reflector = reflector;

        this.wrappingOptions = wrappingOptions;
        this.assemblySupplierPair = Family.initializeDataSource(sourceSupplier, targetSupplier);
        this.kvSerDerSupplierPair = extractKvSerDerSupplierPair(wrappingOptions);
        this.schemaFamily = extractSchemaFamily(wrappingOptions, this.assemblySupplierPair, this.kvSerDerSupplierPair);
        this.filter = extractFilter(wrappingOptions);

        // 把supplier计入options中
        assemblySupplierToOptions(sourceSupplier, wrappingOptions.getSourceOptions());
        assemblySupplierToOptions(targetSupplier, wrappingOptions.getTargetOptions());

        // setOptions
        assemblySupplierPair.getSourceItem().setOptions(wrappingOptions.getSourceOptions());
        assemblySupplierPair.getTargetItem().setOptions(wrappingOptions.getTargetOptions());
        if (hasSerDer(DataSourceRole.SOURCE)) {
            kvSerDerSupplierPair.getSourceItem().setOptions(wrappingOptions.getGenericOptions(OptionsType.DER));
        }
        if (hasSerDer(DataSourceRole.TARGET)) {
            kvSerDerSupplierPair.getTargetItem().setOptions(wrappingOptions.getGenericOptions(OptionsType.SER));
        }
    }

    public Reflector getReflector() {
        return reflector;
    }

    public boolean hasSerDer(DataSourceRole role) {
        return kvSerDerSupplierPair.getItem(role) != null;
    }

    /**
     * 仅允许inject的对象为private类型，不然在field有否必要赋值以及检查循环依赖时还要考虑继承等复杂问题，
     * 且基于注解的依赖注入本来就已经大大减轻依赖注入的复杂度，重复写几个private而已，作为代价个人认为完全可以接受。
     *
     * @param obj
     * @param <T>
     * @return
     */
    public <T> T inject(T obj) {
        LOG.debug("Injecting: " + obj);

        // 检查调用栈上是否已经正在inject此类，若有，则说明出现循环引用。
        if (injectingClass.contains(obj.getClass())) {
            throw new RuntimeException("Circular reference occurs, criminal: " + obj.getClass().getCanonicalName());
        } else {
            injectingClass.add(obj.getClass());
        }

        // 如果此类实现getRole()方法，则先调一下这个接口拿到此类角色
        DataSourceRole classConfiguredRole = null;
        if (ReflectUtils.doesImplementInterface(obj.getClass(), DataSourceRoleGetter.class)) {
            classConfiguredRole = ((DataSourceRoleGetter) obj).getRole();
        }

        // 仅注入被注入对象的类属性，不注入父类
        // List<Field> filedList = ReflectUtils.getFiledList(obj.getClass());
        // 递归注入上去
        List<Field> filedList = FieldUtils.getAllFieldsList(obj.getClass());
        for (Field field : filedList) {
            // 限制不能有超过一个的context annotation
            Annotation annotation = null;
            List<HerculesContextElement> contextElementList = new ArrayList<>(HerculesContextElement.values().length);
            for (HerculesContextElement contextElement : HerculesContextElement.values()) {
                Annotation tmpAnnotation = field.getAnnotation(contextElement.getInjectAnnotation());
                if (tmpAnnotation != null) {
                    contextElementList.add(contextElement);
                    // annotation有覆盖的可能也无所谓，反正合法情况有且仅有一个annotation
                    annotation = tmpAnnotation;
                }
            }
            if (contextElementList.size() > 1) {
                throw new RuntimeException(String.format("The context injection field [%s[ of class [%s] can only be annotated by one annotation, now: %s.",
                        field.getName(), obj.getClass().getCanonicalName(), contextElementList.toString()));
            } else if (contextElementList.size() == 0) {
                continue;
            }

            // 检查field修饰符
            if (!Modifier.isPrivate(field.getModifiers())) {
                throw new RuntimeException(String.format("The field [%s] of class [%s] must be a private field so as to be injected.",
                        field.getName(), obj.getClass().getCanonicalName()));
            }
            if (Modifier.isStatic(field.getModifiers())) {
                throw new RuntimeException(String.format("Unable to inject a static field [%s] of class [%s].",
                        field.getName(), obj.getClass().getCanonicalName()));
            }

            HerculesContextElement contextElement = contextElementList.get(0);
            Object fieldValueFromContext;
            try {
                fieldValueFromContext = contextElement.getContextReader()
                        .pulloutValueFromContext(this, field, annotation, classConfiguredRole);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Inject [%s] failed.", obj.getClass().getCanonicalName()), e);
            }

            boolean accessible = field.isAccessible();
            try {
                field.setAccessible(true);
                field.set(obj, field.getType().cast(fieldValueFromContext));
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } finally {
                field.setAccessible(accessible);
            }
        }

        // 所有成员变量访问完，循环引用的警报解除
        injectingClass.remove(obj.getClass());

        // 如果这个类继承了InjectedClass，则执行一下这个函数
        if (ReflectUtils.doesImplementInterface(obj.getClass(), InjectedClass.class)) {
            ((InjectedClass) obj).afterInject();
        }

        return obj;
    }

    private Family<AssemblySupplier> extractAssemblySupplierPair(WrappingOptions wrappingOptions) {
        return Family.initializeDataSource(
                assemblySupplierFromOptions(wrappingOptions.getSourceOptions()),
                assemblySupplierFromOptions(wrappingOptions.getTargetOptions())
        );
    }

    private Family<KvSerDerSupplier> extractKvSerDerSupplierPair(WrappingOptions wrappingOptions) {
        return Family.initializeSerDer(
                kvSupplierFromOptions(wrappingOptions.getSourceOptions()),
                kvSupplierFromOptions(wrappingOptions.getTargetOptions())
        );
    }

    private Family<Schema> extractSchemaFamily(WrappingOptions wrappingOptions,
                                               Family<AssemblySupplier> assemblySupplierPair,
                                               Family<KvSerDerSupplier> serDerSupplierPair) {
        return Family.initialize(
                Schema.fromOptions(
                        wrappingOptions.getSourceOptions(),
                        assemblySupplierPair.getSourceItem().getCustomDataTypeManager()
                ),
                Schema.fromOptions(
                        wrappingOptions.getTargetOptions(),
                        assemblySupplierPair.getTargetItem().getCustomDataTypeManager()
                ),
                hasSerDer(DataSourceRole.DER)
                        ? Schema.fromOptions(
                        wrappingOptions.getGenericOptions(OptionsType.DER),
                        serDerSupplierPair.getDerItem().getCustomDataTypeManager()
                )
                        : null,
                hasSerDer(DataSourceRole.SER)
                        ? Schema.fromOptions(
                        wrappingOptions.getGenericOptions(OptionsType.SER),
                        serDerSupplierPair.getSerItem().getCustomDataTypeManager()
                )
                        : null
        );
    }

    private Expr extractFilter(WrappingOptions wrappingOptions) {
        String filterStr = wrappingOptions.getCommonOptions().getString(FILTER, null);
        filterStr = filterStr == null ? null : filterStr.trim();
        if (!StringUtils.isEmpty(filterStr)) {
            // 注册Custom Type Manager，用于注册kast函数
            FilterCoreFunction.registerCustomTypeManager(assemblySupplierPair.getSourceItem().getCustomDataTypeManager());
            // 若有der则覆写
            if (kvSerDerSupplierPair.getDerItem() != null) {
                FilterCoreFunction.registerCustomTypeManager(kvSerDerSupplierPair.getDerItem().getCustomDataTypeManager());
            }
            return Parser.INSTANCE.parse(filterStr);
        } else {
            return null;
        }
    }

    public Family<AssemblySupplier> getAssemblySupplierPair() {
        return assemblySupplierPair;
    }

    public Family<KvSerDerSupplier> getKvSerDerSupplierPair() {
        return kvSerDerSupplierPair;
    }

    public WrappingOptions getWrappingOptions() {
        return wrappingOptions;
    }

    public Family<Schema> getSchemaFamily() {
        return schemaFamily;
    }

    public Expr getFilter() {
        return filter;
    }

    private static final String ASSEMBLY_SUPPLIER_CLASS_NAME = "assembly-supplier-class-name-internal";

    private void assemblySupplierToOptions(AssemblySupplier supplier, GenericOptions options) {
        options.set(ASSEMBLY_SUPPLIER_CLASS_NAME, supplier.getClass().getCanonicalName());
    }

    private AssemblySupplier assemblySupplierFromOptions(GenericOptions options) {
        return reflector.constructWithNonArgsConstructor(
                options.getString(ASSEMBLY_SUPPLIER_CLASS_NAME, null),
                AssemblySupplier.class
        );
    }

    private KvSerDerSupplier kvSupplierFromOptions(GenericOptions options) {
        String supplierName = options.getString(SERDER_SUPPLIER, null);
        if (supplierName == null) {
            return null;
        } else {
            KvSerDerSupplier res = reflector.constructWithNonArgsConstructor(
                    supplierName,
                    KvSerDerSupplier.class
            );
            res.setOptions(options);
            return res;
        }
    }

}
