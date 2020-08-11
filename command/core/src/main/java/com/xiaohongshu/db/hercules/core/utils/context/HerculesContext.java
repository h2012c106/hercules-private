package com.xiaohongshu.db.hercules.core.utils.context;

import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.supplier.AssemblySupplier;
import com.xiaohongshu.db.hercules.core.supplier.KvSerDerSupplier;
import com.xiaohongshu.db.hercules.core.utils.ReflectUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.xiaohongshu.db.hercules.core.option.KvOptionsConf.SUPPLIER;

public final class HerculesContext {

    private static final Log LOG = LogFactory.getLog(BaseSchemaFetcher.class);

    private final Pair<AssemblySupplier> assemblySupplierPair;

    private final Pair<KvSerDerSupplier> kvSerDerSupplierPair;

    private final WrappingOptions wrappingOptions;

    private final Pair<Schema> schemaPair;

    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static HerculesContext INSTANCE;

    /**
     * 防止循环引用，当第二次调用某个方法的inject的时候，说明出现循环引用，在inject结束后，取出此元素
     * 初始化完全是单线程的，相关代码无需担心多线程情况
     */
    private final Set<Class<?>> injectingClass = new HashSet<>();

    public static HerculesContext initialize(WrappingOptions wrappingOptions) {
        if (INITIALIZED.getAndSet(true)) {
            throw new RuntimeException("Not allow to initialize HerculesContext repeatedly.");
        } else {
            INSTANCE = new HerculesContext(wrappingOptions);
            return INSTANCE;
        }
    }

    public static HerculesContext initialize(WrappingOptions wrappingOptions,
                                             AssemblySupplier sourceSupplier,
                                             AssemblySupplier targetSupplier) {
        if (INITIALIZED.getAndSet(true)) {
            throw new RuntimeException("Not allow to initialize HerculesContext repeatedly.");
        } else {
            INSTANCE = new HerculesContext(wrappingOptions, sourceSupplier, targetSupplier);
            return INSTANCE;
        }
    }

    public static HerculesContext instance() {
        if (INITIALIZED.getAndSet(true)) {
            return INSTANCE;
        } else {
            throw new RuntimeException("The HerculesContext is not initialized yet.");
        }
    }

    private HerculesContext(WrappingOptions wrappingOptions) {
        this.wrappingOptions = wrappingOptions;
        this.assemblySupplierPair = extractAssemblySupplierPair(wrappingOptions);
        this.kvSerDerSupplierPair = extractKvSerDerSupplierPair(wrappingOptions);
        this.schemaPair = extractSchemaPair(wrappingOptions, this.assemblySupplierPair);

        // setOptions
        assemblySupplierPair.getSourceItem().setOptions(wrappingOptions.getSourceOptions());
        assemblySupplierPair.getTargetItem().setOptions(wrappingOptions.getTargetOptions());
        kvSerDerSupplierPair.getSourceItem().setOptions(wrappingOptions.getGenericOptions(OptionsType.SOURCE_SERDER));
        kvSerDerSupplierPair.getTargetItem().setOptions(wrappingOptions.getGenericOptions(OptionsType.TARGET_SERDER));
    }

    private HerculesContext(WrappingOptions wrappingOptions, AssemblySupplier sourceSupplier, AssemblySupplier targetSupplier) {
        this.wrappingOptions = wrappingOptions;
        this.assemblySupplierPair = new Pair<>(sourceSupplier, targetSupplier);
        this.kvSerDerSupplierPair = extractKvSerDerSupplierPair(wrappingOptions);
        this.schemaPair = extractSchemaPair(wrappingOptions, this.assemblySupplierPair);

        // 把supplier计入options中
        assemblySupplierToOptions(sourceSupplier, wrappingOptions.getSourceOptions());
        assemblySupplierToOptions(targetSupplier, wrappingOptions.getTargetOptions());

        // setOptions
        assemblySupplierPair.getSourceItem().setOptions(wrappingOptions.getSourceOptions());
        assemblySupplierPair.getTargetItem().setOptions(wrappingOptions.getTargetOptions());
        kvSerDerSupplierPair.getSourceItem().setOptions(wrappingOptions.getGenericOptions(OptionsType.SOURCE_SERDER));
        kvSerDerSupplierPair.getTargetItem().setOptions(wrappingOptions.getGenericOptions(OptionsType.TARGET_SERDER));
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
            Object fieldValueFromContext = contextElement.getContextReader().pulloutValueFromContext(this, field, annotation);

            boolean accessible = field.isAccessible();
            try {
                field.setAccessible(true);
                field.set(obj, fieldValueFromContext);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } finally {
                field.setAccessible(accessible);
            }
        }

        // 所有成员变量访问完，循环引用的警报解除
        injectingClass.remove(obj.getClass());

        return obj;
    }

    private Pair<AssemblySupplier> extractAssemblySupplierPair(WrappingOptions wrappingOptions) {
        return new Pair<>(
                assemblySupplierFromOptions(wrappingOptions.getSourceOptions()),
                assemblySupplierFromOptions(wrappingOptions.getTargetOptions())
        );
    }

    private Pair<KvSerDerSupplier> extractKvSerDerSupplierPair(WrappingOptions wrappingOptions) {
        return new Pair<>(
                kvSupplierFromOptions(wrappingOptions.getSourceOptions()),
                kvSupplierFromOptions(wrappingOptions.getTargetOptions())
        );
    }

    private Pair<Schema> extractSchemaPair(WrappingOptions wrappingOptions, Pair<AssemblySupplier> assemblySupplierPair) {
        return new Pair<>(
                Schema.fromOptions(
                        wrappingOptions.getSourceOptions(),
                        assemblySupplierPair.getSourceItem().getCustomDataTypeManager()
                ),
                Schema.fromOptions(
                        wrappingOptions.getTargetOptions(),
                        assemblySupplierPair.getTargetItem().getCustomDataTypeManager()
                )
        );
    }

    public Pair<AssemblySupplier> getAssemblySupplierPair() {
        return assemblySupplierPair;
    }

    public Pair<KvSerDerSupplier> getKvSerDerSupplierPair() {
        return kvSerDerSupplierPair;
    }

    public WrappingOptions getWrappingOptions() {
        return wrappingOptions;
    }

    public Pair<Schema> getSchemaPair() {
        return schemaPair;
    }

    private static final String ASSEMBLY_SUPPLIER_CLASS_NAME = "assembly-supplier-class-name-internal";

    private void assemblySupplierToOptions(AssemblySupplier supplier, GenericOptions options) {
        options.set(ASSEMBLY_SUPPLIER_CLASS_NAME, supplier.getClass().getCanonicalName());
    }

    private AssemblySupplier assemblySupplierFromOptions(GenericOptions options) {
        return ReflectUtils.constructWithNonArgsConstructor(
                options.getString(ASSEMBLY_SUPPLIER_CLASS_NAME, null),
                AssemblySupplier.class
        );
    }

    private KvSerDerSupplier kvSupplierFromOptions(GenericOptions options) {
        String supplierName = options.getString(SUPPLIER, null);
        if (supplierName == null) {
            return null;
        } else {
            KvSerDerSupplier res = ReflectUtils.constructWithNonArgsConstructor(
                    supplierName,
                    KvSerDerSupplier.class
            );
            res.setOptions(options);
            return res;
        }
    }

}
