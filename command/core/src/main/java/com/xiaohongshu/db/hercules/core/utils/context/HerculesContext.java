package com.xiaohongshu.db.hercules.core.utils.context;

import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.schema.Schema;
import com.xiaohongshu.db.hercules.core.supplier.AssemblySupplier;
import com.xiaohongshu.db.hercules.core.supplier.KvSerializerSupplier;
import com.xiaohongshu.db.hercules.core.utils.ReflectUtils;

import static com.xiaohongshu.db.hercules.core.option.KvOptionsConf.SUPPLIER;

public final class HerculesContext {

    private static Pair<AssemblySupplier> assemblySupplierPair = null;

    private static Pair<KvSerializerSupplier> kvSerializerSupplierPair = null;

    private static WrappingOptions wrappingOptions = null;

    private static Pair<Schema> schemaPair = null;

    public static Pair<AssemblySupplier> getAssemblySupplierPair() {
        if (assemblySupplierPair == null) {
            assemblySupplierPair = new Pair<>(
                    assemblySupplierFromOptions(wrappingOptions.getSourceOptions()),
                    assemblySupplierFromOptions(wrappingOptions.getTargetOptions())
            );
        }
        return assemblySupplierPair;
    }

    public static void setAssemblySupplierPair(AssemblySupplier sourceAssemblySupplier, AssemblySupplier targetAssemblySupplier) {
        assemblySupplierToOptions(sourceAssemblySupplier, wrappingOptions.getSourceOptions());
        assemblySupplierToOptions(targetAssemblySupplier, wrappingOptions.getTargetOptions());
        assemblySupplierPair = new Pair<>(sourceAssemblySupplier, targetAssemblySupplier);
    }

    public static Pair<KvSerializerSupplier> getKvSerializerSupplierPair() {
        if (kvSerializerSupplierPair == null) {
            kvSerializerSupplierPair = new Pair<>(
                    kvSupplierFromOptions(wrappingOptions.getSourceOptions()),
                    kvSupplierFromOptions(wrappingOptions.getTargetOptions())
            );
        }
        return kvSerializerSupplierPair;
    }

    public static Pair<Schema> getSchemaPair() {
        Pair<AssemblySupplier> assemblySupplierPair = getAssemblySupplierPair();
        if (schemaPair == null) {
            schemaPair = new Pair<>(
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
        return schemaPair;
    }

    public static WrappingOptions getWrappingOptions() {
        return wrappingOptions;
    }

    public static void setWrappingOptions(WrappingOptions wrappingOptions) {
        HerculesContext.wrappingOptions = wrappingOptions;
    }

    static final String ASSEMBLY_SUPPLIER_CLASS_NAME = "assembly-supplier-class-name-internal";

    private static void assemblySupplierToOptions(AssemblySupplier supplier, GenericOptions options) {
        options.set(ASSEMBLY_SUPPLIER_CLASS_NAME, supplier.getClass().getCanonicalName());
    }

    private static AssemblySupplier assemblySupplierFromOptions(GenericOptions options) {
        AssemblySupplier res = ReflectUtils.constructWithNonArgsConstructor(
                options.getString(ASSEMBLY_SUPPLIER_CLASS_NAME, null),
                AssemblySupplier.class
        );
        res.setOptions(options);
        return res;
    }

    private static KvSerializerSupplier kvSupplierFromOptions(GenericOptions options) {
        String supplierName = options.getString(SUPPLIER, null);
        if (supplierName == null) {
            return null;
        } else {
            KvSerializerSupplier res = ReflectUtils.constructWithNonArgsConstructor(
                    supplierName,
                    KvSerializerSupplier.class
            );
            res.setOptions(options);
            return res;
        }
    }

}
