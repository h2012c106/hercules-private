package com.xiaohongshu.db.hercules.core.schema;

import com.google.common.collect.BiMap;
import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datatype.CustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.OptionsType;
import com.xiaohongshu.db.hercules.core.option.optionsconf.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.Options;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SchemaInfo;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.SerDerAssembly;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 负责对齐上下游列名List、列类型Map，并且把结果写入options中，在map阶段不需要二次对齐/获取，保证schema信息全局取一次。
 * 经过这个对象处理，所有schema信息均通过option取。
 *
 * @author huanghanxiang
 */
public final class SchemaNegotiator {

    @GeneralAssembly(role = DataSourceRole.SOURCE)
    private DataSource sourceDataSource;

    @GeneralAssembly(role = DataSourceRole.TARGET)
    private DataSource targetDataSource;

    @Options(type = OptionsType.COMMON)
    private GenericOptions commonOptions;

    @Options(type = OptionsType.SOURCE)
    private GenericOptions sourceOptions;

    @Options(type = OptionsType.TARGET)
    private GenericOptions targetOptions;

    @Options(type = OptionsType.DER)
    private GenericOptions derOptions;

    @Options(type = OptionsType.SER)
    private GenericOptions serOptions;

    @GeneralAssembly(role = DataSourceRole.SOURCE)
    private SchemaFetcher sourceSchemaFetcher;

    @GeneralAssembly(role = DataSourceRole.TARGET)
    private SchemaFetcher targetSchemaFetcher;

    @GeneralAssembly(role = DataSourceRole.SOURCE)
    private CustomDataTypeManager<?, ?> sourceDataTypeManager;

    @GeneralAssembly(role = DataSourceRole.TARGET)
    private CustomDataTypeManager<?, ?> targetDataTypeManager;

    @GeneralAssembly(role = DataSourceRole.SOURCE, getMethodName = "getSchemaNegotiatorContextAsSource")
    private SchemaNegotiatorContext sourceContext;

    @GeneralAssembly(role = DataSourceRole.TARGET, getMethodName = "getSchemaNegotiatorContextAsTarget")
    private SchemaNegotiatorContext targetContext;

    @SerDerAssembly(role = DataSourceRole.DER, getMethodName = "getSchemaNegotiatorContextAsSource")
    private SchemaNegotiatorContext derContext;

    @SerDerAssembly(role = DataSourceRole.SER, getMethodName = "getSchemaNegotiatorContextAsTarget")
    private SchemaNegotiatorContext serContext;

    @SchemaInfo(role = DataSourceRole.SOURCE)
    private Schema sourceSchema;

    @SchemaInfo(role = DataSourceRole.TARGET)
    private Schema targetSchema;

    @SchemaInfo(role = DataSourceRole.DER)
    private Schema derSchema;

    @SchemaInfo(role = DataSourceRole.SER)
    private Schema serSchema;

    private static final Log LOG = LogFactory.getLog(SchemaNegotiator.class);

    private List<String> getColumnNameList(List<String> configuredColumnList, SchemaFetcher schemaFetcher) {
        // 若用户有配置，则直接使用用户配置的
        if (configuredColumnList.size() != 0) {
            return configuredColumnList;
        }
        List<String> res = schemaFetcher.getColumnNameList();
        return res == null ? new ArrayList<>(0) : res;
    }

    /**
     * 检查映射好的上游列和下游列的对应关系（多列少列）
     * 仅在上下游皆用户配置了列/从数据源能拿到列时，做这一步，若一侧没有固定的schema，那么列的more/less根本没有可比性。
     * 可能有人说了，那我逐行比不还是有可比性。先不说性能影响，一个nosql的东西要是逐行地约束schema，按照nosql的特性，这数基本导不了了。
     *
     * @return 返回两侧的列交集，列名是经columnMap转换后的列名（也就是目标表为准的列名）
     */
    private Set<String> validateAndGetIntersection(List<String> sourceColumnNameList,
                                                   List<String> targetColumnNameList,
                                                   BiMap<String, String> biColumnMap) {
        // 按照规则映射后的源数据库列名
        List<String> convertedSourceColumnNameList = sourceColumnNameList.stream()
                .map(columnName -> biColumnMap.getOrDefault(columnName, columnName))
                .collect(Collectors.toList());

        Set<String> tmpSet;

        // 检查源数据源多列
        tmpSet = new HashSet<>(convertedSourceColumnNameList);
        tmpSet.removeAll(targetColumnNameList);
        if (tmpSet.size() > 0) {
            // 需要把多的列名根据转换规则转回去，不然显示的是转换后的如果变动较大会造成看日志的人迷惑
            Map<String, String> inversedColumnMap = biColumnMap.inverse();
            Set<String> tmpTmpSet = tmpSet.stream()
                    .map(columnName -> inversedColumnMap.getOrDefault(columnName, columnName))
                    .collect(Collectors.toSet());
            if (!commonOptions.getBoolean(CommonOptionsConf.ALLOW_SOURCE_MORE_COLUMN, false)) {
                throw new SchemaException(String.format("Source data source has more columns: %s, " +
                                "if want to ignore, please use '--%s'",
                        tmpTmpSet, CommonOptionsConf.ALLOW_SOURCE_MORE_COLUMN
                ));
            } else {
                LOG.warn(String.format("Source data source has more columns: %s", tmpTmpSet));
            }
        }

        // 检查目标数据源多列
        tmpSet = new HashSet<>(targetColumnNameList);
        tmpSet.removeAll(convertedSourceColumnNameList);
        if (tmpSet.size() > 0) {
            if (!commonOptions.getBoolean(CommonOptionsConf.ALLOW_TARGET_MORE_COLUMN, false)) {
                throw new SchemaException(String.format("Target data source has more columns: %s, " +
                        "if want to ignore, please use '--%s'", tmpSet, CommonOptionsConf.ALLOW_TARGET_MORE_COLUMN));
            } else {
                LOG.warn(String.format("Target data source has more columns: %s", tmpSet));
            }
        }

        // 检查两侧交集，至少得有一列才合理
        tmpSet = new HashSet<>(targetColumnNameList);
        tmpSet.retainAll(convertedSourceColumnNameList);
        if (tmpSet.size() == 0) {
            throw new SchemaException("Not column is in the intersection, meaningless!");
        } else {
            LOG.info("The public columns is: " + tmpSet);
        }

        return tmpSet;
    }

    private Map<String, DataType> getColumnTypeMap(Map<String, DataType> configuredColumnTypeMap,
                                                   SchemaFetcher schemaFetcher) {
        Map<String, DataType> datasourceQueriedTypeMap = schemaFetcher.getColumnTypeMap();
        datasourceQueriedTypeMap = datasourceQueriedTypeMap == null
                ? new HashMap<>(configuredColumnTypeMap.size())
                : datasourceQueriedTypeMap;
        // 若有相同，配置的覆盖查出来的
        datasourceQueriedTypeMap.putAll(configuredColumnTypeMap);

        return datasourceQueriedTypeMap;
    }

    private List<String> copyNameList(List<String> copiedList, BiMap<String, String> columnMap, DataSourceRole role) {
        if (role == DataSourceRole.SOURCE) {
            columnMap = columnMap.inverse();
        }
        BiMap<String, String> finalColumnMap = columnMap;
        return copiedList.stream()
                .map(columnName -> finalColumnMap.getOrDefault(columnName, columnName))
                .collect(Collectors.toList());
    }

    /**
     * @param source
     * @param target
     * @param columnMap
     * @param role      表示谁是抄袭者
     * @return
     */
    private Map<String, DataType> copyTypeMap(Map<String, DataType> source, Map<String, DataType> target,
                                              BiMap<String, String> columnMap, DataSourceRole role) {
        // 被抄袭的
        Map<String, DataType> tmpSource;
        // 抄袭的
        Map<String, DataType> tmpTarget;
        CustomDataTypeManager<?, ?> targetManager;
        if (role == DataSourceRole.SOURCE) {
            columnMap = columnMap.inverse();
            tmpSource = new HashMap<>(target);
            tmpTarget = new HashMap<>(source);
            targetManager = sourceDataTypeManager;
        } else {
            tmpSource = new HashMap<>(source);
            tmpTarget = new HashMap<>(target);
            targetManager = targetDataTypeManager;
        }
        final BiMap<String, String> finalColumnMap = columnMap;
        // 被抄袭的列名先转一下
        tmpSource = tmpSource.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> finalColumnMap.getOrDefault(entry.getKey(), entry.getKey()),
                        Map.Entry::getValue));
        for (Map.Entry<String, DataType> entry : tmpSource.entrySet()) {
            String columnName = entry.getKey();
            DataType dataType = entry.getValue();
            // 抄袭者自己的答案优先
            if (!tmpTarget.containsKey(columnName)) {
                // 若抄袭者的custom type中包含被抄的类型的名字，则可以抄，否则抄关联基础类型
                // 这里的特殊类型名必须大小写都一致，越严格越好，ignoreCase仅用于对用户填写类型时的放宽要求
                // TODO 需要加个开关允许同名不同类特殊类型抄和warn提示抄了同名不同类特殊类型
                if (dataType.isCustom() && targetManager.contains(dataType.getName())) {
                    tmpTarget.put(columnName, targetManager.get(dataType.getName()));
                } else {
                    tmpTarget.put(columnName, dataType.getBaseDataType());
                }
            }
        }
        return tmpTarget;
    }

    private void fillMapWithColumnList(List<String> columnNameList, Map<String, String> columnMap) {
        for (String columnName : columnNameList) {
            columnMap.putIfAbsent(columnName, columnName);
        }
    }

    private List<String> intersect(List<String> a, List<String> b) {
        Set<String> c = new LinkedHashSet<>(a);
        c.retainAll(b);
        return new ArrayList<>(c);
    }

    private List<Set<String>> getIndex(List<Set<String>> configuredList, Function<Void, List<Set<String>>> fetchMethod) {
        // 若用户有配置，则直接使用用户配置的
        if (configuredList.size() != 0) {
            return configuredList;
        }
        List<Set<String>> res = fetchMethod.apply(null);
        return res == null ? new ArrayList<>(0) : res;
    }

    /**
     * @param list
     * @param columnMap
     * @param role      抄袭者
     * @return
     */
    private List<Set<String>> copyIndex(List<Set<String>> list, BiMap<String, String> columnMap, DataSourceRole role) {
        if (role == DataSourceRole.SOURCE) {
            columnMap = columnMap.inverse();
        }
        BiMap<String, String> finalColumnMap = columnMap;
        return list.stream()
                .map(set -> set.stream()
                        .map(columnName -> finalColumnMap.getOrDefault(columnName, columnName))
                        .collect(Collectors.toSet()))
                .collect(Collectors.toList());
    }

    /**
     * negotiate一对schema，可以是数据源-数据源、DER-SER、DER-数据源、数据源-SER，总之是在Mapper两侧的处理逻辑的schema需要negotiate
     *
     * @param sourceSchema
     * @param targetSchema
     * @param sourceSchemaFetcher
     * @param targetSchemaFetcher
     * @param sourceOptions
     * @param targetOptions
     * @param sourceContext
     * @param targetContext
     */
    private void negotiatePair(Schema sourceSchema, Schema targetSchema,
                               SchemaFetcher sourceSchemaFetcher, SchemaFetcher targetSchemaFetcher,
                               GenericOptions sourceOptions, GenericOptions targetOptions,
                               SchemaNegotiatorContext sourceContext, SchemaNegotiatorContext targetContext,
                               boolean sourceCopyKey, boolean targetCopyKey) {

        //+++++++++++++列名映射+++++++++++++//

        BiMap<String, String> biColumnMap
                = SchemaUtils.convertColumnMapFromOption(commonOptions.getJson(CommonOptionsConf.COLUMN_MAP, null));

        //-------------列名映射-------------//

        //+++++++++++++列名+++++++++++++//

        boolean fullColumnMap = commonOptions.getBoolean(CommonOptionsConf.RELIABLE_COLUMN_MAP, false);
        List<String> sourceColumnNameList = sourceSchema.getColumnNameList();
        List<String> targetColumnNameList = targetSchema.getColumnNameList();
        if (fullColumnMap) {
            // 都说好要依靠map来导了就不允许为空
            if (biColumnMap.size() == 0) {
                throw new RuntimeException(String.format("Why given a empty column map and specify it as a 'full column map'? " +
                        "If you want to transport full table, please not specify '--%s'.", CommonOptionsConf.RELIABLE_COLUMN_MAP));
            }

            // 取交集
            List<String> intersect = intersect(sourceColumnNameList, targetColumnNameList);
            sourceColumnNameList = intersect;
            targetColumnNameList = intersect;

            fillMapWithColumnList(sourceColumnNameList, biColumnMap);
            fillMapWithColumnList(targetColumnNameList, biColumnMap.inverse());
            sourceColumnNameList = new ArrayList<>(biColumnMap.keySet());
            targetColumnNameList = new ArrayList<>(biColumnMap.values());
        } else {
            sourceColumnNameList = getColumnNameList(sourceColumnNameList, sourceSchemaFetcher);
            targetColumnNameList = getColumnNameList(targetColumnNameList, targetSchemaFetcher);
        }

        sourceContext.afterReadColumnNameList(sourceColumnNameList);
        targetContext.afterReadColumnNameList(targetColumnNameList);

        if (sourceColumnNameList.size() > 0 && targetColumnNameList.size() > 0) {
            List<String> beforeChangedSourceColumnNameList = new ArrayList<>(sourceColumnNameList);
            List<String> beforeChangedTargetColumnNameList = new ArrayList<>(targetColumnNameList);

            // 检查上下游多列少列，并取交集
            Set<String> intersectTargetColumnNameSet
                    = validateAndGetIntersection(sourceColumnNameList, targetColumnNameList, biColumnMap);
            // 筛选出公共列并保持原本顺序
            sourceColumnNameList = sourceColumnNameList.stream()
                    .filter(columnName
                            -> intersectTargetColumnNameSet.contains(biColumnMap.getOrDefault(columnName, columnName)))
                    .collect(Collectors.toList());
            targetColumnNameList = targetColumnNameList.stream()
                    .filter(intersectTargetColumnNameSet::contains)
                    .collect(Collectors.toList());

            sourceContext.afterIntersectColumnNameList(beforeChangedSourceColumnNameList, sourceColumnNameList);
            targetContext.afterIntersectColumnNameList(beforeChangedTargetColumnNameList, targetColumnNameList);
        }

        if (commonOptions.getBoolean(CommonOptionsConf.ALLOW_COPY_COLUMN_NAME, false)) {
            if (sourceColumnNameList.size() == 0 && targetColumnNameList.size() != 0) {
                LOG.info("The source data source copy the column name list.");
                sourceColumnNameList = copyNameList(targetColumnNameList, biColumnMap, DataSourceRole.SOURCE);
                sourceContext.afterCopyColumnNameList(sourceColumnNameList);
            } else if (sourceColumnNameList.size() != 0 && targetColumnNameList.size() == 0) {
                LOG.info("The target data source copy the column name list.");
                targetColumnNameList = copyNameList(sourceColumnNameList, biColumnMap, DataSourceRole.TARGET);
                targetContext.afterCopyColumnNameList(targetColumnNameList);
            }
        }

        LOG.info("The source column name list is: " + sourceColumnNameList);
        LOG.info("The target column name list is: " + targetColumnNameList);

        //-------------列名-------------//
        //+++++++++++++列类型+++++++++++++//

        Map<String, DataType> sourceColumnTypeMap = sourceSchema.getColumnTypeMap();
        Map<String, DataType> targetColumnTypeMap = targetSchema.getColumnTypeMap();
        sourceColumnTypeMap = getColumnTypeMap(sourceColumnTypeMap, sourceSchemaFetcher);
        targetColumnTypeMap = getColumnTypeMap(targetColumnTypeMap, targetSchemaFetcher);

        sourceContext.afterReadColumnTypeMap(sourceColumnNameList, sourceColumnTypeMap);
        targetContext.afterReadColumnTypeMap(targetColumnNameList, targetColumnTypeMap);

        if (commonOptions.getBoolean(CommonOptionsConf.ALLOW_COPY_COLUMN_TYPE, false)) {
            Map<String, DataType> beforeChangedSourceColumnTypeMap = new HashMap<>(sourceColumnTypeMap);
            Map<String, DataType> beforeChangedTargetColumnTypeMap = new HashMap<>(targetColumnTypeMap);

            // 临时变量用于在source给target抄的时候处于无污染状态
            Map<String, DataType> tmpSourceColumnTypeMap = copyTypeMap(sourceColumnTypeMap, targetColumnTypeMap,
                    biColumnMap, DataSourceRole.SOURCE);
            Map<String, DataType> tmpTargetColumnTypeMap = copyTypeMap(sourceColumnTypeMap,
                    targetColumnTypeMap, biColumnMap, DataSourceRole.TARGET);

            sourceColumnTypeMap = tmpSourceColumnTypeMap;
            targetColumnTypeMap = tmpTargetColumnTypeMap;
            // copy必是添加元素，故只需要比大小便可分辨是否copy
            if (beforeChangedSourceColumnTypeMap.size() != sourceColumnTypeMap.size()) {
                sourceContext.afterCopyColumnTypeMap(sourceColumnNameList, beforeChangedSourceColumnTypeMap, sourceColumnTypeMap);
            }
            if (beforeChangedTargetColumnTypeMap.size() != targetColumnTypeMap.size()) {
                targetContext.afterCopyColumnTypeMap(targetColumnNameList, beforeChangedTargetColumnTypeMap, targetColumnTypeMap);
            }
        }

        LOG.info("The source column type map is: " + sourceColumnTypeMap);
        LOG.info("The target column type map is: " + targetColumnTypeMap);

        //-------------列类型-------------//
        //+++++++++++++索引 唯一键+++++++++++++//

        List<Set<String>> sourceIndexGroupList = sourceSchema.getIndexGroupList();
        List<Set<String>> targetIndexGroupList = targetSchema.getIndexGroupList();
        sourceIndexGroupList = getIndex(sourceIndexGroupList, aVoid -> sourceSchemaFetcher.getIndexGroupList());
        sourceContext.afterReadIndexGroupList(sourceIndexGroupList);
        targetIndexGroupList = getIndex(targetIndexGroupList, aVoid -> targetSchemaFetcher.getIndexGroupList());
        targetContext.afterReadIndexGroupList(targetIndexGroupList);

        if (sourceCopyKey && CollectionUtils.isEmpty(sourceIndexGroupList) && !CollectionUtils.isEmpty(targetIndexGroupList)) {
            sourceIndexGroupList = copyIndex(targetIndexGroupList, biColumnMap, DataSourceRole.SOURCE);
        }
        if (targetCopyKey && CollectionUtils.isEmpty(targetIndexGroupList) && !CollectionUtils.isEmpty(sourceIndexGroupList)) {
            targetIndexGroupList = copyIndex(sourceIndexGroupList, biColumnMap, DataSourceRole.TARGET);
        }

        LOG.info("The source index group is: " + sourceIndexGroupList);
        LOG.info("The target index group is: " + targetIndexGroupList);

        List<Set<String>> sourceUniqueKeyGroupList = sourceSchema.getUniqueKeyGroupList();
        List<Set<String>> targetUniqueKeyGroupList = targetSchema.getUniqueKeyGroupList();
        sourceUniqueKeyGroupList = getIndex(sourceUniqueKeyGroupList, aVoid -> sourceSchemaFetcher.getUniqueKeyGroupList());
        sourceContext.afterReadUniqueKeyGroupList(sourceUniqueKeyGroupList);
        targetUniqueKeyGroupList = getIndex(targetUniqueKeyGroupList, aVoid -> targetSchemaFetcher.getUniqueKeyGroupList());
        targetContext.afterReadUniqueKeyGroupList(targetUniqueKeyGroupList);

        if (sourceCopyKey && CollectionUtils.isEmpty(sourceUniqueKeyGroupList) && !CollectionUtils.isEmpty(targetUniqueKeyGroupList)) {
            sourceUniqueKeyGroupList = copyIndex(targetUniqueKeyGroupList, biColumnMap, DataSourceRole.SOURCE);
        }
        if (targetCopyKey && CollectionUtils.isEmpty(targetUniqueKeyGroupList) && !CollectionUtils.isEmpty(sourceUniqueKeyGroupList)) {
            targetUniqueKeyGroupList = copyIndex(sourceUniqueKeyGroupList, biColumnMap, DataSourceRole.TARGET);
        }

        LOG.info("The source unique key group is: " + sourceUniqueKeyGroupList);
        LOG.info("The target unique key group is: " + targetUniqueKeyGroupList);

        //-------------索引 唯一键-------------//

        // 塞回options
        sourceSchema.setColumnNameList(sourceColumnNameList);
        sourceSchema.setColumnTypeMap(sourceColumnTypeMap);
        sourceSchema.setIndexGroupList(sourceIndexGroupList);
        sourceSchema.setUniqueKeyGroupList(sourceUniqueKeyGroupList);
        targetSchema.setColumnNameList(targetColumnNameList);
        targetSchema.setColumnTypeMap(targetColumnTypeMap);
        targetSchema.setIndexGroupList(targetIndexGroupList);
        targetSchema.setUniqueKeyGroupList(targetUniqueKeyGroupList);

        sourceSchema.toOptions(sourceOptions);
        targetSchema.toOptions(targetOptions);

        sourceContext.afterAll(sourceColumnNameList, sourceColumnTypeMap);
        targetContext.afterAll(targetColumnNameList, targetColumnTypeMap);
    }

    public void negotiate() {
        SchemaFetcher derFetcher = new BaseSchemaFetcher(derOptions) {
            @Override
            protected List<String> innerGetColumnNameList() {
                return null;
            }

            @Override
            protected Map<String, DataType> innerGetColumnTypeMap() {
                return null;
            }
        };
        SchemaFetcher serFetcher = new BaseSchemaFetcher(serOptions) {
            @Override
            protected List<String> innerGetColumnNameList() {
                return null;
            }

            @Override
            protected Map<String, DataType> innerGetColumnTypeMap() {
                return null;
            }
        };
        if (derSchema == null && serSchema == null) {
            // 大家都是数据源了，key信息只有自己的才是真的，不抄
            negotiatePair(
                    sourceSchema, targetSchema,
                    sourceSchemaFetcher, targetSchemaFetcher,
                    sourceOptions, targetOptions,
                    sourceContext, targetContext,
                    false, false
            );
        } else if (derSchema == null) {
            negotiatePair(
                    sourceSchema, serSchema,
                    sourceSchemaFetcher, serFetcher,
                    sourceOptions, serOptions,
                    sourceContext, serContext,
                    false, true
            );
        } else if (serSchema == null) {
            negotiatePair(
                    derSchema, targetSchema,
                    derFetcher, targetSchemaFetcher,
                    derOptions, targetOptions,
                    derContext, targetContext,
                    true, false
            );
        } else {
            negotiatePair(
                    derSchema, serSchema,
                    derFetcher, serFetcher,
                    derOptions, serOptions,
                    derContext, serContext,
                    true, true
            );
        }
    }
}
