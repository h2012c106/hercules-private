package com.xiaohongshu.db.hercules.core.schema;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.xiaohongshu.db.hercules.common.option.CommonOptionsConf;
import com.xiaohongshu.db.hercules.core.datasource.DataSourceRole;
import com.xiaohongshu.db.hercules.core.datatype.BaseCustomDataTypeManager;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.exception.SchemaException;
import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.WrappingOptions;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 负责对齐上下游列名List、列类型Map，并且把结果写入options中，在map阶段不需要二次对齐/获取，保证schema信息全局取一次。
 * 经过这个对象处理，所有schema信息均通过option取。
 *
 * @author huanghanxiang
 */
public final class SchemaNegotiator {

    private static final Log LOG = LogFactory.getLog(SchemaNegotiator.class);

    private WrappingOptions options;
    private BaseSchemaFetcher sourceSchemaFetcher;
    private BaseSchemaFetcher targetSchemaFetcher;
    private SchemaNegotiatorContext sourceContext;
    private SchemaNegotiatorContext targetContext;

    public SchemaNegotiator(WrappingOptions options,
                            BaseSchemaFetcher sourceSchemaFetcher, BaseSchemaFetcher targetSchemaFetcher,
                            SchemaNegotiatorContext sourceContext, SchemaNegotiatorContext targetContext) {
        this.options = options;
        this.sourceSchemaFetcher = sourceSchemaFetcher;
        this.targetSchemaFetcher = targetSchemaFetcher;
        this.sourceContext = sourceContext;
        this.sourceContext.setSchemaFetcher(this.sourceSchemaFetcher);
        this.targetContext = targetContext;
        this.targetContext.setSchemaFetcher(this.targetSchemaFetcher);
    }

    private List<String> getColumnNameListFromOptions(GenericOptions dataSourceOptions) {
        return Arrays.asList(dataSourceOptions.getStringArray(BaseDataSourceOptionsConf.COLUMN, new String[0]));
    }

    private List<String> getColumnNameListFromDataSource(BaseSchemaFetcher schemaFetcher) {
        List<String> res = schemaFetcher.getColumnNameList();
        return res == null ? new ArrayList<>(0) : res;
    }

    private List<String> getColumnNameList(GenericOptions dataSourceOptions, BaseSchemaFetcher schemaFetcher) {
        List<String> configuredColumnList = getColumnNameListFromOptions(dataSourceOptions);
        // 若用户有配置，则直接使用用户配置的
        if (configuredColumnList.size() != 0) {
            return configuredColumnList;
        }
        return getColumnNameListFromDataSource(schemaFetcher);
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
            if (!options.getCommonOptions().getBoolean(CommonOptionsConf.ALLOW_SOURCE_MORE_COLUMN, false)) {
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
            if (!options.getCommonOptions().getBoolean(CommonOptionsConf.ALLOW_TARGET_MORE_COLUMN, false)) {
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

    private Map<String, DataType> getColumnTypeMap(GenericOptions datasourceOptions,
                                                   BaseSchemaFetcher schemaFetcher,
                                                   List<String> columnNameList) {
        // 搞到需要列类型的完整集合，这个集合应该不仅包含需要同步的列，还需要一些别的（数据源相关配置）。
        Set<String> additionalNeedTypeColumnNameSet = schemaFetcher.getAdditionalNeedTypeColumn();
        Set<String> needTypeColumnNameSet = new HashSet<>(additionalNeedTypeColumnNameSet.size() + columnNameList.size());
        needTypeColumnNameSet.addAll(additionalNeedTypeColumnNameSet);
        needTypeColumnNameSet.addAll(columnNameList);

        Map<String, DataType> res = SchemaUtils.convert(datasourceOptions.getJson(BaseDataSourceOptionsConf.COLUMN_TYPE, new JSONObject()),
                schemaFetcher.getCustomDataTypeManager());
        // 如果用户全部指定，则返回就完事儿了
        if (res.keySet().containsAll(needTypeColumnNameSet)) {
            return res;
        }

        // 优先使用用户配置的，剩下不够的未知的再去数据源搞
        needTypeColumnNameSet.removeAll(res.keySet());
        Map<String, DataType> datasourceQueriedTypeMap = schemaFetcher.getColumnTypeMap(needTypeColumnNameSet);
        datasourceQueriedTypeMap = datasourceQueriedTypeMap == null
                ? new HashMap<>(res.size())
                : datasourceQueriedTypeMap;
        // 若有相同，配置的覆盖查出来的
        datasourceQueriedTypeMap.putAll(res);

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
        BaseCustomDataTypeManager targetManager;
        if (role == DataSourceRole.SOURCE) {
            columnMap = columnMap.inverse();
            tmpSource = new HashMap<>(target);
            tmpTarget = new HashMap<>(source);
            targetManager = sourceSchemaFetcher.getCustomDataTypeManager();
        } else {
            tmpSource = new HashMap<>(source);
            tmpTarget = new HashMap<>(target);
            targetManager = targetSchemaFetcher.getCustomDataTypeManager();
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
                // 若抄袭者的custom type中包含被抄的类型，则可以抄，否则抄关联基础类型
                if (dataType.isCustom() && targetManager.contains(dataType.getClass())) {
                    tmpTarget.put(columnName, dataType);
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

    public void negotiate() {
        GenericOptions sourceOptions = options.getSourceOptions();
        GenericOptions targetOptions = options.getTargetOptions();
        GenericOptions commonOptions = options.getCommonOptions();

        JSONObject columnMap = commonOptions.getJson(CommonOptionsConf.COLUMN_MAP, null);
        // 源列名->目标列名
        BiMap<String, String> biColumnMap = HashBiMap.create(columnMap.size());
        for (String key : columnMap.keySet()) {
            // 如果存在相同的value则bimap会报错
            biColumnMap.put(key, columnMap.getString(key));
        }

        boolean fullColumnMap = commonOptions.getBoolean(CommonOptionsConf.RELIABLE_COLUMN_MAP, false);
        List<String> sourceColumnNameList;
        List<String> targetColumnNameList;
        if (fullColumnMap) {
            // 都说好要依靠map来导了就不允许为空
            if (biColumnMap.size() == 0) {
                throw new RuntimeException(String.format("Why given a empty column map and specify it as a 'full column map'? " +
                        "If you want to transport full table, please not specify '--%s'.", CommonOptionsConf.RELIABLE_COLUMN_MAP));
            }
            sourceColumnNameList = getColumnNameListFromOptions(sourceOptions);
            targetColumnNameList = getColumnNameListFromOptions(targetOptions);
            fillMapWithColumnList(sourceColumnNameList, biColumnMap);
            fillMapWithColumnList(targetColumnNameList, biColumnMap.inverse());
            sourceColumnNameList = new ArrayList<>(biColumnMap.keySet());
            targetColumnNameList = new ArrayList<>(biColumnMap.values());
        } else {
            sourceColumnNameList = getColumnNameList(sourceOptions, sourceSchemaFetcher);
            targetColumnNameList = getColumnNameList(targetOptions, targetSchemaFetcher);
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

        Map<String, DataType> sourceColumnTypeMap
                = getColumnTypeMap(sourceOptions, sourceSchemaFetcher, sourceColumnNameList);
        Map<String, DataType> targetColumnTypeMap
                = getColumnTypeMap(targetOptions, targetSchemaFetcher, targetColumnNameList);

        sourceContext.afterReadColumnTypeMap(sourceColumnNameList, sourceColumnTypeMap);
        targetContext.afterReadColumnTypeMap(targetColumnNameList, targetColumnTypeMap);

        if (commonOptions.getBoolean(CommonOptionsConf.ALLOW_COPY_COLUMN_TYPE, false)) {
            Map<String, DataType> beforeChangedSourceColumnTypeMap = new HashMap<>(sourceColumnTypeMap);
            Map<String, DataType> beforeChangedTargetColumnTypeMap = new HashMap<>(sourceColumnTypeMap);

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

        // 塞column name list
        sourceOptions.set(BaseDataSourceOptionsConf.COLUMN, sourceColumnNameList.toArray(new String[0]));
        targetOptions.set(BaseDataSourceOptionsConf.COLUMN, targetColumnNameList.toArray(new String[0]));

        // 塞column type map
        sourceOptions.set(BaseDataSourceOptionsConf.COLUMN_TYPE,
                SchemaUtils.convert(sourceColumnTypeMap).toJSONString());
        targetOptions.set(BaseDataSourceOptionsConf.COLUMN_TYPE,
                SchemaUtils.convert(targetColumnTypeMap).toJSONString());

        sourceContext.afterAll(sourceColumnNameList, sourceColumnTypeMap);
        targetContext.afterAll(targetColumnNameList, targetColumnTypeMap);
    }
}