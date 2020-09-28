package com.xiaohongshu.db.hercules.core.utils;

import com.xiaohongshu.db.hercules.core.option.optionsconf.datasource.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.MapWrapper;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.NullWrapper;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class WritableUtils {

    /**
     * List->ListWrapper企图复用WrapperSetter的无奈之举，不然需要写一大长串DataType的switch case去Object转Wrapper
     */
    public static final String FAKE_COLUMN_NAME_USED_BY_LIST = "FAKE_NAME";
    /**
     * 用这个假document名的目的在于防止list内元素同主map元素同名导致类型会从column type map抄错
     */
    public static final String FAKE_PARENT_NAME_USED_BY_LIST = "123###LIST###321";

    /**
     * @param column
     * @return 切分好中间节点与叶子结点的结果
     */
    public static ColumnSplitResult splitColumnWrapped(String column) {
        if (StringUtils.isEmpty(column)) {
            throw new RuntimeException("The column name cannot be empty.");
        }
        List<String> res = Arrays.asList(column.split(BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER_REGEX));
        return new ColumnSplitResult(res.get(res.size() - 1), res.subList(0, res.size() - 1));
    }

    /**
     * @param column
     * @return 原始结果
     */
    public static List<String> splitColumnRaw(String column) {
        if (StringUtils.isEmpty(column)) {
            throw new RuntimeException("The column name cannot be empty.");
        }
        return Arrays.asList(column.split(BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER_REGEX));
    }

    public static class ColumnSplitResult {
        private String finalColumn;
        private List<String> parentColumnList;

        public ColumnSplitResult(String finalColumn, List<String> parentColumnList) {
            this.finalColumn = finalColumn;
            this.parentColumnList = parentColumnList;
        }

        public String getFinalColumn() {
            return finalColumn;
        }

        public List<String> getParentColumnList() {
            return parentColumnList;
        }
    }

    public static String concatColumn(List<String> list) {
        return String.join(BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER, list);
    }

    public static String concatColumn(String columnNameListStr, String column) {
        return StringUtils.isEmpty(columnNameListStr)
                ? column
                : columnNameListStr + BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER + column;
    }

    public static BaseWrapper<?> get(MapWrapper wrapper, @NonNull String columnName) {
        List<String> columnEmbedList = splitColumnRaw(columnName);
        MapWrapper tmpMapWrapper = wrapper;
        int i;
        for (i = 0; i < columnEmbedList.size() - 1; ++i) {
            String tmpColumnName = columnEmbedList.get(i);
            BaseWrapper<?> tmpWrapper = tmpMapWrapper.get(tmpColumnName);
            // 如果不包含这列或者这列不是Map，说明不存在
            if (!(tmpWrapper instanceof MapWrapper)) {
                return null;
            }
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        return tmpMapWrapper.get(columnEmbedList.get(i));
    }

    public static void put(MapWrapper wrapper, @NonNull String columnName, @NonNull BaseWrapper<?> value) {
        List<String> columnEmbedList = splitColumnRaw(columnName);
        MapWrapper tmpMapWrapper = wrapper;
        int i;
        for (i = 0; i < columnEmbedList.size() - 1; ++i) {
            String tmpColumnName = columnEmbedList.get(i);
            BaseWrapper<?> tmpWrapper = tmpMapWrapper.get(tmpColumnName);
            // 如果已经有值但不是Map类型的直接覆盖
            if (!(tmpWrapper instanceof MapWrapper)) {
                tmpWrapper = new MapWrapper();
                tmpMapWrapper.put(tmpColumnName, tmpWrapper);
            }
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        tmpMapWrapper.put(columnEmbedList.get(i), value);
    }

    public static BaseWrapper<?> remove(MapWrapper wrapper, @NonNull String columnName) {
        List<String> columnEmbedList = splitColumnRaw(columnName);
        MapWrapper tmpMapWrapper = wrapper;
        int i;
        for (i = 0; i < columnEmbedList.size() - 1; ++i) {
            String tmpColumnName = columnEmbedList.get(i);
            BaseWrapper<?> tmpWrapper = tmpMapWrapper.get(tmpColumnName);
            // 如果不包含这列或者这列不是Map，说明不存在
            if (!(tmpWrapper instanceof MapWrapper)) {
                return null;
            }
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        return tmpMapWrapper.remove(columnEmbedList.get(i));
    }

    /**
     * 就地操作。先把source删干净，再加。
     *
     * @param in
     * @param columnMap
     */
    public static void convertColumnName(HerculesWritable in, Map<String, String> columnMap) {
        List<ConvertMission> missionList = new ArrayList<>(columnMap.size());
        MapWrapper baseWrapper = in.getRow();
        for (Map.Entry<String, String> entry : columnMap.entrySet()) {
            String from = entry.getKey();
            String to = entry.getValue();
            BaseWrapper<?> fromValue = remove(baseWrapper, from);
            // MapWrapper里不可能存null值，所以null一定代表不存在
            if (fromValue != null) {
                missionList.add(new ConvertMission(fromValue, to));
            }
        }
        for (ConvertMission mission : missionList) {
            put(baseWrapper, mission.getTo(), mission.getWrapper());
        }
    }

    public static MapWrapper copyColumn(MapWrapper in, List<String> whiteNameList, @NonNull FilterUnexistOption option) {
        MapWrapper out = new MapWrapper(whiteNameList.size());
        for (String columnName : whiteNameList) {
            BaseWrapper<?> value = get(in, columnName);
            // MapWrapper里不可能存null值，所以null一定代表不存在
            if (value == null) {
                if (option == FilterUnexistOption.IGNORE) {
                    continue;
                } else if (option == FilterUnexistOption.EXCEPTION) {
                    throw new RuntimeException(String.format("Column [%s] must exist in the data map: %s", columnName, in));
                } else if (option == FilterUnexistOption.DEFAULT_NULL_VALUE) {
                    value = NullWrapper.INSTANCE;
                }
            }
            put(out, columnName, value);
        }
        return out;
    }

    public static HerculesWritable copyColumn(HerculesWritable in, List<String> whiteNameList, @NonNull FilterUnexistOption option) {
        HerculesWritable res = new HerculesWritable(copyColumn(in.getRow(), whiteNameList, option));
        res.addAllWriteStrategy(in.getWriteStrategyList());
        return res;
    }

    public static void filterColumn(MapWrapper in, List<String> blackNameList) {
        for (String columnName : blackNameList) {
            remove(in, columnName);
        }
    }

    private static class ConvertMission {
        private BaseWrapper<?> wrapper;
        private String to;

        public ConvertMission(BaseWrapper<?> wrapper, String to) {
            this.wrapper = wrapper;
            this.to = to;
        }

        public BaseWrapper<?> getWrapper() {
            return wrapper;
        }

        public String getTo() {
            return to;
        }
    }

    public enum FilterUnexistOption {
        IGNORE,
        EXCEPTION,
        DEFAULT_NULL_VALUE;
    }
}
