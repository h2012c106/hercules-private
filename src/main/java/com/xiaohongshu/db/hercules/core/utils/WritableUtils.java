package com.xiaohongshu.db.hercules.core.utils;

import com.xiaohongshu.db.hercules.core.option.BaseDataSourceOptionsConf;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.datatype.BaseWrapper;
import com.xiaohongshu.db.hercules.core.serialize.datatype.MapWrapper;
import com.xiaohongshu.db.hercules.core.serialize.datatype.NullWrapper;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class WritableUtils {

    public static List<String> splitColumn(String column) {
        return Arrays.asList(column.split(BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER_REGEX));
    }

    public static String concatColumn(String columnNameListStr, String column) {
        return StringUtils.isEmpty(columnNameListStr)
                ? column
                : columnNameListStr + BaseDataSourceOptionsConf.NESTED_COLUMN_NAME_DELIMITER_REGEX + column;
    }

    public static BaseWrapper get(MapWrapper wrapper, List<String> columnNameList) {
        MapWrapper tmpMapWrapper = wrapper;
        int i;
        for (i = 0; i < columnNameList.size() - 1; ++i) {
            String columnName = columnNameList.get(i);
            BaseWrapper tmpWrapper = tmpMapWrapper.get(columnName);
            // 如果不包含这列或者这列不是Map，说明不存在
            if (!(tmpWrapper instanceof MapWrapper)) {
                return null;
            }
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        return tmpMapWrapper.get(columnNameList.get(i));
    }

    public static void put(MapWrapper wrapper, List<String> columnNameList, @NonNull BaseWrapper value) {
        MapWrapper tmpMapWrapper = wrapper;
        int i;
        for (i = 0; i < columnNameList.size() - 1; ++i) {
            String columnName = columnNameList.get(i);
            BaseWrapper tmpWrapper = tmpMapWrapper.get(columnName);
            // 如果已经有值但不是Map类型的直接覆盖
            if (!(tmpWrapper instanceof MapWrapper)) {
                tmpWrapper = new MapWrapper();
                tmpMapWrapper.put(columnName, tmpWrapper);
            }
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        tmpMapWrapper.put(columnNameList.get(i), value);
    }

    public static BaseWrapper remove(MapWrapper wrapper, List<String> columnNameList) {
        MapWrapper tmpMapWrapper = wrapper;
        int i;
        for (i = 0; i < columnNameList.size() - 1; ++i) {
            String columnName = columnNameList.get(i);
            BaseWrapper tmpWrapper = tmpMapWrapper.get(columnName);
            // 如果不包含这列或者这列不是Map，说明不存在
            if (!(tmpWrapper instanceof MapWrapper)) {
                return null;
            }
            tmpMapWrapper = (MapWrapper) tmpWrapper;
        }
        return tmpMapWrapper.remove(columnNameList.get(i));
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
            List<String> fromList = splitColumn(from);
            List<String> toList = splitColumn(to);
            BaseWrapper fromValue = remove(baseWrapper, fromList);
            // MapWrapper里不可能存null值，所以null一定代表不存在
            if (fromValue != null) {
                missionList.add(new ConvertMission(fromValue, toList));
            }
        }
        for (ConvertMission mission : missionList) {
            put(baseWrapper, mission.getToList(), mission.getWrapper());
        }
    }

    public static MapWrapper copyColumn(MapWrapper in, List<String> whiteNameList, FilterUnexistOption option) {
        MapWrapper out = new MapWrapper(whiteNameList.size());
        for (String columnName : whiteNameList) {
            List<String> splitColumnName = splitColumn(columnName);
            BaseWrapper value = get(in, splitColumnName);
            // MapWrapper里不可能存null值，所以null一定代表不存在
            if (value == null) {
                if (option == FilterUnexistOption.IGNORE) {
                    continue;
                } else if (option == FilterUnexistOption.THROW) {
                    throw new RuntimeException(String.format("Column [%s] must exist in the data map.", columnName));
                } else if (option == FilterUnexistOption.DEFAULT_NULL_VALUE) {
                    value = NullWrapper.INSTANCE;
                }
            }
            put(out, splitColumnName, value);
        }
        return out;
    }

    public static void filterColumn(MapWrapper in, List<String> blackNameList){
        for (String columnName : blackNameList) {
            List<String> splitColumnName = splitColumn(columnName);
            remove(in, splitColumnName);
        }
    }

    private static class ConvertMission {
        private BaseWrapper wrapper;
        private List<String> toList;

        public ConvertMission(BaseWrapper wrapper, List<String> toList) {
            this.wrapper = wrapper;
            this.toList = toList;
        }

        public BaseWrapper getWrapper() {
            return wrapper;
        }

        public List<String> getToList() {
            return toList;
        }
    }

    public enum FilterUnexistOption {
        IGNORE,
        THROW,
        DEFAULT_NULL_VALUE;
    }
}
