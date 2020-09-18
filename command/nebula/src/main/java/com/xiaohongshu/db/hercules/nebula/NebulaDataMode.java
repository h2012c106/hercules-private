package com.xiaohongshu.db.hercules.nebula;

import com.xiaohongshu.db.hercules.core.exception.ParseException;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.xiaohongshu.db.hercules.nebula.option.NebulaOptionsConf.*;

public enum NebulaDataMode {
    VERTEX("TAG", "VERTEX",
            new BiFunction<GenericOptions, Map<String, Long>, String>() {
                @Override
                public String apply(GenericOptions options, Map<String, Long> stringLongMap) {
                    String vertexIdColumn = options.getString(VERTEX_ID_COLUMN, null);
                    Long value = stringLongMap.get(vertexIdColumn);
                    if (value != null) {
                        return String.valueOf(value);
                    } else {
                        throw new RuntimeException("Not given vertex id for vertex inserting.");
                    }
                }
            },
            new Function<GenericOptions, String[]>() {
                @Override
                public String[] apply(GenericOptions options) {
                    return new String[]{options.getString(VERTEX_ID_COLUMN, null)};
                }
            }
    ),
    EDGE("EDGE", "EDGE",
            new BiFunction<GenericOptions, Map<String, Long>, String>() {
                @Override
                public String apply(GenericOptions options, Map<String, Long> stringLongMap) {
                    String sourceVertexIdColumn = options.getString(EDGE_SOURCE_ID_COLUMN, null);
                    String targetVertexIdColumn = options.getString(EDGE_TARGET_ID_COLUMN, null);
                    Long sourceValue = stringLongMap.get(sourceVertexIdColumn);
                    Long targetValue = stringLongMap.get(targetVertexIdColumn);
                    if (sourceValue != null && targetValue != null) {
                        return sourceValue + " -> " + targetValue;
                    } else {
                        throw new RuntimeException("Not given source/target vertex id for edge inserting.");
                    }
                }
            },
            new Function<GenericOptions, String[]>() {
                @Override
                public String[] apply(GenericOptions options) {
                    return new String[]{
                            options.getString(EDGE_SOURCE_ID_COLUMN, null),
                            options.getString(EDGE_TARGET_ID_COLUMN, null)
                    };
                }
            }
    );

    private final String descName;
    private final String insertName;
    private final BiFunction<GenericOptions, Map<String, Long>, String> getInsertRowHeadFunction;
    private final Function<GenericOptions, String[]> getVidNamesFunction;

    NebulaDataMode(String descName, String insertName, BiFunction<GenericOptions, Map<String, Long>, String> getInsertRowHeadFunction, Function<GenericOptions, String[]> getVidNamesFunction) {
        this.descName = descName;
        this.insertName = insertName;
        this.getInsertRowHeadFunction = getInsertRowHeadFunction;
        this.getVidNamesFunction = getVidNamesFunction;
    }

    public String getDescName() {
        return descName;
    }

    public String getInsertName() {
        return insertName;
    }

    public BiFunction<GenericOptions, Map<String, Long>, String> getGetInsertRowHeadFunction() {
        return getInsertRowHeadFunction;
    }

    public Function<GenericOptions, String[]> getGetVidNamesFunction() {
        return getVidNamesFunction;
    }

    public static NebulaDataMode valueOfIgnoreCase(String value) {
        for (NebulaDataMode mode : NebulaDataMode.values()) {
            if (StringUtils.equalsIgnoreCase(mode.name(), value)) {
                return mode;
            }
        }
        throw new ParseException("Illegal data type: " + value);
    }
}
