package com.xiaohongshu.db.hercules.nebula.option;

import com.alibaba.fastjson.JSONObject;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.option.SingleOptionConf;
import com.xiaohongshu.db.hercules.core.option.optionsconf.BaseOptionsConf;
import com.xiaohongshu.db.hercules.core.utils.SchemaUtils;
import com.xiaohongshu.db.hercules.nebula.NebulaDataMode;
import com.xiaohongshu.db.hercules.nebula.datatype.VidCustomDataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.COLUMN;
import static com.xiaohongshu.db.hercules.core.option.optionsconf.TableOptionsConf.COLUMN_TYPE;

public class NebulaOptionsConf extends BaseOptionsConf {

    public static final String ADDRESSES = "addresses";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String SPACE = "space";
    public static final String TABLE = "table";

    public static final String MODE = "mode";
    public static final String VERTEX_ID_COLUMN = "vertex-id-column";
    public static final String EDGE_SOURCE_ID_COLUMN = "edge-source-id-column";
    public static final String EDGE_TARGET_ID_COLUMN = "edge-target-id-column";

    @Override
    protected List<BaseOptionsConf> generateAncestorList() {
        return null;
    }

    @Override
    protected List<SingleOptionConf> innerGenerateOptionConf() {
        List<SingleOptionConf> tmpList = new ArrayList<>();
        tmpList.add(SingleOptionConf.builder()
                .name(ADDRESSES)
                .needArg(true)
                .necessary(true)
                .description("The nebula engine connection url, delimited by: ,")
                .list(true)
                .validateFunction(SingleOptionConf.NOT_EMPTY)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(USER)
                .needArg(true)
                .necessary(true)
                .description("The database username.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(PASSWORD)
                .needArg(true)
                .necessary(true)
                .description("The database password.")
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(SPACE)
                .needArg(true)
                .necessary(true)
                .description("The database space.")
                .validateFunction(SingleOptionConf.NOT_EMPTY)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(TABLE)
                .needArg(true)
                .necessary(true)
                .description("The table name.")
                .validateFunction(SingleOptionConf.NOT_EMPTY)
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(MODE)
                .needArg(true)
                .necessary(true)
                .description("The nebula data mode (case-ignored): " + Arrays.stream(NebulaDataMode.values()).map(Enum::name).collect(Collectors.joining(",")))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(VERTEX_ID_COLUMN)
                .needArg(true)
                .description(String.format("The vertex id column name, only needed when mode as %s.", NebulaDataMode.VERTEX))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(EDGE_SOURCE_ID_COLUMN)
                .needArg(true)
                .description(String.format("The edge source id column name, only needed when mode as %s.", NebulaDataMode.EDGE))
                .build());
        tmpList.add(SingleOptionConf.builder()
                .name(EDGE_TARGET_ID_COLUMN)
                .needArg(true)
                .description(String.format("The edge target id column name, only needed when mode as %s.", NebulaDataMode.EDGE))
                .build());
        return tmpList;
    }

    @Override
    protected void innerValidateOptions(GenericOptions options) {
        NebulaDataMode mode = NebulaDataMode.valueOfIgnoreCase(options.getString(MODE, null));
        switch (mode) {
            case VERTEX:
                if (!options.hasProperty(VERTEX_ID_COLUMN)) {
                    throw new RuntimeException(String.format("Must specify --%s when using <%s> mode.", VERTEX_ID_COLUMN, NebulaDataMode.VERTEX));
                }
                break;
            case EDGE:
                if (!options.hasProperty(EDGE_SOURCE_ID_COLUMN) || !options.hasProperty(EDGE_TARGET_ID_COLUMN)) {
                    throw new RuntimeException(String.format("Must specify --%s and --%s when using <%s> mode.", EDGE_SOURCE_ID_COLUMN, EDGE_TARGET_ID_COLUMN, NebulaDataMode.EDGE));
                }
                break;
            default:
        }
    }


    /**
     * 苟！
     *
     * @param options
     */
    @Override
    protected void innerProcessOptions(GenericOptions options) {
        NebulaDataMode mode = NebulaDataMode.valueOfIgnoreCase(options.getString(MODE, null));
        List<String> columnList = SchemaUtils.convertNameFromOption(options.getTrimmedStringArray(COLUMN, new String[0]));
        JSONObject columnType = options.getJson(COLUMN_TYPE, new JSONObject());
        for (String vidKeyName : mode.getGetVidNamesFunction().apply(options)) {
            columnList.add(vidKeyName);
            columnType.put(vidKeyName, VidCustomDataType.INSTANCE.getName());
        }
        if (options.hasProperty(COLUMN)) {
            options.set(COLUMN, columnList.toArray(new String[0]));
        }
        options.set(COLUMN_TYPE, columnType.toJSONString());
    }
}
