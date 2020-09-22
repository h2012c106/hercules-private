package com.xiaohongshu.db.hercules.nebula.schema;

import com.vesoft.nebula.client.graph.GraphClient;
import com.vesoft.nebula.client.graph.ResultSet;
import com.xiaohongshu.db.hercules.core.datatype.DataType;
import com.xiaohongshu.db.hercules.core.option.GenericOptions;
import com.xiaohongshu.db.hercules.core.schema.BaseSchemaFetcher;
import com.xiaohongshu.db.hercules.core.utils.context.annotation.GeneralAssembly;
import com.xiaohongshu.db.hercules.nebula.NebulaDataMode;
import com.xiaohongshu.db.hercules.nebula.datatype.VidCustomDataType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

import static com.xiaohongshu.db.hercules.nebula.option.NebulaOptionsConf.MODE;
import static com.xiaohongshu.db.hercules.nebula.option.NebulaOptionsConf.TABLE;

public class NebulaSchemaFetcher extends BaseSchemaFetcher {

    private static final Log LOG = LogFactory.getLog(NebulaSchemaFetcher.class);

    @GeneralAssembly
    private NebulaDataTypeConverter dataTypeConverter;

    private NebulaDataMode mode;

    public NebulaSchemaFetcher(GenericOptions options) {
        super(options);
        mode = NebulaDataMode.valueOfIgnoreCase(options.getString(MODE, null));
    }

    private String getDescSql(String tableName) {
        return "DESCRIBE " + mode.getDescName() + " `" + tableName + "`;";
    }

    @Override
    protected List<String> innerGetColumnNameList() {
        try (GraphClient client = NebulaUtils.getConnection(getOptions())) {
            String descSql = getDescSql(getOptions().getString(TABLE, null));
            ResultSet resultSet = NebulaUtils.executeQuery(client, descSql);
            List<String> res = new ArrayList<>(resultSet.getResults().size());
            for (ResultSet.Result row : resultSet.getResults()) {
                res.add(row.getString("Field"));
            }
            res.addAll(getVidColumnList());
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getVidColumnList() {
        return Arrays.asList(mode.getGetVidNamesFunction().apply(getOptions()));
    }

    @Override
    protected Map<String, DataType> innerGetColumnTypeMap() {
        try (GraphClient client = NebulaUtils.getConnection(getOptions())) {
            String descSql = getDescSql(getOptions().getString(TABLE, null));
            ResultSet resultSet = NebulaUtils.executeQuery(client, descSql);
            Map<String, DataType> res = new HashMap<>();
            for (ResultSet.Result row : resultSet.getResults()) {
                res.put(row.getString("Field"), dataTypeConverter.convertElementType(row.getString("Type")));
            }
            // 将id列置为vid
            for (String column : getVidColumnList()) {
                res.put(column, VidCustomDataType.INSTANCE);
            }
            return res;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected List<Set<String>> innerGetUniqueKeyGroupList() {
        return Collections.singletonList(new HashSet<>(getVidColumnList()));
    }
}
