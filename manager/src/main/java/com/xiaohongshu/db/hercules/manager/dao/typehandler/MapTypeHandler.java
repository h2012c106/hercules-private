package com.xiaohongshu.db.hercules.manager.dao.typehandler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedJdbcTypes;
import org.apache.ibatis.type.MappedTypes;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@MappedJdbcTypes(value = JdbcType.VARCHAR)
@MappedTypes(Map.class)
public class MapTypeHandler extends BaseTypeHandler<Map<String, String>> {

    @SneakyThrows
    private static String mapToString(@NonNull Map<String, String> map) {
        return new ObjectMapper().writeValueAsString(map);
    }

    private static final TypeReference<HashMap<String, String>> HASH_MAP_TYPE_REFERENCE = new TypeReference<HashMap<String, String>>() {
    };

    @SneakyThrows
    private static Map<String, String> stringToMap(@NonNull String string) {
        return new ObjectMapper().readValue(string, HASH_MAP_TYPE_REFERENCE);
    }

    @SneakyThrows
    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, Map<String, String> parameter, JdbcType jdbcType) throws SQLException {
        ps.setString(i, mapToString(parameter));
    }

    @Override
    public Map<String, String> getNullableResult(ResultSet rs, String columnName) throws SQLException {
        String res = rs.getString(columnName);
        return res == null ? null : stringToMap(res);
    }

    @Override
    public Map<String, String> getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        String res = rs.getString(columnIndex);
        return res == null ? null : stringToMap(res);
    }

    @Override
    public Map<String, String> getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        String res = cs.getString(columnIndex);
        return res == null ? null : stringToMap(res);
    }
}
