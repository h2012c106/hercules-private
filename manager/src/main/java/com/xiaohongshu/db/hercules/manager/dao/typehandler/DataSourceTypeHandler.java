package com.xiaohongshu.db.hercules.manager.dao.typehandler;

import com.xiaohongshu.db.hercules.core.datasource.DataSource;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataSourceTypeHandler extends BaseTypeHandler<DataSource> {

    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, DataSource parameter, JdbcType jdbcType) throws SQLException {
        ps.setString(i, parameter.name());
    }

    @Override
    public DataSource getNullableResult(ResultSet rs, String columnName) throws SQLException {
        String res = rs.getString(columnName);
        return res == null ? null : DataSource.valueOfIgnoreCase(res);
    }

    @Override
    public DataSource getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        String res = rs.getString(columnIndex);
        return res == null ? null : DataSource.valueOfIgnoreCase(res);
    }

    @Override
    public DataSource getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        String res = cs.getString(columnIndex);
        return res == null ? null : DataSource.valueOfIgnoreCase(res);
    }
}
