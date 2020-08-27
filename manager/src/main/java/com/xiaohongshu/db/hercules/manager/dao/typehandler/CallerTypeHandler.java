package com.xiaohongshu.db.hercules.manager.dao.typehandler;

import com.xiaohongshu.db.share.entity.Task;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CallerTypeHandler extends BaseTypeHandler<Task.Caller> {
    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, Task.Caller parameter, JdbcType jdbcType) throws SQLException {
        ps.setString(i, parameter.name());
    }

    @Override
    public Task.Caller getNullableResult(ResultSet rs, String columnName) throws SQLException {
        String res = rs.getString(columnName);
        return rs.wasNull() ? null : Task.Caller.valueOf(res);
    }

    @Override
    public Task.Caller getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        String res = rs.getString(columnIndex);
        return rs.wasNull() ? null : Task.Caller.valueOf(res);
    }

    @Override
    public Task.Caller getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        String res = cs.getString(columnIndex);
        return cs.wasNull() ? null : Task.Caller.valueOf(res);
    }
}
