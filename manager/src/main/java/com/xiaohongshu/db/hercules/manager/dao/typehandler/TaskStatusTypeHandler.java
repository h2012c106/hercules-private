package com.xiaohongshu.db.hercules.manager.dao.typehandler;

import com.xiaohongshu.db.share.entity.Task;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedJdbcTypes;
import org.apache.ibatis.type.MappedTypes;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@MappedJdbcTypes(JdbcType.INTEGER)
@MappedTypes(Task.TaskStatus.class)
public class TaskStatusTypeHandler extends BaseTypeHandler<Task.TaskStatus> {

    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, Task.TaskStatus parameter, JdbcType jdbcType) throws SQLException {
        ps.setString(i, parameter.name());
    }

    @Override
    public Task.TaskStatus getNullableResult(ResultSet rs, String columnName) throws SQLException {
        String res = rs.getString(columnName);
        return rs.wasNull() ? null : Task.TaskStatus.valueOf(res);
    }

    @Override
    public Task.TaskStatus getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        String res = rs.getString(columnIndex);
        return rs.wasNull() ? null : Task.TaskStatus.valueOf(res);
    }

    @Override
    public Task.TaskStatus getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        String res = cs.getString(columnIndex);
        return cs.wasNull() ? null : Task.TaskStatus.valueOf(res);
    }
}
