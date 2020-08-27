package com.xiaohongshu.db.hercules.manager.dao.typehandler;

import com.xiaohongshu.db.share.entity.Cluster;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CloudTypeHandler extends BaseTypeHandler<Cluster.Cloud> {
    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, Cluster.Cloud parameter, JdbcType jdbcType) throws SQLException {
        ps.setString(i, parameter.name());
    }

    @Override
    public Cluster.Cloud getNullableResult(ResultSet rs, String columnName) throws SQLException {
        String res = rs.getString(columnName);
        return rs.wasNull() ? null : Cluster.Cloud.valueOf(res);
    }

    @Override
    public Cluster.Cloud getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        String res = rs.getString(columnIndex);
        return rs.wasNull() ? null : Cluster.Cloud.valueOf(res);
    }

    @Override
    public Cluster.Cloud getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        String res = cs.getString(columnIndex);
        return cs.wasNull() ? null : Cluster.Cloud.valueOf(res);
    }
}
