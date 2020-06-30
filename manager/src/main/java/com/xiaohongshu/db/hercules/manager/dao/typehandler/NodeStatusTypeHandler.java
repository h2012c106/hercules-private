package com.xiaohongshu.db.hercules.manager.dao.typehandler;

import com.xiaohongshu.db.share.entity.Node;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedJdbcTypes;
import org.apache.ibatis.type.MappedTypes;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@MappedJdbcTypes(JdbcType.INTEGER)
@MappedTypes(Node.NodeStatus.class)
public class NodeStatusTypeHandler extends BaseTypeHandler<Node.NodeStatus> {

    @Override
    public void setNonNullParameter(PreparedStatement preparedStatement, int i, Node.NodeStatus nodeStatus, JdbcType jdbcType) throws SQLException {
        preparedStatement.setString(i, nodeStatus.name());
    }

    @Override
    public Node.NodeStatus getNullableResult(ResultSet resultSet, String s) throws SQLException {
        String res = resultSet.getString(s);
        return resultSet.wasNull() ? null : Node.NodeStatus.valueOf(res);
    }

    @Override
    public Node.NodeStatus getNullableResult(ResultSet resultSet, int i) throws SQLException {
        String res = resultSet.getString(i);
        return resultSet.wasNull() ? null : Node.NodeStatus.valueOf(res);
    }

    @Override
    public Node.NodeStatus getNullableResult(CallableStatement callableStatement, int i) throws SQLException {
        String res = callableStatement.getString(i);
        return callableStatement.wasNull() ? null : Node.NodeStatus.valueOf(res);
    }
}
