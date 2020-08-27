package com.xiaohongshu.db.hercules.manager.dao.mapper.provider;

import com.google.common.collect.Sets;
import com.xiaohongshu.db.share.entity.Node;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class NodeProvider extends BaseProvider<Node> {

    @Deprecated
    public String selectWithNode(Node node) {
        List<String> whereList = new ArrayList<>(2);
        if (node.getId() != null) {
            whereList.add("`id` = #{id}");
        }
        if (node.getHost() != null && node.getPort() != null) {
            whereList.add("`host` = #{host} and `port` = #{port}");
        }
        return "select * from `node` where " + StringUtils.join(whereList, " and ");
    }

    @Override
    protected String getTableName() {
        return "node";
    }

    @Override
    protected Set<String> unallowedPatchColumn() {
        return Sets.newHashSet("status");
    }

    @Override
    protected Set<String> unallowedInsertColumn() {
        return Sets.newHashSet("status");
    }
}
