package com.xiaohongshu.db.hercules.manager.dao.mapper.provider;

import com.xiaohongshu.db.share.entity.Cluster;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterProvider extends BaseProvider<Cluster> {

    public String selectWithCluster(Cluster cluster) {
        List<String> whereList = new ArrayList<>(2);
        if (cluster.getId() != null) {
            whereList.add("`id` = #{id}");
        }
        if (cluster.getName() != null) {
            whereList.add("`name` = #{name}");
        }
        return "select * from `cluster` where " + StringUtils.join(whereList, " and ");
    }

    @Override
    protected String getTableName() {
        return "cluster";
    }
}
