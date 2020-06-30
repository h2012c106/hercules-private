package com.xiaohongshu.db.hercules.manager.service.impl;

import com.xiaohongshu.db.hercules.manager.dao.mapper.ClusterMapper;
import com.xiaohongshu.db.share.entity.Cluster;
import com.xiaohongshu.db.hercules.manager.exception.NotFoundException;
import com.xiaohongshu.db.hercules.manager.service.ClusterService;
import lombok.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ClusterServiceImpl implements ClusterService {

    @Autowired
    private ClusterMapper clusterMapper;

    @Override
    public long insert(Cluster cluster) {
        clusterMapper.insert(cluster);
        return cluster.getId();
    }

    @Override
    public void delete(long id) {
        clusterMapper.delete(id);
    }

    @Override
    public Cluster findOne(@NonNull Cluster cluster) {
        Cluster res = clusterMapper.findOne(cluster);
        if (res == null) {
            throw new NotFoundException("No cluster with cluster info: " + cluster);
        } else {
            return res;
        }
    }

    @Override
    public List<Cluster> findAll() {
        return clusterMapper.findAll();
    }

    /**
     * TODO 事务
     *
     * @param map
     */
    @Override
    public void patch(Map<String, Object> map) {
        int row = clusterMapper.patch(map);
        if (row != 1) {
            throw new RuntimeException("Unexpected updated row: " + row);
        }
    }
}
