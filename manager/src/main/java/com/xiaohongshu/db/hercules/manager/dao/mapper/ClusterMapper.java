package com.xiaohongshu.db.hercules.manager.dao.mapper;

import com.xiaohongshu.db.hercules.manager.dao.mapper.provider.ClusterProvider;
import com.xiaohongshu.db.share.entity.Cluster;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Mapper
@Repository
public interface ClusterMapper {

    @Select("select * from `cluster`;")
    @ResultMap("clusterResultMap")
    public List<Cluster> findAll();

    @SelectProvider(type = ClusterProvider.class, method = "selectWithCluster")
    @ResultMap("clusterResultMap")
    public Cluster findOne(Cluster cluster);

    @InsertProvider(type = ClusterProvider.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    public int insert(Cluster cluster);

    @UpdateProvider(type = ClusterProvider.class, method = "updateWithMap")
    int patch(Map<String, Object> map);

    @Delete("delete from `cluster` where `id` = #{id};")
    public int delete(long id);

    @Select("select * from `cluster` where `cloud` = #{cloudCode} order by rand() limit 1;")
    @ResultMap("clusterResultMap")
    public Cluster pickByCloud(String cloudCode);

}
