package com.xiaohongshu.db.hercules.manager.dao.mapper;

import com.xiaohongshu.db.hercules.manager.dao.mapper.provider.NodeProvider;
import com.xiaohongshu.db.share.entity.Node;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Mapper
@Repository
public interface NodeMapper {

    @InsertProvider(type = NodeProvider.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    public int insert(Node node);

    @Delete("delete from `node` where `id` = #{id};")
    public int delete(long id);

    @UpdateProvider(type = NodeProvider.class, method = "updateWithMap")
    int patch(Map<String, Object> map);

    @Update("update `node` set `status` = #{statusCode} where `id` = #{id};")
    public int updateStatus(long id, String statusCode);

    @Select("select * from `node`;")
    @ResultMap("nodeResultMap")
    public List<Node> findAll();

    @Select("select * from `node` where `id` = #{id};")
    @ResultMap("nodeResultMap")
    public Node findOne(Node node);

    @Select("select * from `node` where `cluster_id` = #{clusterId};")
    @ResultMap("nodeResultMap")
    public List<Node> findByClusterId(long clusterId);

}
