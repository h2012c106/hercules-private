package com.xiaohongshu.db.hercules.manager.dao.mapper;

import com.xiaohongshu.db.hercules.manager.dao.mapper.provider.TaskProvider;
import com.xiaohongshu.db.share.entity.Task;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Mapper
@Repository
public interface TaskMapper {

    @InsertProvider(type = TaskProvider.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(Task entity);

    @Select("select * from `task`;")
    @ResultMap("taskResultMap")
    List<Task> findAll();

    @Select("select * from `task` where `id` = #{id};")
    @ResultMap("taskResultMap")
    Task findOne(Task task);

    @Select("select * from `task` where `job_id` = #{jobId};")
    @ResultMap("taskResultMap")
    List<Task> findByJobId(long jobId);

    @Select("select * from `task` as `t` where `create_time` = ( select max(`create_time`) from `task` where `job_id` = #{jobId} );")
    @ResultMap("taskResultMap")
    Task findLatestByJobId(long jobId);

    @Select("select * from `task` where `node_id` = #{nodeId};")
    @ResultMap("taskResultMap")
    List<Task> findByNodeId(long nodeId);

    @UpdateProvider(type = TaskProvider.class, method = "updateWithMap")
    int patch(Map<String, Object> map);

    @UpdateProvider(type = TaskProvider.class, method = "updateStatus")
    public int updateStatus(long taskId, Task.TaskStatus status);
}
