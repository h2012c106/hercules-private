package com.xiaohongshu.db.hercules.manager.dao.mapper;

import com.xiaohongshu.db.hercules.manager.dao.mapper.provider.JobProvider;
import com.xiaohongshu.db.share.entity.Job;
import com.xiaohongshu.db.share.entity.Task;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Mapper
@Repository
public interface JobMapper {

    @InsertProvider(type = JobProvider.class, method = "insert")
    @Options(useGeneratedKeys = true, keyProperty = "id")
    int insert(Job entity);

    @Delete("delete from `job` where `id` = #{id};")
    int delete(long id);

    @Select("select * from `job`;")
    @ResultMap("jobResultMap")
    List<Job> findAll();

    @UpdateProvider(type = JobProvider.class, method = "updateWithMap")
    int patch(Map<String, Object> map);

    /**
     * 由于在申请起一个task时，task并未真正注册进数据库，不能直接上来就置taskId，得先要个锁，保证仅有一个线程要到起task的资格
     * 如果不这么做，需要在node发起insert之后进行start，再检查有没有start成功然后再告知node，逻辑过于复杂了。
     *
     * @param jobId
     * @return
     */
    @Update("update `job` set `running_task_id` = -1 where `id` = #{jobId} and `running_task_id` is null;")
    int readyTask(long jobId);

    /**
     * 手动回滚，由于发生嵌套事务，百分百死锁，故要拆分事务处理
     * @param jobId
     * @return
     */
    @Update("update `job` set `running_task_id` = null where `id` = #{jobId};")
    int unreadyTask(long jobId);

    @Update("update `job` set `running_task_id` = #{taskId} where `id` = #{jobId} and `running_task_id` = -1;")
    int startTask(long jobId, long taskId);

    /**
     * 不需要jobId来约束，由于taskId能够唯一确定一个job，而此时taskId一定已经有值了，具体原因下面写了。
     * 保证一个task在这只会有一次注销行为。上层service只要收到一条end status，这个task就一定结束了，所以一次足矣。
     * 此外，由于taskId可能为-1，也许会担心是否会出现在-1时调用本sql的情况，其实是不会的，总共三种状态，分类讨论一下:
     * 1. SUCCESS/ERROR: 归为一类，都属于正常退出，按照流程，一定会在启动前把taskId置好，此时不可能为-1。
     * 2. KILL: 唯一的边界条件在{@link TaskMapper#insert(Task)}后{@link #startTask(long, long)}前调用本函数，
     * &nbsp;但是在kill的service逻辑内会使用{@link #findByRunningTaskId(long)}先检查这个job下这个task有没有注册进来，
     * &nbsp;但是{@link TaskMapper#insert(Task)}、{@link #startTask(long, long)}这两个调用是事务，保证不发生脏读，
     * &nbsp;外界不会对两个操作的一前一后有感知。此外借助在service kill的时候借助{@link #findByRunningTaskId(long)}做double check。
     *
     * @param taskId
     * @return
     */
    @Update("update `job` set `running_task_id` = null where `running_task_id` = #{taskId};")
    int stopTask(long taskId);

    @Select("select * from `job` where `running_task_id` = #{taskId};")
    @ResultMap("jobResultMap")
    Job findByRunningTaskId(long taskId);

    @Select("select * from `job` where `id` = #{id};")
    @ResultMap("jobResultMap")
    Job findOne(Job node);
}
