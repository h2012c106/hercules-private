package com.xiaohongshu.db.share.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
public class Task {

    public Task(Long id) {
        this.id = id;
    }

    private Long id;

    @NotNull
    private long jobId;

    @NotNull
    private long nodeId;

    @NotEmpty
    private String command;

    private java.sql.Timestamp submitTime;

    private java.sql.Timestamp startTime;

    private long totalTimeMs;

    private long mrTimeMs;

    private long mapNum;

    private long recordNum;

    private long estimatedByteSize;

    private TaskStatus status;

    private String applicationId;

    private String mrLogUrl;

    private int mrProgress;

    private Caller caller;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task task = (Task) o;
        return Objects.equal(id, task.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @SneakyThrows
    @Override
    public String toString() {
        return new ObjectMapper().writeValueAsString(this);
    }

    public enum TaskStatus {
        /**
         * 初始
         */
        PENDING,
        /**
         * 执行中
         */
        RUNNING,
        /**
         * 被用户中断
         */
        KILLED,
        /**
         * 成功
         */
        SUCCESS,
        /**
         * 失败
         */
        ERROR,
        /**
         * 被强行制为结束，未知成功或失败
         */
        DONE_UNKNOWN,
        /**
         * 无人管理此任务
         **/
        ZOMBIE;

        public boolean isEnd() {
            return this == KILLED || this == SUCCESS || this == ERROR;
        }

        public boolean isRunning() {
            return this == PENDING || this == RUNNING;
        }

        /**
         * 每个状态允许的前置状态，若为空，则说明任何状态都可以转移到此状态
         */
        private static final Map<TaskStatus, List<TaskStatus>> PRE_STATUS_MAP = new HashMap<>();

        static {
            PRE_STATUS_MAP.put(PENDING, Collections.emptyList());
            PRE_STATUS_MAP.put(RUNNING, Lists.newArrayList(PENDING));
            PRE_STATUS_MAP.put(KILLED, Lists.newArrayList(PENDING, RUNNING, ZOMBIE));
            PRE_STATUS_MAP.put(SUCCESS, Collections.emptyList());
            PRE_STATUS_MAP.put(ERROR, Collections.emptyList());
            PRE_STATUS_MAP.put(DONE_UNKNOWN, Collections.emptyList());
            PRE_STATUS_MAP.put(ZOMBIE, Lists.newArrayList(PENDING, RUNNING));
        }

        public static List<TaskStatus> getPreStatusList(TaskStatus status) {
            return PRE_STATUS_MAP.computeIfAbsent(status, key -> Collections.emptyList());
        }
    }

    public enum Caller {
        RDS,
        AIRFLOW;
    }

}
