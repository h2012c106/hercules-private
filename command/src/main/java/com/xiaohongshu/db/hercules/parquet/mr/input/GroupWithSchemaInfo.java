package com.xiaohongshu.db.hercules.parquet.mr.input;


import org.apache.parquet.example.data.Group;

/**
 * 记录应该从当前group的某columnName下拿第几个值，适配repeated情况
 * 仅在list时会指定{@link GroupWithSchemaInfo#valueSeq}
 */
public class GroupWithSchemaInfo {
    private Group group;
    private int valueSeq;
    /**
     * 在Setter外拿过，不必再拿，故存在里面
     */
    private boolean empty;

    public GroupWithSchemaInfo(Group group, boolean empty) {
        this(group, 0, empty);
    }

    public GroupWithSchemaInfo(Group group, int valueSeq, boolean empty) {
        this.group = group;
        this.valueSeq = valueSeq;
        this.empty = empty;
    }

    public Group getGroup() {
        return group;
    }

    public int getValueSeq() {
        return valueSeq;
    }

    public boolean isEmpty() {
        return empty;
    }
}
