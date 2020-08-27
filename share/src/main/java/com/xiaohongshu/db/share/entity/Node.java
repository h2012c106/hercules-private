package com.xiaohongshu.db.share.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Getter
@Setter
@NoArgsConstructor
public class Node {

    public Node(Long id) {
        this.id = id;
    }

    private Long id;

    @NotNull
    private Long clusterId;

    @NotEmpty
    private String host;

    private Long port;

    private NodeStatus status;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equal(id, node.id);
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

    public enum NodeStatus {
        /**
         * 关闭
         **/
        OFF,
        /**
         * 开启
         **/
        ON;
    }
}
