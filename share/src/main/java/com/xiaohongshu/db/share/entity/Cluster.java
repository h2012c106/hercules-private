package com.xiaohongshu.db.share.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

import javax.validation.constraints.NotEmpty;

@Getter
@Setter
@NoArgsConstructor
public class Cluster {

    public Cluster(Long id) {
        this.id = id;
    }

    private Long id;

    @NotEmpty
    private String name;

    private String desc;

    private Cloud cloud;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cluster cluster = (Cluster) o;
        return Objects.equal(id, cluster.id);
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

    public enum Cloud {
        /**
         * 腾讯
         **/
        TX,
        /**
         * 华为
         **/
        HW;
    }
}
