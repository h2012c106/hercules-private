package com.xiaohongshu.db.hercules.manager.service;

import java.util.List;
import java.util.Map;

public interface BaseService<T> {

    public long insert(T entity);

    public void delete(long id);

    public T findOne(T entity);

    public List<T> findAll();

    public void patch(Map<String, Object> map);

}
