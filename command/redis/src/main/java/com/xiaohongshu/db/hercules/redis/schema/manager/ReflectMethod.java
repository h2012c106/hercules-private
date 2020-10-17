package com.xiaohongshu.db.hercules.redis.schema.manager;


import java.lang.reflect.Method;

public class ReflectMethod {

    private Class obj;

    private Method method;

    public Class getObj() {
        return obj;
    }

    public void setObj(Class obj) {
        this.obj = obj;
    }

    public Method getMethod() {
        return method;
    }

    public void setMethod(Method method) {
        this.method = method;
    }
}
