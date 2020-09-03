package com.xiaohongshu.db.hercules.core.filter.expr;

import com.google.common.base.Objects;
import com.xiaohongshu.db.hercules.core.filter.function.FilterCoreFunction;
import com.xiaohongshu.db.hercules.core.serialize.HerculesWritable;
import com.xiaohongshu.db.hercules.core.serialize.wrapper.BaseWrapper;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionExpr extends AbstractExpr {

    private Method method;
    private List<Expr> paramList;

    public FunctionExpr(String methodName, Expr... params) {
        method = MethodUtils.getAccessibleMethod(
                FilterCoreFunction.class,
                methodName,
                Collections.nCopies(params.length, BaseWrapper.class).toArray(new Class[0])
        );
        if (method == null) {
            throw new RuntimeException("Cannot find the function name: " + methodName);
        }
        if (!Modifier.isStatic(method.getModifiers())) {
            throw new RuntimeException("The method need to be static: " + method);
        }
        for (Expr param : params) {
            param.setParent(this);
        }
        paramList = Arrays.stream(params).collect(Collectors.toCollection(LinkedList::new));
    }

    public Method getMethod() {
        return method;
    }

    @SneakyThrows
    @Override
    public BaseWrapper<?> getResult(final HerculesWritable row) {
        return (BaseWrapper<?>) method.invoke(null, paramList.stream().map(param -> param.getResult(row)).toArray(Object[]::new));
    }

    @Override
    public List<Expr> getChildren() {
        return paramList;
    }

    @Override
    public String toString() {
        return "METHOD[" + method.getName() + "]<" + StringUtils.join(paramList, ", ") + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FunctionExpr that = (FunctionExpr) o;
        return Objects.equal(method, that.method) &&
                Objects.equal(paramList, that.paramList);
    }
}
