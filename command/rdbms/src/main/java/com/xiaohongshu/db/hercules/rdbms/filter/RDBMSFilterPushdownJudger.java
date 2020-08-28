package com.xiaohongshu.db.hercules.rdbms.filter;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.filter.expr.*;
import com.xiaohongshu.db.hercules.core.filter.pushdown.FilterPushdownJudger;
import com.xiaohongshu.db.hercules.core.utils.BytesUtils;

import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RDBMSFilterPushdownJudger extends FilterPushdownJudger<String> {

    @Override
    protected boolean canColumnSelfAsCondition() {
        return true;
    }

    @Override
    protected Function<Expr, String> getColumnExprFunction() {
        return new Function<Expr, String>() {
            @Override
            public String apply(Expr expr) {
                return "`" + ((ColumnExpr) expr).getColumnName() + "`";
            }
        };
    }

    @Override
    protected Function<Expr, String> getCombinationExprFunction() {
        return new Function<Expr, String>() {
            @Override
            public String apply(Expr expr) {
                String combinationStr = ((CombinationExpr) expr).getType().name();
                return "( " + expr.getChildren()
                        .stream()
                        .map(child -> convert(child))
                        .collect(Collectors.joining(" " + combinationStr + " ")) + " )";
            }
        };
    }

    @Override
    protected Function<Expr, String> getFunctionExprFunction() {
        return new Function<Expr, String>() {

            private boolean isNullValueExpr(Expr expr) {
                return expr instanceof ValueExpr && ((ValueExpr) expr).getResult().isNull();
            }

            @Override
            public String apply(Expr expr) {
                Method method = ((FunctionExpr) expr).getMethod();
                List<Expr> paramList = expr.getChildren();
                switch (method.getName()) {
                    case "gt":
                        return convert(paramList.get(0)) + ">" + convert(paramList.get(1));
                    case "gte":
                        return convert(paramList.get(0)) + ">=" + convert(paramList.get(1));
                    case "lt":
                        return convert(paramList.get(0)) + "<" + convert(paramList.get(1));
                    case "lte":
                        return convert(paramList.get(0)) + "<=" + convert(paramList.get(1));
                    case "eq":
                        if (isNullValueExpr(paramList.get(0))) {
                            return convert(paramList.get(1)) + "IS NULL";
                        } else if (isNullValueExpr(paramList.get(1))) {
                            return convert(paramList.get(0)) + "IS NULL";
                        } else {
                            return convert(paramList.get(0)) + "=" + convert(paramList.get(1));
                        }
                    case "neq":
                        if (isNullValueExpr(paramList.get(0))) {
                            return convert(paramList.get(1)) + "IS NOT NULL";
                        } else if (isNullValueExpr(paramList.get(1))) {
                            return convert(paramList.get(0)) + "IS NOT NULL";
                        } else {
                            return convert(paramList.get(0)) + "<>" + convert(paramList.get(1));
                        }
                    case "in":
                        return convert(paramList.get(0)) + "in " + convert(paramList.get(1));
                    default:
                        throw new RuntimeException("Unknown pushdown function: " + method);
                }
            }
        };
    }

    @Override
    protected Function<Expr, String> getListExprFunction() {
        return new Function<Expr, String>() {
            @Override
            public String apply(Expr expr) {
                return "( " + expr.getChildren().stream().map(child -> convert(child)).collect(Collectors.joining(", ")) + " )";
            }
        };
    }

    @Override
    protected Function<Expr, String> getValueExprFunction() {
        return new Function<Expr, String>() {
            @Override
            public String apply(Expr expr) {
                ValueExpr valueExpr = (ValueExpr) expr;
                BaseDataType dataType = valueExpr.getDataType();
                if (dataType.isNumber() || dataType.isBoolean()) {
                    return valueExpr.getResult().asString();
                } else if (dataType.isBytes()) {
                    byte[] bytesValue = valueExpr.getResult().asBytes();
                    return "b'" + BytesUtils.bytesToBinStr(bytesValue) + "'";
                } else if (dataType.isNull()) {
                    return "NULL";
                } else if (dataType.isString() || dataType.isDate()) {
                    return "'" + valueExpr.getResult().asString() + "'";
                } else {
                    throw new UnsupportedOperationException("Unsupported pushdown base datatype: " + dataType);
                }
            }
        };
    }
}
