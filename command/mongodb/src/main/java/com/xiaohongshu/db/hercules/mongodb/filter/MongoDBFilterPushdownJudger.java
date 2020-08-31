package com.xiaohongshu.db.hercules.mongodb.filter;

import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.filter.expr.*;
import com.xiaohongshu.db.hercules.core.filter.pushdown.FilterPushdownJudger;
import com.xiaohongshu.db.hercules.core.utils.OverflowUtils;
import lombok.NonNull;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.bson.Document;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 虽然MongoDB查询用Document但是因为子元素不能自成一个Document，没办法
 */
public class MongoDBFilterPushdownJudger extends FilterPushdownJudger<MongoDBFilterPushdownJudger.KeyValue> {

    @Override
    protected Function<Expr, KeyValue> getColumnExprFunction() {
        return new Function<Expr, KeyValue>() {
            @Override
            public KeyValue apply(Expr expr) {
                ColumnExpr columnExpr = (ColumnExpr) expr;
                return KeyValue.initializeKey(columnExpr.getColumnName());
            }
        };
    }

    @Override
    protected Function<Expr, KeyValue> getCombinationExprFunction() {
        return new Function<Expr, KeyValue>() {
            @Override
            public KeyValue apply(Expr expr) {
                CombinationExpr combinationExpr = (CombinationExpr) expr;
                String exprStr;
                switch (combinationExpr.getType()) {
                    case AND:
                        exprStr = "$and";
                        break;
                    case OR:
                        exprStr = "$or";
                        break;
                    default:
                        throw new RuntimeException("Unknwon combination type: " + combinationExpr.getType());
                }
                ArrayList<Document> exprList = new ArrayList<>(combinationExpr.getChildren().size());
                for (Expr child : combinationExpr.getChildren()) {
                    KeyValue value = convert(child);
                    if (value.isValue() && value.getValue() instanceof Document) {
                        Document childValue = (Document) value.getValue();
                        exprList.add(childValue);
                    } else {
                        throw new RuntimeException("Illegal combination element: " + value);
                    }
                }
                return KeyValue.initializeValue(new Document(exprStr, exprList));
            }
        };
    }

    @Override
    protected Function<Expr, KeyValue> getFunctionExprFunction() {
        return new Function<Expr, KeyValue>() {
            private Document assemble(Expr x, Expr y, String op) {
                Expr column;
                Expr value;
                if (isColumn(x)) {
                    column = x;
                    value = y;
                } else {
                    column = y;
                    value = x;
                }
                KeyValue columnKey = convert(column);
                KeyValue valueValue = convert(value);
                return new Document(columnKey.getKey(), new Document(op, valueValue.getValue()));
            }

            private Document assembleEq(Expr x, Expr y) {
                Expr column;
                Expr value;
                if (isColumn(x)) {
                    column = x;
                    value = y;
                } else {
                    column = y;
                    value = x;
                }
                KeyValue columnKey = convert(column);
                KeyValue valueValue = convert(value);
                return new Document(columnKey.getKey(), valueValue.getValue());
            }

            @Override
            public KeyValue apply(Expr expr) {
                Method method = ((FunctionExpr) expr).getMethod();
                List<Expr> paramList = expr.getChildren();
                Document res;
                switch (method.getName()) {
                    case "gt":
                        res = assemble(paramList.get(0), paramList.get(1), "$gt");
                        break;
                    case "gte":
                        res = assemble(paramList.get(0), paramList.get(1), "$gte");
                        break;
                    case "lt":
                        res = assemble(paramList.get(0), paramList.get(1), "$lt");
                        break;
                    case "lte":
                        res = assemble(paramList.get(0), paramList.get(1), "$lte");
                        break;
                    case "eq":
                        res = assembleEq(paramList.get(0), paramList.get(1));
                        break;
                    case "neq":
                        res = assemble(paramList.get(0), paramList.get(1), "$ne");
                        break;
                    case "in":
                        res = assemble(paramList.get(0), paramList.get(1), "$in");
                        break;
                    default:
                        throw new RuntimeException("Unknown pushdown function: " + method);
                }
                return KeyValue.initializeValue(res);
            }
        };
    }

    @Override
    protected Function<Expr, KeyValue> getListExprFunction() {
        return new Function<Expr, KeyValue>() {
            @Override
            public KeyValue apply(Expr expr) {
                return KeyValue.initializeValue(
                        expr.getChildren()
                                .stream()
                                .map(child -> convert(child).getValue())
                                .collect(Collectors.toCollection(ArrayList::new))
                );
            }
        };
    }

    @Override
    protected Function<Expr, KeyValue> getValueExprFunction() {
        return new Function<Expr, KeyValue>() {
            @Override
            public KeyValue apply(Expr expr) {
                ValueExpr valueExpr = (ValueExpr) expr;
                BaseDataType dataType = valueExpr.getDataType();
                Object res;
                if (dataType.isInteger()) {
                    res = valueExpr.getResult().asBigInteger().longValueExact();
                } else if (dataType.isFloat()) {
                    res = OverflowUtils.numberToDouble(valueExpr.getResult().asBigDecimal());
                } else if (dataType.isBoolean() || dataType.isNull() || dataType.isString() || dataType.isDate()) {
                    res = valueExpr.getValue();
                } else {
                    throw new UnsupportedOperationException("Unsupported pushdown base datatype: " + dataType);
                }
                return KeyValue.initializeValue(res);
            }
        };
    }

    public static class KeyValue {
        private String key = null;
        private Object value = null;
        private boolean isKey = false;
        private boolean isValue = false;

        public String getKey() {
            if (isKey) {
                return key;
            } else {
                throw new RuntimeException("Not a key: " + this);
            }
        }

        public Object getValue() {
            if (isValue) {
                return value;
            } else {
                throw new RuntimeException("Not a value: " + this);
            }
        }

        public boolean isKey() {
            return isKey;
        }

        public boolean isValue() {
            return isValue;
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("key", key)
                    .append("value", value)
                    .toString();
        }

        public static KeyValue initializeKey(@NonNull String key) {
            KeyValue res = new KeyValue();
            res.key = key;
            res.isKey = true;
            return res;
        }

        public static KeyValue initializeValue(Object value) {
            KeyValue res = new KeyValue();
            res.value = value;
            res.isValue = true;
            return res;
        }
    }
}
