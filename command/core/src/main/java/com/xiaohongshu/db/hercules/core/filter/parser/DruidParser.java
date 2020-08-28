package com.xiaohongshu.db.hercules.core.filter.parser;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.primitives.Bytes;
import com.xiaohongshu.db.hercules.core.datatype.BaseDataType;
import com.xiaohongshu.db.hercules.core.filter.expr.*;
import com.xiaohongshu.db.hercules.core.utils.BytesUtils;
import org.apache.commons.compress.utils.ByteUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 基于Druid AST的parser，将来有追求可以搞一个javacc的版本
 */
public class DruidParser extends AbstractParser {

    @Override
    protected Expr innerParse(String str) {
        // 由于用户输入的内容仅包括where条件的内容，而我暂不知道druid只parse where子句的姿势，故拼一个头
        String fakeSql = "select * from fake_table where " + str;
        SQLSelectStatement selectStatement = (SQLSelectStatement) SQLUtils.parseSingleStatement(fakeSql, JdbcConstants.MYSQL);
        SQLSelectQueryBlock selectQueryBlock = (SQLSelectQueryBlock) selectStatement.getSelect().getQuery();
        SQLExpr where = selectQueryBlock.getWhere();
        return convert(where);
    }

    private static final Map<Class<?>, ConvertFunction> CONVERT_MAP = new HashMap<>();

    static {
        CONVERT_MAP.put(SQLBinaryOpExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) expr;
                if (binaryOpExpr.getOperator().isLogical()) {
                    CombinationExpr.CombinationType combinationType;
                    if (binaryOpExpr.getOperator().equals(SQLBinaryOperator.BooleanAnd)) {
                        combinationType = CombinationExpr.CombinationType.AND;
                    } else if (binaryOpExpr.getOperator().equals(SQLBinaryOperator.BooleanOr)) {
                        combinationType = CombinationExpr.CombinationType.OR;
                    } else {
                        throw new UnsupportedOperationException(binaryOpExpr.getOperator().getName());
                    }
                    return new CombinationExpr(
                            combinationType,
                            parser.convert(binaryOpExpr.getLeft()),
                            parser.convert(binaryOpExpr.getRight())
                    );
                } else if (binaryOpExpr.getOperator().isRelational()) {
                    switch (binaryOpExpr.getOperator()) {
                        case GreaterThan:
                            return new FunctionExpr(
                                    "gt",
                                    parser.convert(binaryOpExpr.getLeft()),
                                    parser.convert(binaryOpExpr.getRight())
                            );
                        case GreaterThanOrEqual:
                            return new FunctionExpr(
                                    "gte",
                                    parser.convert(binaryOpExpr.getLeft()),
                                    parser.convert(binaryOpExpr.getRight())
                            );
                        case LessThan:
                            return new FunctionExpr(
                                    "lt",
                                    parser.convert(binaryOpExpr.getLeft()),
                                    parser.convert(binaryOpExpr.getRight())
                            );
                        case LessThanOrEqual:
                            return new FunctionExpr(
                                    "lte",
                                    parser.convert(binaryOpExpr.getLeft()),
                                    parser.convert(binaryOpExpr.getRight())
                            );
                        case Equality:
                            return new FunctionExpr(
                                    "eq",
                                    parser.convert(binaryOpExpr.getLeft()),
                                    parser.convert(binaryOpExpr.getRight())
                            );
                        case NotEqual:
                            return new FunctionExpr(
                                    "neq",
                                    parser.convert(binaryOpExpr.getLeft()),
                                    parser.convert(binaryOpExpr.getRight())
                            );
                        default:
                            throw new UnsupportedOperationException(binaryOpExpr.getOperator().getName());
                    }
                } else {
                    throw new UnsupportedOperationException(binaryOpExpr.getOperator().getName());
                }
            }
        });
        CONVERT_MAP.put(SQLMethodInvokeExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) expr;
                return new FunctionExpr(
                        methodInvokeExpr.getMethodName(),
                        methodInvokeExpr.getArguments().stream().map(parser::convert).toArray(Expr[]::new)
                );
            }
        });
        CONVERT_MAP.put(SQLNullExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                return new ValueExpr(null, BaseDataType.NULL);
            }
        });
        CONVERT_MAP.put(SQLIntegerExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLIntegerExpr integerExpr = (SQLIntegerExpr) expr;
                BigInteger value = integerExpr.getNumber() instanceof BigInteger
                        ? (BigInteger) integerExpr.getNumber()
                        : BigInteger.valueOf(integerExpr.getNumber().longValue());
                return new ValueExpr(value, BaseDataType.LONGLONG);
            }
        });
        CONVERT_MAP.put(SQLNumberExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLNumberExpr numberExpr = (SQLNumberExpr) expr;
                BigDecimal value = numberExpr.getValue() instanceof BigDecimal
                        ? (BigDecimal) numberExpr.getValue()
                        : BigDecimal.valueOf(numberExpr.getValue().doubleValue());
                return new ValueExpr(value, BaseDataType.DECIMAL);
            }
        });
        CONVERT_MAP.put(SQLBooleanExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLBooleanExpr booleanExpr = (SQLBooleanExpr) expr;
                return new ValueExpr(booleanExpr.getValue(), BaseDataType.BOOLEAN);
            }
        });
        CONVERT_MAP.put(SQLCharExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLCharExpr charExpr = (SQLCharExpr) expr;
                return new ValueExpr(charExpr.getText(), BaseDataType.STRING);
            }
        });
        CONVERT_MAP.put(SQLBinaryExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLBinaryExpr binaryExpr = (SQLBinaryExpr) expr;
                return new ValueExpr(BytesUtils.binStrToBytes(binaryExpr.getText()), BaseDataType.BYTES);
            }
        });
        CONVERT_MAP.put(SQLInListExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLInListExpr inListExpr = (SQLInListExpr) expr;
                ListExpr listExpr = new ListExpr(inListExpr.getTargetList().stream().map(parser::convert).collect(Collectors.toList()));
                return new FunctionExpr("in", parser.convert(inListExpr.getExpr()), listExpr);
            }
        });
        CONVERT_MAP.put(SQLIdentifierExpr.class, new ConvertFunction() {
            @Override
            public Expr convert(DruidParser parser, SQLExpr expr) {
                SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
                return new ColumnExpr(identifierExpr.getName());
            }
        });
    }

    private Expr convert(SQLExpr expr) {
        ConvertFunction convertFunction = CONVERT_MAP.get(expr.getClass());
        if (convertFunction != null) {
            return convertFunction.convert(this, expr);
        } else {
            throw new UnsupportedOperationException(String.format("Druid class [%s], value: %s", expr.getClass().getCanonicalName(), expr.toString()));
        }
    }

    private interface ConvertFunction {
        Expr convert(DruidParser parser, SQLExpr expr);
    }
}
