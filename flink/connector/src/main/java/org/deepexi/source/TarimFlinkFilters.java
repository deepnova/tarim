package org.deepexi.source;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkFilters;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.NaNUtil;
import org.deepexi.FlinkSqlPrimaryKey;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TarimFlinkFilters {

    private  TarimFlinkFilters() {
    }

    public static boolean partitionFilter = false;
    public static boolean partitionEqFilter = false;
    public static boolean primaryKeyFilter = false;
    public static boolean otherFilter = false;
    public static HashSet<Object> partitionValues = new HashSet<>();
    public static HashSet<FlinkSqlPrimaryKey> flinkSqlPrimaryKeys = new HashSet<>();
    private static final Pattern STARTS_WITH_PATTERN = Pattern.compile("([^%]+)%");

    private static FilterStatus status = FilterStatus.STATUS_INIT;

    private static final Map<FunctionDefinition, Expression.Operation> FILTERS = ImmutableMap
            .<FunctionDefinition, Expression.Operation>builder()
            .put(BuiltInFunctionDefinitions.EQUALS, Expression.Operation.EQ)
            .put(BuiltInFunctionDefinitions.NOT_EQUALS, Expression.Operation.NOT_EQ)
            .put(BuiltInFunctionDefinitions.GREATER_THAN, Expression.Operation.GT)
            .put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, Expression.Operation.GT_EQ)
            .put(BuiltInFunctionDefinitions.LESS_THAN, Expression.Operation.LT)
            .put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, Expression.Operation.LT_EQ)
            .put(BuiltInFunctionDefinitions.IS_NULL, Expression.Operation.IS_NULL)
            .put(BuiltInFunctionDefinitions.IS_NOT_NULL, Expression.Operation.NOT_NULL)
            .put(BuiltInFunctionDefinitions.AND, Expression.Operation.AND)
            .put(BuiltInFunctionDefinitions.OR, Expression.Operation.OR)
            .put(BuiltInFunctionDefinitions.NOT, Expression.Operation.NOT)
            .put(BuiltInFunctionDefinitions.LIKE, Expression.Operation.STARTS_WITH)
            .build();

    /**
     * Convert flink expression to iceberg expression.
     * <p>
     * the BETWEEN, NOT_BETWEEN, IN expression will be converted by flink automatically. the BETWEEN will be converted to
     * (GT_EQ AND LT_EQ), the NOT_BETWEEN will be converted to (LT_EQ OR GT_EQ), the IN will be converted to OR, so we do
     * not add the conversion here
     *
     * @param flinkExpression the flink expression
     * @return the iceberg expression
     */
    public static Optional<Expression> convert(org.apache.flink.table.expressions.Expression flinkExpression, List<String> partitionKeys, List<String> primaryKeys) {
        if (!(flinkExpression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) flinkExpression;
        Expression.Operation op = FILTERS.get(call.getFunctionDefinition());
        if (op != null) {
            switch (op) {
                case IS_NULL:
                    partitionFilter = false;
                    return onlyChildAs(call, FieldReferenceExpression.class)
                            .map(FieldReferenceExpression::getName)
                            .map(Expressions::isNull);

                case NOT_NULL:
                    partitionFilter = false;
                    return onlyChildAs(call, FieldReferenceExpression.class)
                            .map(FieldReferenceExpression::getName)
                            .map(Expressions::notNull);

                case LT:
                    return convertFieldAndLiteral(Expressions::lessThan, Expressions::greaterThan, call, partitionKeys, primaryKeys, op);

                case LT_EQ:
                    return convertFieldAndLiteral(Expressions::lessThanOrEqual, Expressions::greaterThanOrEqual, call, partitionKeys, primaryKeys, op);

                case GT:
                    return convertFieldAndLiteral(Expressions::greaterThan, Expressions::lessThan, call, partitionKeys, primaryKeys, op);

                case GT_EQ:
                    return convertFieldAndLiteral(Expressions::greaterThanOrEqual, Expressions::lessThanOrEqual, call, partitionKeys, primaryKeys, op);

                case EQ:
                    return convertFieldAndLiteral((ref, lit) -> {
                        if (NaNUtil.isNaN(lit)) {
                            return Expressions.isNaN(ref);
                        } else {
                            return Expressions.equal(ref, lit);
                        }
                    }, call, partitionKeys, primaryKeys, op);

                case NOT_EQ:
                    return convertFieldAndLiteral((ref, lit) -> {
                        if (NaNUtil.isNaN(lit)) {
                            return Expressions.notNaN(ref);
                        } else {
                            return Expressions.notEqual(ref, lit);
                        }
                    }, call, partitionKeys, primaryKeys, op);

                case NOT:
                    return onlyChildAs(call, CallExpression.class).flatMap(FlinkFilters::convert).map(Expressions::not);

                case AND:
                    return convertLogicExpression(Expressions::and, call, partitionKeys, primaryKeys);

                case OR:
                    status = FilterStatus.STATUS_OR;
                    return convertLogicExpression(Expressions::or, call, partitionKeys, primaryKeys);

                case STARTS_WITH:
                    return convertLike(call);
            }
        }

        return Optional.empty();
    }

    private static <T extends ResolvedExpression> Optional<T> onlyChildAs(CallExpression call,
                                                                          Class<T> expectedChildClass) {
        List<ResolvedExpression> children = call.getResolvedChildren();
        if (children.size() != 1) {
            return Optional.empty();
        }

        ResolvedExpression child = children.get(0);
        if (!expectedChildClass.isInstance(child)) {
            return Optional.empty();
        }

        return Optional.of(expectedChildClass.cast(child));
    }

    private static Optional<Expression> convertLike(CallExpression call) {
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (args.size() != 2) {
            return Optional.empty();
        }

        org.apache.flink.table.expressions.Expression left = args.get(0);
        org.apache.flink.table.expressions.Expression right = args.get(1);

        if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
            String name = ((FieldReferenceExpression) left).getName();
            return convertLiteral((ValueLiteralExpression) right).flatMap(lit -> {
                if (lit instanceof String) {
                    String pattern = (String) lit;
                    Matcher matcher = STARTS_WITH_PATTERN.matcher(pattern);
                    // exclude special char of LIKE
                    // '_' is the wildcard of the SQL LIKE
                    if (!pattern.contains("_") && matcher.matches()) {
                        return Optional.of(Expressions.startsWith(name, matcher.group(1)));
                    }
                }

                return Optional.empty();
            });
        }

        return Optional.empty();
    }

    private static Optional<Expression> convertLogicExpression(BiFunction<Expression, Expression, Expression> function,
                                                               CallExpression call, List<String> partitionKeys, List<String> primaryKeys) {
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (args == null || args.size() != 2) {
            return Optional.empty();
        }

        Optional<Expression> left = convert(args.get(0), partitionKeys, primaryKeys);
        Optional<Expression> right = convert(args.get(1), partitionKeys, primaryKeys);
        if (left.isPresent() && right.isPresent()) {
            return Optional.of(function.apply(left.get(), right.get()));
        }

        return Optional.empty();
    }

    private static Optional<Object> convertLiteral(ValueLiteralExpression expression) {
        Optional<?> value = expression.getValueAs(expression.getOutputDataType().getLogicalType().getDefaultConversion());
        return value.map(o -> {
            if (o instanceof LocalDateTime) {
                return DateTimeUtil.microsFromTimestamp((LocalDateTime) o);
            } else if (o instanceof Instant) {
                return DateTimeUtil.microsFromInstant((Instant) o);
            } else if (o instanceof LocalTime) {
                return DateTimeUtil.microsFromTime((LocalTime) o);
            } else if (o instanceof LocalDate) {
                return DateTimeUtil.daysFromDate((LocalDate) o);
            }

            return o;
        });
    }

    private static Optional<Expression> convertFieldAndLiteral(BiFunction<String, Object, Expression> expr,
                                                               CallExpression call, List<String> partitionKeys,
                                                               List<String> primaryKeys, Expression.Operation op) {
        return convertFieldAndLiteral(expr, expr, call, partitionKeys, primaryKeys, op);
    }

    private static Optional<Expression> convertFieldAndLiteral(
            BiFunction<String, Object, Expression> convertLR, BiFunction<String, Object, Expression> convertRL,
            CallExpression call, List<String> partitionKeys, List<String> primaryKeys, Expression.Operation op) {
        List<ResolvedExpression> args = call.getResolvedChildren();
        if (args.size() != 2) {
            return Optional.empty();
        }

        org.apache.flink.table.expressions.Expression left = args.get(0);
        org.apache.flink.table.expressions.Expression right = args.get(1);

        if (left instanceof FieldReferenceExpression && right instanceof ValueLiteralExpression) {
            String name = ((FieldReferenceExpression) left).getName();
            Optional<Object> lit = convertLiteral((ValueLiteralExpression) right);
            setFilter(partitionKeys, primaryKeys, op, name, lit);
            if (lit.isPresent()) {
                return Optional.of(convertLR.apply(name, lit.get()));
            }
        } else if (left instanceof ValueLiteralExpression && right instanceof FieldReferenceExpression) {
            Optional<Object> lit = convertLiteral((ValueLiteralExpression) left);
            String name = ((FieldReferenceExpression) right).getName();
            setFilter(partitionKeys, primaryKeys, op, name, lit);
            if (lit.isPresent()) {
                return Optional.of(convertRL.apply(name, lit.get()));
            }
        }

        return Optional.empty();
    }

    private static void setFilter(List<String> partitionKeys, List<String> primaryKeys, Expression.Operation op,
                                  String name, Optional<Object> lit){
        for (String partitionKey: partitionKeys){
            if (partitionKey.equals(name)){
                partitionFilter = true;
                if (status == FilterStatus.STATUS_INIT && op == Expression.Operation.EQ){
                    partitionValues.add(lit.get());
                    partitionEqFilter = true;
                }else{
                    otherFilter = true;
                }
            }else{
                int i = 0;
                for (;i< primaryKeys.size(); i++){
                    if (primaryKeys.get(i).equals(name)){
                        if (op == Expression.Operation.EQ && !otherFilter){
                            primaryKeyFilter = true;
                            flinkSqlPrimaryKeys.add(new FlinkSqlPrimaryKey(name, lit.get()));
                            break;
                        }
                    }
                }
                if (i == primaryKeys.size()){
                    otherFilter = true;
                }
            }
        }

    }
    private enum FilterStatus{
        STATUS_INIT,
        STATUS_OR;
    }
}
