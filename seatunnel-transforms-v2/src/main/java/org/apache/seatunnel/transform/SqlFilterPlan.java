/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.transform;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.DataType;

/** Library AST adaptation only. SQL semantics belong to the embedded JDBC evaluator. */
final class SqlFilterPlan {
  private final Map<String, DataType> fields = new HashMap<>();
  private int nodes;

  private SqlFilterPlan(CatalogTable input) {
    if (input.columns().size() > 128)
      throw new IllegalArgumentException("At most 128 SQL input columns");
    for (var column : input.columns()) {
      String name = column.name();
      if (name.length() > 128
          || !name.matches("[A-Za-z_][A-Za-z0-9_]*")
          || Set.of("TRUE", "FALSE", "NULL").contains(name.toUpperCase(Locale.ROOT)))
        throw new IllegalArgumentException("Unsupported SQL field name");
      sqlType(column.type());
      fields.put(name, column.type());
    }
  }

  static String compile(CatalogTable input, String condition) {
    if (condition == null || condition.isBlank() || condition.length() > 4096)
      throw new IllegalArgumentException("Expected SQL condition of 1..4096 characters");
    var compiler = new SqlFilterPlan(input);
    Expression expression;
    try {
      expression = CCJSqlParserUtil.parseCondExpression(condition, false);
    } catch (JSQLParserException invalid) {
      throw new IllegalArgumentException("Invalid SQL predicate", invalid);
    }
    if (expression == null) throw new IllegalArgumentException("Invalid SQL predicate");
    compiler.condition(expression, 0);
    return "SELECT 1 FROM " + rowSource(input) + " WHERE CAST((" + expression + ") AS BOOLEAN)";
  }

  static String rowSource(CatalogTable input) {
    new SqlFilterPlan(input); // Reuse the same schema/name/type validation for every SQL row query.
    String bindings =
        input.columns().stream()
            .map(column -> "CAST(? AS " + sqlType(column.type()) + ")")
            .collect(Collectors.joining(","));
    String columns =
        input.columns().stream()
            .map(column -> "\"" + column.name() + "\"")
            .collect(Collectors.joining(","));
    return "(VALUES(" + bindings + ")) AS \"input\"(" + columns + ")";
  }

  static String sqlType(DataType type) {
    return switch (type) {
      case BOOLEAN -> "BOOLEAN";
      case INT -> "INTEGER";
      case LONG -> "BIGINT";
      case STRING -> "VARCHAR(" + SqlFilterTransform.MAX_STRING_CHARS + ")";
      default ->
          throw new IllegalArgumentException(
              "SQL filter supports BOOLEAN, INT, LONG and STRING only");
    };
  }

  private void visit(int depth) {
    if (depth > 64 || ++nodes > 512)
      throw new IllegalArgumentException("SQL predicate AST exceeds limits");
  }

  private void condition(Expression expression, int depth) {
    visit(depth);
    if (expression instanceof Parenthesis parenthesis) {
      condition(parenthesis.getExpression(), depth + 1);
      return;
    }
    if (expression instanceof NotExpression not) {
      if (not.isExclamationMark()) throw new IllegalArgumentException("Use SQL NOT");
      condition(not.getExpression(), depth + 1);
      return;
    }
    if (expression instanceof AndExpression || expression instanceof OrExpression) {
      if (expression instanceof AndExpression and && and.isUseOperator())
        throw new IllegalArgumentException("Use SQL AND");
      BinaryExpression binary = (BinaryExpression) expression;
      condition(binary.getLeftExpression(), depth + 1);
      condition(binary.getRightExpression(), depth + 1);
      return;
    }
    if (expression instanceof EqualsTo
        || expression instanceof NotEqualsTo
        || expression instanceof GreaterThan
        || expression instanceof GreaterThanEquals
        || expression instanceof MinorThan
        || expression instanceof MinorThanEquals) {
      var comparison = (OldOracleJoinBinaryExpression) expression;
      if (comparison.getOldOracleJoinSyntax() != 0 || comparison.getOraclePriorPosition() != 0)
        throw new IllegalArgumentException("Oracle join syntax is unsupported");
      scalar(comparison.getLeftExpression(), depth + 1);
      scalar(comparison.getRightExpression(), depth + 1);
      return;
    }
    if (expression instanceof IsNullExpression isNull) {
      if (isNull.isUseIsNull() || isNull.isUseNotNull())
        throw new IllegalArgumentException("Use IS NULL / IS NOT NULL");
      scalar(isNull.getLeftExpression(), depth + 1);
      return;
    }
    if (expression instanceof NullValue) return;
    if (expression instanceof Column && scalar(expression, depth + 1) == DataType.BOOLEAN) return;
    throw new IllegalArgumentException("Unsupported SQL predicate expression");
  }

  private DataType scalar(Expression expression, int depth) {
    visit(depth);
    if (expression instanceof Parenthesis parenthesis)
      return scalar(parenthesis.getExpression(), depth + 1);
    if (expression instanceof StringValue) return DataType.STRING;
    if (expression instanceof LongValue) return DataType.LONG;
    if (expression instanceof NullValue) return null;
    if (expression instanceof SignedExpression signed
        && signed.getExpression() instanceof LongValue
        && (signed.getSign() == '-' || signed.getSign() == '+')) return DataType.LONG;
    if (expression instanceof Column column) {
      if (column.getTable() != null && column.getTable().getName() != null)
        throw new IllegalArgumentException("Qualified SQL columns are unsupported");
      String name = column.getColumnName();
      DataType type = fields.get(name);
      if (type != null) {
        column.setColumnName("\"" + name + "\"");
        return type;
      }
      if (name.equalsIgnoreCase("true") || name.equalsIgnoreCase("false")) {
        column.setColumnName(name.toUpperCase(Locale.ROOT));
        return DataType.BOOLEAN;
      }
      throw new IllegalArgumentException("Unknown SQL field");
    }
    throw new IllegalArgumentException("Unsupported SQL scalar expression");
  }
}
