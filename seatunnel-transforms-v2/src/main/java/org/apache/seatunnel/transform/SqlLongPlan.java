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

import java.util.HashSet;
import java.util.Set;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.seatunnel.api.table.CatalogTable;

/** Canonical BIGINT query from an allowlisted library AST, never Java arithmetic. */
final class SqlLongPlan {
  private final Set<String> numbers = new HashSet<>();
  private int nodes;

  private SqlLongPlan(CatalogTable input) {
    for (var field : input.columns())
      if (field.type() == CatalogTable.DataType.INT || field.type() == CatalogTable.DataType.LONG)
        numbers.add(field.name());
  }

  static String compile(CatalogTable input, String expression) {
    String source = SqlFilterPlan.rowSource(input);
    if (expression == null || expression.isBlank() || expression.length() > 4096)
      throw new IllegalArgumentException("Expected SQL integer expression of 1..4096 characters");
    try {
      Expression parsed = CCJSqlParserUtil.parseExpression(expression, false);
      return "SELECT " + new SqlLongPlan(input).render(parsed, 0) + " FROM " + source;
    } catch (JSQLParserException invalid) {
      throw new IllegalArgumentException("Invalid SQL integer expression", invalid);
    }
  }

  private String render(Expression expression, int depth) {
    if (++nodes > 64 || depth > 16)
      throw new IllegalArgumentException("SQL integer expression AST exceeds limits");
    if (expression instanceof Column column) {
      if (column.getTable() != null
          || column.getArrayConstructor() != null
          || column.getCommentText() != null
          || !numbers.contains(column.getColumnName()))
        throw new IllegalArgumentException("Expected unqualified INT/LONG field");
      return "CAST(\"" + column.getColumnName() + "\" AS BIGINT)";
    }
    if (expression instanceof Parenthesis parenthesis)
      return "(" + render(parenthesis.getExpression(), depth + 1) + ")";
    if (expression instanceof SignedExpression signed
        && (signed.getSign() == '+' || signed.getSign() == '-'))
      return "(" + signed.getSign() + render(signed.getExpression(), depth + 1) + ")";
    if (expression instanceof Addition
        || expression instanceof Subtraction
        || expression instanceof Multiplication) {
      var binary = (BinaryExpression) expression;
      String operator =
          expression instanceof Addition ? "+" : expression instanceof Subtraction ? "-" : "*";
      return "("
          + render(binary.getLeftExpression(), depth + 1)
          + operator
          + render(binary.getRightExpression(), depth + 1)
          + ")";
    }
    throw new IllegalArgumentException("Unsupported SQL integer expression");
  }
}
