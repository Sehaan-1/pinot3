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
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.seatunnel.api.table.CatalogTable;

/** Allowlisted AST to canonical SQL adaptation; HSQLDB owns all function evaluation. */
final class SqlStringPlan {
  private final Set<String> strings = new HashSet<>();
  private int nodes;

  private SqlStringPlan(CatalogTable input) {
    for (var column : input.columns())
      if (column.type() == CatalogTable.DataType.STRING) strings.add(column.name());
  }

  static String compile(CatalogTable input, String expression) {
    String source = SqlFilterPlan.rowSource(input);
    if (expression == null || expression.isBlank() || expression.length() > 4096)
      throw new IllegalArgumentException("Expected SQL string expression of 1..4096 characters");
    try {
      Expression parsed = CCJSqlParserUtil.parseExpression(expression, false);
      return "SELECT " + new SqlStringPlan(input).render(parsed, 0) + " FROM " + source;
    } catch (JSQLParserException invalid) {
      throw new IllegalArgumentException("Invalid SQL string expression", invalid);
    }
  }

  private String render(Expression expression, int depth) {
    if (++nodes > 64 || depth > 16)
      throw new IllegalArgumentException("SQL string expression AST exceeds limits");
    if (expression instanceof Column column) {
      if (column.getTable() != null
          || column.getArrayConstructor() != null
          || column.getCommentText() != null
          || !strings.contains(column.getColumnName()))
        throw new IllegalArgumentException("Expected unqualified existing STRING field");
      return "\"" + column.getColumnName() + "\"";
    }
    if (!(expression instanceof Function function))
      throw new IllegalArgumentException("Unsupported SQL string expression");
    if (function.getMultipartName() == null
        || function.getMultipartName().size() != 1
        || function.isAllColumns()
        || function.isDistinct()
        || function.isUnique()
        || function.isEscaped()
        || function.isIgnoreNulls()
        || function.getNamedParameters() != null
        || function.getAttribute() != null
        || function.getKeep() != null
        || function.getOrderByElements() != null)
      throw new IllegalArgumentException("Decorated SQL functions are unsupported");
    String name = function.getName().toUpperCase(Locale.ROOT);
    var arguments = function.getParameters();
    int size = arguments == null ? 0 : arguments.size();
    boolean valid =
        switch (name) {
          case "UPPER", "LOWER" -> size == 1;
          case "CONCAT" -> size == 2;
          case "COALESCE" -> size >= 2 && size <= 8;
          default -> false;
        };
    if (!valid) throw new IllegalArgumentException("Unsupported SQL string function or arity");
    return name
        + "("
        + arguments.stream()
            .map(argument -> render(argument, depth + 1))
            .collect(Collectors.joining(","))
        + ")";
  }
}
