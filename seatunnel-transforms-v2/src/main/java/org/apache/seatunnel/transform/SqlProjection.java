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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import net.sf.jsqlparser.parser.CCJSqlParserConstants;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.TokenMgrException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Compiles library-parsed column selections to existing positional row mappings. */
public final class SqlProjection {
  private SqlProjection() {}

  public static SeaTunnelTransform create(CatalogTable input, String columns) {
    Objects.requireNonNull(input, "input");
    if (columns == null || columns.isBlank() || columns.length() > 4096)
      throw new IllegalArgumentException("Expected SQL columns of 1..4096 characters");
    List<SelectItem<?>> items;
    try {
      var parser = CCJSqlParserUtil.newParser(columns);
      items = parser.SelectItemsList();
      if (parser.getNextToken().kind != CCJSqlParserConstants.EOF)
        throw new IllegalArgumentException("Unexpected trailing SQL projection tokens");
    } catch (ParseException | TokenMgrException invalid) {
      throw new IllegalArgumentException("Invalid SQL projection", invalid);
    }
    if (items.isEmpty() || items.size() > 128)
      throw new IllegalArgumentException("Expected 1..128 SQL projection columns");
    Map<String, Integer> sources = new HashMap<>();
    for (int i = 0; i < input.columns().size(); i++) sources.put(input.columns().get(i).name(), i);
    List<CatalogTable.Column> output = new ArrayList<>();
    int[] positions = new int[items.size()];
    for (int i = 0; i < items.size(); i++) {
      var item = items.get(i);
      if (!(item.getExpression() instanceof Column column)
          || column.getArrayConstructor() != null
          || column.getCommentText() != null
          || (column.getTable() != null && column.getTable().getName() != null))
        throw new IllegalArgumentException(
            "SQL projection accepts unqualified field references only");
      String source = identifier(column.getColumnName());
      Integer position = sources.get(source);
      if (position == null)
        throw new IllegalArgumentException("Unknown SQL projection field: " + source);
      String target = source;
      var alias = item.getAlias();
      if (alias != null) {
        if (!alias.isUseAs()
            || (alias.getAliasColumns() != null && !alias.getAliasColumns().isEmpty()))
          throw new IllegalArgumentException("Expected explicit AS field alias");
        target = identifier(alias.getName());
      }
      var field = input.columns().get(position);
      output.add(new CatalogTable.Column(target, field.type(), field.nullable()));
      positions[i] = position;
    }
    return FieldTransforms.project(input, new CatalogTable(input.name(), output), positions);
  }

  private static String identifier(String name) {
    if (name == null || name.length() > 128 || !name.matches("[A-Za-z_][A-Za-z0-9_]*"))
      throw new IllegalArgumentException("Expected simple SQL field identifier");
    return name;
  }
}
