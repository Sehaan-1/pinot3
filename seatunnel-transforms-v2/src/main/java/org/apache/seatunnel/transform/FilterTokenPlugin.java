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

import java.util.Map;
import java.util.Set;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.plugin.TransformPlugin;

/** Installed exact-token filter with explicit ANY/ALL semantics. */
public final class FilterTokenPlugin implements TransformPlugin {
  public String identifier() {
    return "filter-token";
  }

  public SeaTunnelTransform createTransform(Map<String, Object> options, CatalogTable input) {
    StringOptions.keys(options, Set.of("field", "delimiter", "tokens", "mode"));
    var mode =
        switch (StringOptions.string(options, "mode")) {
          case "any" -> FilterTokenTransform.MatchMode.ANY;
          case "all" -> FilterTokenTransform.MatchMode.ALL;
          default -> throw new IllegalArgumentException("Expected token mode any or all");
        };
    return new FilterTokenTransform(
        input,
        StringOptions.string(options, "field"),
        StringOptions.string(options, "delimiter"),
        StringOptions.strings(options, "tokens"),
        mode);
  }
}
