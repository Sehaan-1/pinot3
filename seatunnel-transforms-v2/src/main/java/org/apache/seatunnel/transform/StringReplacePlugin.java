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

/** Literal replace provider; resource acquisition belongs to the operator lifecycle. */
public final class StringReplacePlugin implements TransformPlugin {
  public String identifier() {
    return "string-replace";
  }

  public SeaTunnelTransform createTransform(Map<String, Object> options, CatalogTable input) {
    StringOptions.keys(options, Set.of("field", "search", "replacement"));
    return StringTransforms.replace(
        input,
        StringOptions.string(options, "field"),
        StringOptions.string(options, "search"),
        StringOptions.string(options, "replacement"));
  }
}
